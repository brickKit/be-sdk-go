package pg

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/brickKit/be-sdk-go/internal/problem"
	gomigrate "github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// MigrateUp runs the component's migrations, then the platform migration, then creates the current
// partition window of the outbox and of every partitioned table of lifecycle.yaml, all as the owner on
// a dedicated connection (P11.1, P11.3, P16.6). Running it twice changes nothing the second time. A
// schema newer than the image is left untouched with a WARN and no error (P1.8).
//
// Decision tree: invalid config or a malformed file header → error before anything runs; dirty state →
// error; schema newer than image → WARN, return; otherwise component up one file at a time (each
// retried on 55P03; a `-- be:contract after=` file first waits for no older version to run, else
// error with the earlier files applied, P11.4), platform up (same), window, result.
func MigrateUp(ctx context.Context, c MigrateConfig) (MigrateResult, error) {
	r, err := newRunner(c)
	if err != nil {
		return MigrateResult{}, err
	}
	latest, err := LatestVersion(c.Component)
	if err != nil {
		return MigrateResult{}, err
	}
	headers, err := migrationHeaders(c.Component, c.Version)
	if err != nil {
		return MigrateResult{}, err
	}
	from, dirty, _, err := r.state(ctx)
	if err != nil {
		return MigrateResult{}, err
	}
	res := MigrateResult{From: from, To: from, Latest: latest}
	if dirty {
		return res, problem.Wrap(errors.New("component migration state is dirty: fix the schema, then force the version"), "INTERNAL", nil)
	}
	if from > latest {
		r.log.Warn("schema is newer than this image; migrate up does nothing", "schema_version", from, "image_version", latest)
		res.Newer = true
		return res, nil
	}
	if latest > from {
		if res.To, err = r.up(ctx, func() (source.Driver, error) { return iofs.New(c.Component, ".") }, ComponentStateTable(c.Schema), r.contractGate(headers)); err != nil {
			return res, err
		}
		if res.To != latest {
			return res, problem.Wrap(fmt.Errorf("schema is at version %d, which the image's migrations do not contain (latest %d)", res.To, latest), "INTERNAL", nil)
		}
	}
	if _, err := r.up(ctx, func() (source.Driver, error) { return newPlatformSource(c.ComponentID) }, PlatformStateTable(c.Schema), nil); err != nil {
		return res, err
	}
	if err := r.ensureAuthzProjection(ctx); err != nil {
		return res, err
	}
	if res.PartitionsCreated, err = r.ensureWindow(ctx); err != nil {
		return res, err
	}
	if _, _, res.PlatformVersion, err = r.state(ctx); err != nil {
		return res, err
	}
	r.log.Info("migrate up done", "from", res.From, "to", res.To, "platform_version", res.PlatformVersion,
		"partitions_created", res.PartitionsCreated)
	return res, nil
}

// MigrateDown rolls back the last n component migrations; the platform migration stays (P11.1).
func MigrateDown(ctx context.Context, c MigrateConfig, n int) (MigrateResult, error) {
	if n <= 0 {
		return MigrateResult{}, problem.Wrap(errors.New("migrate down: n must be positive"), "INTERNAL", nil)
	}
	r, err := newRunner(c)
	if err != nil {
		return MigrateResult{}, err
	}
	latest, err := LatestVersion(c.Component)
	if err != nil {
		return MigrateResult{}, err
	}
	from, _, _, err := r.state(ctx)
	if err != nil {
		return MigrateResult{}, err
	}
	res := MigrateResult{From: from, Latest: latest}
	m, closeFn, err := r.migrator(func() (source.Driver, error) { return iofs.New(c.Component, ".") }, ComponentStateTable(c.Schema))
	if err != nil {
		return res, err
	}
	defer closeFn()
	for i := 0; i < n && ctx.Err() == nil; i++ {
		err := r.withLockRetry(ctx, m, func() error { return m.Steps(-1) })
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return res, problem.From(err)
		}
	}
	if err := ctx.Err(); err != nil {
		return res, problem.From(err)
	}
	res.To, _, res.PlatformVersion, err = r.state(ctx)
	r.log.Info("migrate down done", "from", res.From, "to", res.To)
	return res, err
}

// migrator opens golang-migrate on a fresh dedicated connection with the given state table.
func (r *runner) migrator(src func() (source.Driver, error), table string) (*gomigrate.Migrate, func(), error) {
	s, err := src()
	if err != nil {
		return nil, nil, problem.Wrap(err, "INTERNAL", nil)
	}
	db, err := r.openDB(true)
	if err != nil {
		return nil, nil, err
	}
	drv, err := migratepgx.WithInstance(db, &migratepgx.Config{MigrationsTable: table, DatabaseName: r.c.Database, SchemaName: r.c.Schema})
	if err != nil {
		_ = db.Close()
		return nil, nil, problem.Wrap(err, "INTERNAL", nil)
	}
	m, err := gomigrate.NewWithInstance("be", s, "pgx5", drv)
	if err != nil {
		_ = drv.Close()
		return nil, nil, problem.Wrap(err, "INTERNAL", nil)
	}
	return m, func() { _, _ = m.Close() }, nil
}

// up applies every pending migration of one source one step at a time, checking ctx between steps
// (a cancelled step is never cut in half), and returns the version reached, also on error. gate, when
// set, is asked before each file with the file's version.
func (r *runner) up(ctx context.Context, src func() (source.Driver, error), table string, gate func(context.Context, uint) error) (uint, error) {
	s, err := src()
	if err != nil {
		return 0, problem.Wrap(err, "INTERNAL", nil)
	}
	defer func() { _ = s.Close() }()
	m, closeFn, err := r.migrator(src, table)
	if err != nil {
		return 0, err
	}
	defer closeFn()
	for {
		cur, err := current(m)
		if err != nil {
			return cur, err
		}
		if err := ctx.Err(); err != nil {
			return cur, problem.From(err)
		}
		next, ok, err := nextVersion(s, m)
		if err != nil || !ok {
			return cur, err
		}
		if gate != nil {
			if err := gate(ctx, next); err != nil {
				return cur, err
			}
		}
		if err := r.withLockRetry(ctx, m, func() error { return m.Steps(1) }); err != nil {
			return cur, problem.From(err)
		}
	}
}

// current is the state table's version; 0 when no migration ran.
func current(m *gomigrate.Migrate) (uint, error) {
	v, _, err := m.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return 0, problem.Wrap(err, "INTERNAL", nil)
	}
	return v, nil
}

// nextVersion is the version the next Steps(1) applies; ok is false when none is left.
func nextVersion(s source.Driver, m *gomigrate.Migrate) (uint, bool, error) {
	v, _, err := m.Version()
	var next uint
	switch {
	case errors.Is(err, gomigrate.ErrNilVersion):
		next, err = s.First()
	case err != nil:
		return 0, false, problem.Wrap(err, "INTERNAL", nil)
	default:
		next, err = s.Next(v)
	}
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, problem.Wrap(err, "INTERNAL", nil)
	}
	return next, true, nil
}
