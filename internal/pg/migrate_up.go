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

// MigrateUp runs the component's migrations, then the platform migration, then ensures the outbox's
// current partition window, all as the owner on a dedicated connection (P11.1, P11.3, P16.6). Running
// it twice changes nothing the second time. A schema newer than the image is left untouched with a
// WARN and no error (P1.8).
//
// Decision tree: invalid config → error; dirty state → error; schema newer than image → WARN, return;
// otherwise component up (each step retried on 55P03), platform up (same), window, result.
func MigrateUp(ctx context.Context, c MigrateConfig) (MigrateResult, error) {
	r, err := newRunner(c)
	if err != nil {
		return MigrateResult{}, err
	}
	latest, err := LatestVersion(c.Component)
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
		if res.To, err = r.up(ctx, func() (source.Driver, error) { return iofs.New(c.Component, ".") }, ComponentStateTable(c.Schema)); err != nil {
			return res, err
		}
		if res.To != latest {
			return res, problem.Wrap(fmt.Errorf("schema is at version %d, which the image's migrations do not contain (latest %d)", res.To, latest), "INTERNAL", nil)
		}
	}
	if _, err := r.up(ctx, func() (source.Driver, error) { return newPlatformSource(c.ComponentID) }, PlatformStateTable(c.Schema)); err != nil {
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
// (a cancelled step is never cut in half), and returns the version reached.
func (r *runner) up(ctx context.Context, src func() (source.Driver, error), table string) (uint, error) {
	m, closeFn, err := r.migrator(src, table)
	if err != nil {
		return 0, err
	}
	defer closeFn()
	for {
		if err := ctx.Err(); err != nil {
			return 0, problem.From(err)
		}
		err := r.withLockRetry(ctx, m, func() error { return m.Steps(1) })
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return 0, problem.From(err)
		}
	}
	v, _, err := m.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return 0, problem.Wrap(err, "INTERNAL", nil)
	}
	return v, nil
}
