package db

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

// Lock waits of the migrate entry (protocol 14, "parameters"): each attempt waits this long for a lock, and after the
// last attempt the blockers are reported.
const (
	MigrationLockWait     = 3 * time.Second
	MigrationLockAttempts = 5
)

// MigrationSet is one sequence of migration files with its own state table in the component's schema.
type MigrationSet struct {
	Name  string // for logs: "be-sdk" or the component ID
	Table string // the state table
	Files fs.FS  // <version>_<title>.up.sql at its root
}

// SDKMigrations are the SDK's own tables (document number series; later the outbox, inbox and work items), shared by
// every SDK through the protocol module (P5.12). They run before the component's and keep their own state table, so
// the two version sequences never collide.
func SDKMigrations() MigrationSet {
	sub, _ := fs.Sub(protocol.Migrations, "migrations")
	return MigrationSet{Name: "be-sdk", Table: "be_sdk_migrations", Files: sub}
}

// MigrateConfig is the connection of the migrate entry: the owner role, directly to the database (never through a
// transaction-mode pooler).
type MigrateConfig struct {
	Target   Target
	User     string
	Password *config.Secret
	Schema   string
	Member   string
	Log      *slog.Logger
	LockWait time.Duration
	Attempts int
}

// MigrateConfigFrom reads the migrate entry's settings; owner credentials are required here and read nowhere else.
func MigrateConfigFrom(member string, v *config.Values, log *slog.Logger) (MigrateConfig, []config.Problem) {
	var problems []config.Problem
	if v.String("PG_OWNER_USER") == "" {
		problems = append(problems, config.Problem{Key: "PG_OWNER_USER", Kind: "missing", Expected: "the owner role, required by migrate"})
	}
	if v.Secret("PG_OWNER_PASSWORD") == nil {
		problems = append(problems, config.Problem{Key: "PG_OWNER_PASSWORD", Kind: "missing", Expected: "the owner password, required by migrate"})
	}
	host, port := v.String("PG_MIGRATION_HOST"), int(v.Int("PG_MIGRATION_PORT"))
	if host == "" {
		host = v.String("PG_HOST")
	}
	if port == 0 {
		port = int(v.Int("PG_PORT"))
	}
	return MigrateConfig{Target: Target{Host: host, Port: port, Database: v.String("PG_DATABASE")},
		User: v.String("PG_OWNER_USER"), Password: v.Secret("PG_OWNER_PASSWORD"), Schema: v.String("PG_SCHEMA"),
		Member: member, Log: log, LockWait: MigrationLockWait, Attempts: MigrationLockAttempts}, problems
}

// Migrate checks and applies the sets in order. Running it again with nothing new succeeds without changes.
func Migrate(ctx context.Context, c MigrateConfig, sets ...MigrationSet) error {
	for _, set := range sets {
		if problems := LintMigrations(set.Files, c.Schema); len(problems) > 0 {
			lines := make([]string, len(problems))
			for i, p := range problems {
				lines[i] = p.String()
			}
			return fmt.Errorf("%s: refused migrations:\n  %s", set.Name, strings.Join(lines, "\n  "))
		}
	}
	for _, set := range sets {
		if err := migrateSet(ctx, c, set); err != nil {
			return err
		}
	}
	return nil
}

func migrateSet(ctx context.Context, c MigrateConfig, set MigrationSet) error {
	for attempt := 1; ; attempt++ {
		blockers, err := applyOnce(ctx, c, set)
		if err == nil {
			return nil
		}
		if !lockTimeout(err) {
			return err
		}
		if attempt >= c.Attempts {
			return fmt.Errorf("%s: a lock was not granted within %s, %d times; blocked by: %s", set.Name, c.LockWait,
				attempt, describeBlockers(blockers))
		}
		c.Log.Warn("migration waiting for a lock; retrying", "set", set.Name, "attempt", attempt,
			"blocked_by", describeBlockers(blockers))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// applyOnce runs one attempt on one connection: lock_timeout bounds every lock wait (the migration lock included),
// and a watcher on a second connection records who blocks it.
func applyOnce(ctx context.Context, c MigrateConfig, set MigrationSet) (blockers []blocker, err error) {
	cc, err := c.connConfig()
	if err != nil {
		return nil, err
	}
	sqlDB := stdlib.OpenDB(*cc)
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1) // the driver's connection is the one whose pid is watched
	var pid int
	if err := sqlDB.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		return nil, fmt.Errorf("%s: connecting to %s as %s: %w", set.Name, c.Target, c.User, err)
	}
	w := watch(ctx, cc, pid)
	defer func() { blockers = w.stop() }()

	driver, err := pgxmigrate.WithInstance(sqlDB, &pgxmigrate.Config{SchemaName: c.Schema, MigrationsTable: set.Table,
		DatabaseName: c.Target.Database})
	if err != nil {
		return nil, fmt.Errorf("%s: preparing the state table: %w", set.Name, err)
	}
	src, err := iofs.New(set.Files, ".")
	if err != nil {
		return nil, fmt.Errorf("%s: reading the migration files: %w", set.Name, err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", set.Name, err)
	}
	m.LockTimeout = c.LockWait + time.Second // the database's lock_timeout answers first
	err = m.Up()
	if err == nil || errors.Is(err, migrate.ErrNoChange) {
		v, _, _ := m.Version()
		c.Log.Info("migrations applied", "set", set.Name, "schema_version", v)
		return nil, nil
	}
	var dirty migrate.ErrDirty
	if errors.As(err, &dirty) {
		return nil, fmt.Errorf("%s: version %d is marked dirty by an earlier run that stopped mid-way; check whether "+
			"%s was applied, then correct the state table %s.%s", set.Name, dirty.Version,
			fileOf(set.Files, uint(dirty.Version)), c.Schema, set.Table)
	}
	if !ranAFile(err) {
		return nil, fmt.Errorf("%s: %w", set.Name, err)
	}
	// This run's file failed and left no trace (each file is one implicit transaction; the lint refuses transaction
	// control), so its state goes back to the file before it and the entry stays rerunnable. Done under the
	// migration lock and only if the state is still this run's dirty mark: a runner holds the lock for its whole run.
	file, rerr := resetFailed(driver, src)
	if rerr != nil {
		return nil, fmt.Errorf("%s: a migration failed (%w), and resetting its state failed: %v", set.Name, err, rerr)
	}
	return nil, &fileError{set: set.Name, file: fileOf(set.Files, file), err: err}
}

func ranAFile(err error) bool {
	var de database.Error
	return errors.As(err, &de) && strings.HasPrefix(de.Err, "migration failed")
}

func resetFailed(driver database.Driver, src source.Driver) (uint, error) {
	if err := driver.Lock(); err != nil {
		return 0, err
	}
	defer func() { _ = driver.Unlock() }()
	v, dirty, err := driver.Version()
	if err != nil || !dirty || v < 0 {
		return uint(max(v, 0)), err
	}
	prev := database.NilVersion
	if p, perr := src.Prev(uint(v)); perr == nil {
		prev = int(p)
	}
	return uint(v), driver.SetVersion(prev, false)
}

type fileError struct {
	set, file string
	err       error
}

func (e *fileError) Error() string {
	return fmt.Sprintf("%s: migration %s failed: %v", e.set, e.file, e.err)
}
func (e *fileError) Unwrap() error { return e.err }

func (c MigrateConfig) connConfig() (*pgx.ConnConfig, error) {
	cc, err := pgx.ParseConfig("postgres://" + net.JoinHostPort(c.Target.Host, strconv.Itoa(c.Target.Port)) + "/" +
		c.Target.Database + "?sslmode=prefer")
	if err != nil {
		return nil, fmt.Errorf("migration target %s: %w", c.Target, err)
	}
	cc.User = c.User
	if c.Password != nil {
		cc.Password = c.Password.Value()
	}
	cc.RuntimeParams["application_name"] = c.Member + " migrate"
	cc.RuntimeParams["search_path"] = pgx.Identifier{c.Schema}.Sanitize()
	cc.RuntimeParams["lock_timeout"] = strconv.FormatInt(c.LockWait.Milliseconds(), 10) + "ms"
	cc.RuntimeParams["timezone"] = "UTC"
	return cc, nil
}

func lockTimeout(err error) bool {
	for err != nil {
		var pg *pgconn.PgError
		if errors.As(err, &pg) {
			return pg.Code == "55P03"
		}
		var de database.Error
		if errors.As(err, &de) {
			err = de.OrigErr
			continue
		}
		var dp *database.Error
		if errors.As(err, &dp) {
			err = dp.OrigErr
			continue
		}
		return errors.Is(err, migrate.ErrLockTimeout)
	}
	return false
}

func fileOf(files fs.FS, version uint) string {
	entries, _ := fs.ReadDir(files, ".")
	for _, e := range entries {
		if i := strings.IndexByte(e.Name(), '_'); i > 0 && strings.HasSuffix(e.Name(), ".up.sql") {
			if v, err := strconv.ParseUint(e.Name()[:i], 10, 64); err == nil && uint(v) == version {
				return e.Name()
			}
		}
	}
	return fmt.Sprintf("version %d", version)
}

type blocker struct {
	PID         int
	User, App   string
	State       string
	Query       string
	XactSeconds float64
}

func describeBlockers(bs []blocker) string {
	if len(bs) == 0 {
		return "unknown (the blocker had finished before it was seen)"
	}
	parts := make([]string, len(bs))
	for i, b := range bs {
		// another role's state and query are visible only with pg_read_all_stats, which the owner role does not hold:
		// pid, user and application name still identify the blocker
		if b.State == "" {
			parts[i] = fmt.Sprintf("pid %d (user %s, application %q)", b.PID, b.User, b.App)
			continue
		}
		parts[i] = fmt.Sprintf("pid %d (user %s, application %q, %s, in transaction %.0fs: %s)", b.PID, b.User, b.App,
			b.State, b.XactSeconds, short(b.Query))
	}
	return strings.Join(parts, "; ")
}

type watcher struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	last   []blocker
}

// watch polls pg_blocking_pids of the migrating backend from a second connection and keeps the latest non-empty
// answer, so a lock timeout can name who held the lock.
func watch(ctx context.Context, cc *pgx.ConnConfig, pid int) *watcher {
	ctx, cancel := context.WithCancel(ctx)
	w := &watcher{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(w.done)
		wc := cc.Copy()
		wc.RuntimeParams["application_name"] = cc.RuntimeParams["application_name"] + " watcher"
		delete(wc.RuntimeParams, "lock_timeout")
		conn, err := pgx.ConnectConfig(ctx, wc)
		if err != nil {
			return
		}
		defer conn.Close(context.WithoutCancel(ctx))
		t := time.NewTicker(250 * time.Millisecond)
		defer t.Stop()
		for {
			if bs := blockersOf(ctx, conn, pid); len(bs) > 0 {
				w.mu.Lock()
				w.last = bs
				w.mu.Unlock()
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return w
}

func (w *watcher) stop() []blocker {
	w.cancel()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.last
}

func blockersOf(ctx context.Context, conn *pgx.Conn, pid int) []blocker {
	rows, err := conn.Query(ctx, `SELECT pid, coalesce(usename, ''), application_name, coalesce(state, ''),
		coalesce(query, ''), coalesce(extract(epoch FROM now() - xact_start), 0)::float8
		FROM pg_stat_activity WHERE pid = ANY(pg_blocking_pids($1)) ORDER BY pid`, pid)
	if err != nil {
		return nil
	}
	bs, _ := pgx.CollectRows(rows, func(r pgx.CollectableRow) (blocker, error) {
		var b blocker
		err := r.Scan(&b.PID, &b.User, &b.App, &b.State, &b.Query, &b.XactSeconds)
		return b, err
	})
	return bs
}
