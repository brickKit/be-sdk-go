package pg

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Session parameters and retries of the migration connection (P11.1).
const (
	MigrateLockTimeout      = 5 * time.Second
	MigrateStatementTimeout = 15 * time.Minute
	MigrateLockRetries      = 3
)

// MigrateConfig is one migration step: the owner's dedicated, unpooled connection (P11.1).
type MigrateConfig struct {
	Host          string // PG_MIGRATION_HOST, already falling back to PG_HOST
	Port          int    // PG_MIGRATION_PORT, already falling back to PG_PORT; 0 = 5432
	Database      string
	SSLMode       string // "" = prefer
	Owner         string // PG_OWNER_USER
	OwnerPassword string // the content of PG_OWNER_PASSWORD_FILE
	Schema        string // PG_SCHEMA
	ComponentID   string
	Component     fs.FS // <version>_<name>.up.sql / .down.sql at the root, plus lifecycle.yaml
	Logger        *slog.Logger
	Now           func() time.Time // nil = time.Now; picks the outbox window's weeks
	OutboxAhead   int              // weeks after the current one; 0 = 2

	lockTimeout  time.Duration // tests only; 0 = MigrateLockTimeout
	retryBackoff time.Duration // tests only; 0 = 1 s
}

// MigrateResult says what a migration step found and did.
type MigrateResult struct {
	From, To          uint // the component version before and after; 0 = none
	Latest            uint // the image's version, LatestVersion(Component)
	Dirty             bool // the component state is dirty (only from MigrateStatus)
	Newer             bool // the schema is ahead of this image: nothing was changed (P1.8)
	PlatformVersion   int  // besdk_platform_version of this component after the step
	PartitionsCreated []string
}

func (c MigrateConfig) validate() error {
	if c.Host == "" || c.Database == "" || c.Owner == "" || c.Schema == "" || c.ComponentID == "" || c.Component == nil {
		return problem.Wrap(errors.New("migrate: Host, Database, Owner, Schema, ComponentID and Component are required"), "INTERNAL", nil)
	}
	return nil
}

func (c MigrateConfig) logger() *slog.Logger {
	if c.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return c.Logger.With("schema", c.Schema, "component_id", c.ComponentID)
}

func (c MigrateConfig) now() time.Time {
	if c.Now == nil {
		return time.Now()
	}
	return c.Now()
}

// runner holds one migration step's settings.
type runner struct {
	c       MigrateConfig
	log     *slog.Logger
	appName string // unique per step, so the lock watcher finds this step's backend
}

func newRunner(c MigrateConfig) (*runner, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return nil, problem.Wrap(err, "INTERNAL", nil)
	}
	return &runner{c: c, log: c.logger(), appName: "be-migrate-" + hex.EncodeToString(b) + " " + c.ComponentID}, nil
}

// openDB opens the owner's dedicated connection with the migration session parameters (P11.1);
// withSchema adds search_path = PG_SCHEMA (the migration tool's session-level setting, allowed here).
func (r *runner) openDB(withSchema bool) (*sql.DB, error) {
	cfg, err := connConfig(r.c.Host, r.c.Port, r.c.Database, r.c.Owner, r.c.SSLMode)
	if err != nil {
		return nil, err
	}
	cfg.Password = r.c.OwnerPassword
	cfg.RuntimeParams["TimeZone"] = "UTC"
	cfg.RuntimeParams["application_name"] = r.appName
	cfg.RuntimeParams["lock_timeout"] = fmt.Sprintf("%dms", orDefaultDuration(r.c.lockTimeout, MigrateLockTimeout).Milliseconds())
	cfg.RuntimeParams["statement_timeout"] = fmt.Sprintf("%dms", MigrateStatementTimeout.Milliseconds())
	if withSchema {
		cfg.RuntimeParams["search_path"] = pgx.Identifier{r.c.Schema}.Sanitize()
	}
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(1)
	return db, nil
}

// state reads the schema's migration state without creating anything.
func (r *runner) state(ctx context.Context) (version uint, dirty bool, platform int, err error) {
	db, err := r.openDB(true)
	if err != nil {
		return 0, false, 0, err
	}
	defer func() { _ = db.Close() }()
	var hasState, hasPlatform bool
	err = db.QueryRowContext(ctx, `SELECT to_regclass(format('%I.%I', $1::text, $2::text)) IS NOT NULL,
	    to_regclass(format('%I.besdk_platform_version', $1::text)) IS NOT NULL`,
		r.c.Schema, ComponentStateTable(r.c.Schema)).Scan(&hasState, &hasPlatform)
	if err != nil {
		return 0, false, 0, problem.Wrap(err, "INTERNAL", nil)
	}
	if hasState {
		var v int64
		err = db.QueryRowContext(ctx, `SELECT version, dirty FROM `+pgx.Identifier{ComponentStateTable(r.c.Schema)}.Sanitize()+` LIMIT 1`).Scan(&v, &dirty)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, false, 0, problem.Wrap(err, "INTERNAL", nil)
		}
		version = uint(max(v, 0))
	}
	if hasPlatform {
		err = db.QueryRowContext(ctx, `SELECT version FROM besdk_platform_version WHERE component = $1`, r.c.ComponentID).Scan(&platform)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, false, 0, problem.Wrap(err, "INTERNAL", nil)
		}
	}
	return version, dirty, platform, nil
}

// MigrateStatus reports the schema's migration state and the image's latest version; it changes
// nothing (P11.1).
func MigrateStatus(ctx context.Context, c MigrateConfig) (MigrateResult, error) {
	r, err := newRunner(c)
	if err != nil {
		return MigrateResult{}, err
	}
	latest, err := LatestVersion(c.Component)
	if err != nil {
		return MigrateResult{}, err
	}
	v, dirty, platform, err := r.state(ctx)
	if err != nil {
		return MigrateResult{}, err
	}
	return MigrateResult{From: v, To: v, Latest: latest, Dirty: dirty, Newer: v > latest, PlatformVersion: platform}, nil
}
