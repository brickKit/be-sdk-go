//go:build integration

package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gamma is provisioned as the provisioning script would; the migrations themselves never grant.
func gammaFixture(t *testing.T) (MigrateConfig, *pgx.Conn) {
	t.Helper()
	host, port := fixture(t)
	pgtest.Member(t, "gamma", "")
	return MigrateConfig{Target: Target{Host: host, Port: port, Database: testDB}, User: "sdk_gamma_owner",
		Password: secret(t, "gamma-owner-pw"), Schema: "gamma", Member: "demo/gamma", Log: quiet(),
		LockWait: MigrationLockWait, Attempts: MigrationLockAttempts}, admin(t, testDB)
}

func files(m map[string]string) fstest.MapFS {
	out := fstest.MapFS{}
	for k, v := range m {
		out[k] = &fstest.MapFile{Data: []byte(v)}
	}
	return out
}

var gammaV2 = map[string]string{
	"0001_orders.up.sql": "CREATE TABLE orders (id uuid PRIMARY KEY, status text NOT NULL DEFAULT 'draft');",
	"0002_lines.up.sql":  "CREATE TABLE lines (order_id uuid NOT NULL REFERENCES orders, n int NOT NULL, PRIMARY KEY (order_id, n));",
}

func component(m map[string]string) MigrationSet {
	return MigrationSet{Name: "demo/gamma", Table: "schema_migrations", Files: files(m)}
}

func version(t *testing.T, c *pgx.Conn, table string) (v int64, dirty bool) {
	t.Helper()
	require.NoError(t, c.QueryRow(context.Background(), "SELECT version, dirty FROM gamma."+table).Scan(&v, &dirty))
	return
}

func TestMigrateTwiceAndStateInTheComponentSchema(t *testing.T) {
	cfg, c := gammaFixture(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, cfg, SDKMigrations(), component(gammaV2)))
	require.NoError(t, Migrate(ctx, cfg, SDKMigrations(), component(gammaV2)), "the second run has nothing to do")
	v, dirty := version(t, c, "schema_migrations")
	assert.Equal(t, int64(2), v)
	assert.False(t, dirty)
	v, _ = version(t, c, "be_sdk_migrations")
	assert.Equal(t, int64(1), v)
	var public int
	require.NoError(t, c.QueryRow(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '%migrations'").Scan(&public))
	assert.Zero(t, public, "no state table outside the component schema")

	// the runtime role works on the new tables through the provisioning script's default privileges
	host, port := cfg.Target.Host, cfg.Target.Port
	pool, err := Open(ctx, PoolConfig{Target: cfg.Target, User: "sdk_gamma_rw", Password: secret(t, "gamma-pw"), AppName: "demo/gamma", MaxConns: 2, Prepared: true})
	require.NoError(t, err)
	defer pool.Close()
	s := settings(host, port, "gamma", 2)
	d := NewDB("demo/gamma", pool, s)
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO orders (id) VALUES (gen_random_uuid())")
		return err
	}))
}

func TestAFailedFileLeavesNoTraceAndTheEntryStaysRerunnable(t *testing.T) {
	cfg, c := gammaFixture(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, cfg, SDKMigrations(), component(gammaV2)))
	broken := map[string]string{"0003_notes.up.sql": "CREATE TABLE notes (id int); ALTER TABLE missing ADD COLUMN x int;"}
	for k, v := range gammaV2 {
		broken[k] = v
	}
	err := Migrate(ctx, cfg, SDKMigrations(), component(broken))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "migration 0003_notes.up.sql failed")
	var exists bool
	require.NoError(t, c.QueryRow(ctx, "SELECT to_regclass('gamma.notes') IS NOT NULL").Scan(&exists))
	assert.False(t, exists, "the first statement of the failed file was rolled back with it")
	v, dirty := version(t, c, "schema_migrations")
	assert.Equal(t, int64(2), v)
	assert.False(t, dirty)

	broken["0003_notes.up.sql"] = "CREATE TABLE notes (id int);"
	require.NoError(t, Migrate(ctx, cfg, SDKMigrations(), component(broken)))
	v, _ = version(t, c, "schema_migrations")
	assert.Equal(t, int64(3), v)
}

func TestLockWaitReportsTheBlocker(t *testing.T) {
	cfg, c := gammaFixture(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, cfg, SDKMigrations(), component(gammaV2)))

	holder := admin(t, testDB)
	exec(t, holder, "SET application_name = 'long-report'")
	tx, err := holder.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "SELECT count(*) FROM gamma.orders") // ACCESS SHARE blocks the ALTER below
	require.NoError(t, err)

	next := map[string]string{"0003_status_index.up.sql": "ALTER TABLE orders ADD COLUMN note text;"}
	for k, v := range gammaV2 {
		next[k] = v
	}
	cfg.LockWait, cfg.Attempts = 400*time.Millisecond, 2
	start := time.Now()
	err = Migrate(ctx, cfg, SDKMigrations(), component(next))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a lock was not granted within 400ms, 2 times")
	var holderPID int
	require.NoError(t, holder.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&holderPID))
	assert.Contains(t, err.Error(), fmt.Sprintf(`pid %d (user postgres, application "long-report")`, holderPID))
	assert.Less(t, time.Since(start), 5*time.Second)
	v, dirty := version(t, c, "schema_migrations")
	assert.Equal(t, int64(2), v)
	assert.False(t, dirty, "the timed-out file was reset, so a rerun works")

	require.NoError(t, tx.Rollback(ctx))
	require.NoError(t, Migrate(ctx, cfg, SDKMigrations(), component(next)))
	v, _ = version(t, c, "schema_migrations")
	assert.Equal(t, int64(3), v)
}

func TestTwoInstancesMigrateAtOnce(t *testing.T) {
	cfg, c := gammaFixture(t)
	ctx := context.Background()
	slow := map[string]string{"0003_slow.up.sql": "SELECT pg_sleep(0.5); CREATE TABLE slow (id int);"}
	for k, v := range gammaV2 {
		slow[k] = v
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = Migrate(ctx, cfg, SDKMigrations(), component(slow)) }(i)
	}
	wg.Wait()
	assert.NoError(t, errs[0])
	assert.NoError(t, errs[1])
	v, dirty := version(t, c, "schema_migrations")
	assert.Equal(t, int64(3), v)
	assert.False(t, dirty)
}

func TestRefusedMigrationsChangeNothing(t *testing.T) {
	cfg, c := gammaFixture(t)
	err := Migrate(context.Background(), cfg, SDKMigrations(), component(map[string]string{
		"0001_orders.up.sql": "CREATE TABLE orders (id int); GRANT SELECT ON orders TO PUBLIC;"}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "0001_orders.up.sql: grants belong to the provisioning script")
	var n int
	require.NoError(t, c.QueryRow(context.Background(), "SELECT count(*) FROM information_schema.tables WHERE table_schema='gamma'").Scan(&n))
	assert.Zero(t, n)
}

func TestDocumentNumbers(t *testing.T) {
	cfg, _ := gammaFixture(t)
	ctx := context.Background()
	require.NoError(t, Migrate(ctx, cfg, SDKMigrations(), component(gammaV2)))
	pool, err := Open(ctx, PoolConfig{Target: cfg.Target, User: "sdk_gamma_rw", Password: secret(t, "gamma-pw"), AppName: "demo/gamma", MaxConns: 4, Prepared: true})
	require.NoError(t, err)
	defer pool.Close()
	d := NewDB("demo/gamma", pool, settings(cfg.Target.Host, cfg.Target.Port, "gamma", 4))
	inv := Series{LegalEntity: "LE1", Code: "INV", Period: "2026"}
	issue := func(fail bool) (n int64, err error) {
		err = d.Tx(ctx, func(ctx context.Context, tx Tx) error {
			if n, err = NextNumber(ctx, tx, inv); err != nil {
				return err
			}
			if fail {
				return assert.AnError
			}
			return nil
		})
		return n, err
	}
	n, err := issue(false)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	_, err = issue(true)
	require.ErrorIs(t, err, assert.AnError)
	n, err = issue(false)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n, "the rolled-back number was given back: no gap")

	// concurrent issuers serialise on the series row and never share a number
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[int64]bool{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := issue(false)
			if err != nil {
				return // SERVICE_BUSY under the budget is allowed; a duplicate is not
			}
			mu.Lock()
			assert.False(t, seen[n], "number %d issued twice", n)
			seen[n] = true
			mu.Unlock()
		}()
	}
	wg.Wait()

	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx Tx) error {
		return RegisterImported(ctx, tx, Series{LegalEntity: "LE1", Code: "SO"}, false, 5000)
	}))
	g, err := d.NextGappedNumber(ctx, Series{LegalEntity: "LE1", Code: "SO"})
	require.NoError(t, err)
	assert.Equal(t, int64(5001), g)
	_, err = d.NextGappedNumber(ctx, inv)
	assert.ErrorContains(t, err, "declared gapless=true but used as gapless=false")
}
