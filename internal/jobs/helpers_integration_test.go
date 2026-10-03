package jobs

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

const componentID = "conformance/widget"

// componentMigration: the component's own tables the reconciler and dead-handler tests write.
var componentMigration = fstest.MapFS{
	"1_orders.up.sql":     {Data: []byte(`CREATE TABLE orders (id text PRIMARY KEY, state text NOT NULL)`)},
	"1_orders.down.sql":   {Data: []byte(`DROP TABLE orders`)},
	"2_dead_log.up.sql":   {Data: []byte(`CREATE TABLE dead_log (job_id text PRIMARY KEY, attempts int NOT NULL)`)},
	"2_dead_log.down.sql": {Data: []byte(`DROP TABLE dead_log`)},
}

// env is one integration test's schema with the besdk_job_* tables (platform migration).
type env struct {
	id    testpg.Identity
	super *sql.DB
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{id: testpg.New(t)}
	e.super = testpg.Open(t, e.id.SuperDSN)
	e.super.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := pg.MigrateUp(ctx, pg.MigrateConfig{Host: e.id.Host, Port: e.id.Port, Database: e.id.Database,
		SSLMode: "disable", Owner: e.id.Owner, OwnerPassword: e.id.OwnerPassword, Schema: e.id.Schema,
		ComponentID: componentID, Component: componentMigration})
	require.NoError(t, err)
	return e
}

// store opens a store of its own (one per simulated replica).
func (e *env) store(t *testing.T) *pg.Store {
	t.Helper()
	p, err := pg.OpenPool(pg.PoolConfig{Host: e.id.Host, Port: e.id.Port, Database: e.id.Database, User: e.id.User,
		Password: func() string { return e.id.Password }, SSLMode: "disable", MaxConns: 8})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return pg.NewStore(p, pg.StoreConfig{ComponentID: componentID, Role: e.id.User, Schema: e.id.Schema, Budget: 8})
}

// engine builds a replica's engine; tweak adjusts its configuration.
func (e *env) engine(t *testing.T, instance string, d Declarations, tweak ...func(*Config)) *Engine {
	t.Helper()
	cfg := Config{Store: e.store(t), ComponentID: componentID, InstanceID: instance, Poll: 50 * time.Millisecond}
	for _, f := range tweak {
		f(&cfg)
	}
	eng, err := New(cfg, d)
	require.NoError(t, err)
	return eng
}

// scan runs a query as the superuser on the test schema ("SCHEMA." prefix) and scans one row.
func (e *env) scan(t *testing.T, query string, dest ...any) {
	t.Helper()
	require.NoError(t, e.super.QueryRow(e.q(query)).Scan(dest...))
}

func (e *env) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	_, err := e.super.Exec(e.q(query), args...)
	require.NoError(t, err)
}

func (e *env) q(query string) string {
	return strings.ReplaceAll(query, "SCHEMA.", `"`+e.id.Schema+`".`)
}

func (e *env) count(t *testing.T, query string) int {
	t.Helper()
	var n int
	e.scan(t, query, &n)
	return n
}

// runLoops starts every loop of eng (as the root's supervisor would, without restarts) and returns a
// function that stops them and waits; a loop returning an error fails the test.
func runLoops(t *testing.T, eng *Engine) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, l := range eng.Loops() {
		wg.Add(1)
		go func(l Loop) {
			defer wg.Done()
			if err := l.Run(ctx); err != nil {
				t.Errorf("loop %s: %v", l.Name, err)
			}
		}(l)
	}
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); wg.Wait() }) }
	t.Cleanup(stop)
	return stop
}

// eventually polls cond every 20 ms for up to d.
func eventually(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("not within %s: %s", d, msg)
}

func within(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
