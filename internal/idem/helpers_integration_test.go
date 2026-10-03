package idem

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

const componentID = "conformance/widget"

// noopMigration is the component's own (empty) migration; the platform migration brings
// besdk_idempotency.
var noopMigration = fstest.MapFS{
	"1_init.up.sql":   {Data: []byte(`SELECT 1`)},
	"1_init.down.sql": {Data: []byte(`SELECT 1`)},
}

// env is one integration test's world: a fresh schema with the besdk_* tables and a store bound to
// the runtime role.
type env struct {
	id    testpg.Identity
	super *sql.DB // the superuser, for assertions only
	store *pg.Store
}

// majors runs fn once per PostgreSQL major the protocol supports.
func majors(t *testing.T, fn func(t *testing.T, e *env)) {
	for _, m := range []string{"16", "14"} {
		t.Run("pg"+m, func(t *testing.T) { fn(t, newEnv(t, m)) })
	}
}

func newEnv(t *testing.T, major string) *env {
	t.Helper()
	e := &env{id: testpg.NewOn(t, major)}
	e.super = testpg.Open(t, e.id.SuperDSN)
	e.super.SetMaxOpenConns(2)
	_, err := pg.MigrateUp(within(t, 30*time.Second), pg.MigrateConfig{Host: e.id.Host, Port: e.id.Port,
		Database: e.id.Database, SSLMode: "disable", Owner: e.id.Owner, OwnerPassword: e.id.OwnerPassword,
		Schema: e.id.Schema, ComponentID: componentID, Component: noopMigration})
	require.NoError(t, err)
	p, err := pg.OpenPool(pg.PoolConfig{Host: e.id.Host, Port: e.id.Port, Database: e.id.Database,
		User: e.id.User, Password: func() string { return e.id.Password }, SSLMode: "disable", MaxConns: 24})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	e.store = pg.NewStore(p, pg.StoreConfig{ComponentID: componentID, Role: e.id.User, Schema: e.id.Schema, Budget: 24})
	return e
}

func within(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// tx runs fn in one transaction of the store.
func (e *env) tx(t *testing.T, fn func(ctx context.Context, tx *pg.Tx) error) error {
	t.Helper()
	return e.store.Run(within(t, 20*time.Second), pg.TxOptions{}, fn)
}

// scan runs a query as the superuser on the test schema and scans one row.
func (e *env) scan(t *testing.T, query string, args []any, dest ...any) {
	t.Helper()
	q := strings.ReplaceAll(query, "SCHEMA.", `"`+e.id.Schema+`".`)
	require.NoError(t, e.super.QueryRow(q, args...).Scan(dest...))
}

func (e *env) exec(t *testing.T, query string, args ...any) {
	t.Helper()
	_, err := e.super.Exec(strings.ReplaceAll(query, "SCHEMA.", `"`+e.id.Schema+`".`), args...)
	require.NoError(t, err)
}

// count is the number of besdk_idempotency rows.
func (e *env) count(t *testing.T) int {
	var n int
	e.scan(t, `SELECT count(*) FROM SCHEMA.besdk_idempotency`, nil, &n)
	return n
}

// t0 is the fixed runtime clock of the tests (microsecond precision, as PostgreSQL stores it).
var t0 = time.Date(2026, 10, 2, 8, 0, 0, 123456000, time.UTC)

// cmd builds a command in namespace ns, hashing request; a first execution answers 201.
func cmd(t *testing.T, ns, key, name, target string, request any) Command {
	t.Helper()
	h, err := FingerprintValue(request)
	require.NoError(t, err)
	return Command{Caller: ns, Key: key, Name: name, Target: target, Hash: h, Status: 201}
}
