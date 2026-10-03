package lifecycle_test

import (
	"context"
	"database/sql"
	"io/fs"
	"sort"
	"testing"
	"testing/fstest"
	"time"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

const componentID = "conformance/widget"

var day0 = time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC) // a Saturday in ISO week 40

// widgetFS is the widget fixture's reference schema in golang-migrate's layout, plus its declaration.
func widgetFS(t *testing.T) fstest.MapFS {
	sql, err := fs.ReadFile(beprotocol.FS, "fixtures/widget/migrations/0001_widget.sql")
	require.NoError(t, err)
	yml, err := fs.ReadFile(beprotocol.FS, "fixtures/widget/migrations/lifecycle.yaml")
	require.NoError(t, err)
	return fstest.MapFS{
		"0001_widget.up.sql":   {Data: sql},
		"0001_widget.down.sql": {Data: []byte("SELECT 1;")},
		"lifecycle.yaml":       {Data: yml},
	}
}

// env is one test's schema: migrated (component + platform), a runtime store, a superuser handle.
type env struct {
	id    testpg.Identity
	store *pg.Store
	super *sql.DB
	decl  *lifecycle.Declaration
}

// newEnv migrates fsys into a fresh identity on the given major; withWindows passes the declaration
// to the platform migration (otherwise only the outbox window is created).
func newEnv(t *testing.T, major string, fsys fs.FS, withWindows bool) *env {
	t.Helper()
	id := testpg.NewOn(t, major)
	decl, err := lifecycle.Load(fsys)
	require.NoError(t, err)
	mc := pg.MigrateConfig{Host: id.Host, Port: id.Port, Database: id.Database, SSLMode: "disable",
		Owner: id.Owner, OwnerPassword: id.OwnerPassword, Schema: id.Schema, ComponentID: componentID,
		Version: "3.0.0", Component: fsys, Now: func() time.Time { return day0 }}
	if withWindows {
		mc.Lifecycle = decl
	}
	_, err = pg.MigrateUp(within(t, time.Minute), mc)
	require.NoError(t, err)
	p, err := pg.OpenPool(pg.PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User,
		Password: func() string { return id.Password }, SSLMode: "disable", MaxConns: 4})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	s := pg.NewStore(p, pg.StoreConfig{ComponentID: componentID, Role: id.User, Schema: id.Schema})
	super := testpg.Open(t, id.SuperDSN)
	return &env{id: id, store: s, super: super, decl: decl}
}

func within(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// run runs fn in one runtime transaction.
func (e *env) run(t *testing.T, fn func(ctx context.Context, tx *pg.Tx) error) error {
	t.Helper()
	return e.store.Run(within(t, 30*time.Second), pg.TxOptions{}, fn)
}

// partitions lists the partitions of parent in the env's schema, sorted.
func (e *env) partitions(t *testing.T, parent string) []string {
	t.Helper()
	rows, err := e.super.Query(`SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
	  JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace n ON n.oid = p.relnamespace
	  WHERE n.nspname = $1 AND p.relname = $2`, e.id.Schema, parent)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// scalar runs a superuser query in the env's schema and scans one value.
func (e *env) scalar(t *testing.T, into any, query string, args ...any) {
	t.Helper()
	tx, err := e.super.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.Exec(`SELECT set_config('search_path', $1, true)`, e.id.Schema)
	require.NoError(t, err)
	require.NoError(t, tx.QueryRow(query, args...).Scan(into))
}

// ownerConn is a dedicated connection as the owner with search_path = the env's schema.
func ownerConn(t *testing.T, e *env) *sql.Conn {
	t.Helper()
	db := testpg.Open(t, e.id.DSN(e.id.Owner, e.id.OwnerPassword))
	c, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	_, err = c.ExecContext(context.Background(), `SELECT set_config('search_path', $1, false)`, e.id.Schema)
	require.NoError(t, err)
	return c
}
