// Package dbtest gives integration tests a migrated component schema: provisioned like the assembly does it, the SDK's
// migrations and the test's own applied by the migrate entry, and a handle as the runtime role.
package dbtest

import (
	"context"
	"io"
	"io/fs"
	"log/slog"
	"testing"
	"testing/fstest"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
)

// Schema provisions schema name, applies the SDK's migrations and files (nil: a table items(id uuid)), and returns
// the member's handle and an admin connection whose search_path is the schema.
func Schema(t testing.TB, name string, files fs.FS) (*db.DB, *pgx.Conn) {
	t.Helper()
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, name, "")
	values, problems := config.Parse(db.MigrateKeys(), config.Map(pgtest.MemberEnv(host, port, name)))
	if len(problems) > 0 {
		t.Fatalf("%v", problems)
	}
	mc, problems := db.MigrateConfigFrom("demo/"+name, values, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if len(problems) > 0 {
		t.Fatalf("%v", problems)
	}
	if files == nil {
		files = fstest.MapFS{"0001_items.up.sql": {Data: []byte("CREATE TABLE items (id uuid PRIMARY KEY);")}}
	}
	if err := db.Migrate(context.Background(), mc, db.SDKMigrations(), db.MigrationSet{Name: "demo/" + name,
		Table: "schema_migrations", Files: files}); err != nil {
		t.Fatal(err)
	}
	s := db.SettingsFrom(values)
	s.Foreground, s.Background = 16, 16
	pool, err := db.Open(context.Background(), db.PoolConfig{Target: s.Target, User: s.User, Password: s.Password,
		AppName: "demo/" + name, MaxConns: 32, Prepared: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	admin := pgtest.Admin(t, pgtest.Database)
	pgtest.Exec(t, admin, "SET search_path = "+name)
	return db.NewDB("demo/"+name, pool, s), admin
}
