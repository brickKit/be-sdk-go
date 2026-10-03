// Package testpg gives integration tests a fresh database identity in a throwaway PostgreSQL: a random
// owner role, a random runtime role and a random schema, created the way the project's database
// initialisation creates them (be-protocol P10 "Roles", scripts/ddltest.sh). Only test code imports it.
//
// The server comes from TEST_PG16_DSN (default) or TEST_PG14_DSN: a superuser DSN of a container the
// developer started (prefix sdkb-go-). A test is skipped when the variable is unset, never silently
// passed: `make test-integration` sets them and fails when a container is missing.
package testpg

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // the pgx database/sql driver, name "pgx"
)

// Identity is one component's database identity on the test server.
type Identity struct {
	Host, Database       string
	Port                 int
	Owner, OwnerPassword string // PG_OWNER_USER and its password
	User, Password       string // PG_USER (runtime role) and its password
	Schema               string // PG_SCHEMA
	OwnerPasswordFile    string // a file holding OwnerPassword (PG_OWNER_PASSWORD_FILE)
	PasswordFile         string // a file holding Password (PG_PASSWORD_FILE)
	SuperDSN             string // the superuser DSN, for test assertions only
}

// Server selects the test server: "16" (TEST_PG16_DSN) or "14" (TEST_PG14_DSN).
func superDSN(t testing.TB, major string) string {
	t.Helper()
	key := "TEST_PG" + major + "_DSN"
	dsn := os.Getenv(key)
	if dsn == "" {
		t.Skipf("%s not set: integration test needs a throwaway PostgreSQL %s (see Makefile test-integration)", key, major)
	}
	return dsn
}

// New creates a fresh identity on PostgreSQL 16 and drops it when the test ends.
func New(t testing.TB) Identity { return NewOn(t, "16") }

// NewOn creates a fresh identity on the given major ("16" or "14").
func NewOn(t testing.TB, major string) Identity {
	t.Helper()
	dsn := superDSN(t, major)
	db := Open(t, dsn)
	suffix := randHex(t)
	id := Identity{
		Owner: "o_" + suffix, OwnerPassword: "po_" + suffix,
		User: "r_" + suffix, Password: "pr_" + suffix,
		Schema: "s_" + suffix, SuperDSN: dsn,
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse %s: %v", dsn, err)
	}
	id.Host = u.Hostname()
	id.Port, _ = strconv.Atoi(u.Port())
	if id.Port == 0 {
		id.Port = 5432
	}
	id.Database = trimSlash(u.Path)
	stmts := []string{
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, id.Owner, id.OwnerPassword),
		fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, id.User, id.Password),
		fmt.Sprintf(`CREATE SCHEMA %s`, id.Schema),
		fmt.Sprintf(`GRANT USAGE, CREATE ON SCHEMA %s TO %s`, id.Schema, id.Owner),
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, id.Schema, id.User),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s`, id.Owner, id.Schema, id.User),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s GRANT USAGE, SELECT ON SEQUENCES TO %s`, id.Owner, id.Schema, id.User),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO %s`, id.Owner, id.Schema, id.User),
	}
	Exec(t, db, stmts...)
	dir := t.TempDir()
	id.PasswordFile = writeFile(t, dir, "PG_PASSWORD_FILE", id.Password+"\n")
	id.OwnerPasswordFile = writeFile(t, dir, "PG_OWNER_PASSWORD_FILE", id.OwnerPassword+"\n")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx, fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename IN ('%s','%s')`, id.Owner, id.User))
		for _, s := range []string{
			fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, id.Schema),
			fmt.Sprintf(`DROP OWNED BY %s, %s CASCADE`, id.Owner, id.User),
			fmt.Sprintf(`DROP ROLE IF EXISTS %s`, id.User),
			fmt.Sprintf(`DROP ROLE IF EXISTS %s`, id.Owner),
		} {
			if _, err := db.ExecContext(ctx, s); err != nil {
				t.Logf("testpg cleanup %q: %v", s, err)
			}
		}
	})
	return id
}

// ShellRole creates a NOINHERIT login role granted the given runtime roles WITH INHERIT FALSE, SET TRUE
// (P19.5; PostgreSQL 16 only) and returns its name and password.
func ShellRole(t testing.TB, ids ...Identity) (role, password string) {
	t.Helper()
	if len(ids) == 0 {
		t.Fatal("ShellRole needs at least one member identity")
	}
	db := Open(t, ids[0].SuperDSN)
	suffix := randHex(t)
	role, password = "sh_"+suffix, "ps_"+suffix
	Exec(t, db, fmt.Sprintf(`CREATE ROLE %s LOGIN NOINHERIT PASSWORD '%s'`, role, password))
	for _, id := range ids {
		Exec(t, db, fmt.Sprintf(`GRANT %s TO %s WITH INHERIT FALSE, SET TRUE`, id.User, role))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = db.ExecContext(ctx, fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '%s'`, role))
		_, _ = db.ExecContext(ctx, fmt.Sprintf(`DROP ROLE IF EXISTS %s`, role))
	})
	return role, password
}

// DSN returns a DSN for the given role on the identity's server and database.
func (id Identity) DSN(role, password string) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(role, password),
		Host: fmt.Sprintf("%s:%d", id.Host, id.Port), Path: "/" + id.Database, RawQuery: "sslmode=disable"}
	return u.String()
}

// Open opens a database/sql handle (driver pgx) and closes it when the test ends.
func Open(t testing.TB, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Exec runs statements, failing the test on the first error.
func Exec(t testing.TB, db *sql.DB, stmts ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
}

func randHex(t testing.TB) string {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func writeFile(t testing.TB, dir, name, content string) string {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func trimSlash(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}
