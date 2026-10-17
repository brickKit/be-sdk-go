// Package pgtest prepares the integration tests' database (build tag "integration"): a PostgreSQL 16+ superuser DSN
// in BE_SDK_TEST_PG_ADMIN (the assembly project's be-postgres). Without it the tests fail: a test that did not run is
// not a pass (protocol 18).
package pgtest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
)

// Database is the test database; it is created on first use and never dropped.
const Database = "be_sdk_test"

// Admin connects as the superuser to database ("" for the DSN's own).
func Admin(t testing.TB, database string) *pgx.Conn {
	t.Helper()
	dsn := os.Getenv("BE_SDK_TEST_PG_ADMIN")
	if dsn == "" {
		t.Fatal("BE_SDK_TEST_PG_ADMIN is not set: integration tests need a PostgreSQL superuser DSN")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if database != "" {
		cfg.Database = database
	}
	c, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// Exec runs statements, failing the test on the first error.
func Exec(t testing.TB, c *pgx.Conn, sqls ...string) {
	t.Helper()
	for _, s := range sqls {
		if _, err := c.Exec(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// EnsureDatabase creates a database if missing and returns the server's host and port.
func EnsureDatabase(t testing.TB, name string) (string, int) {
	t.Helper()
	a := Admin(t, "")
	var exists bool
	if err := a.QueryRow(context.Background(), "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1)", name).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		Exec(t, a, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	}
	cfg := a.Config()
	return cfg.Host, int(cfg.Port)
}

// Role creates a login role with a password if missing.
func Role(t testing.TB, c *pgx.Conn, name, password string) {
	t.Helper()
	Exec(t, c, fmt.Sprintf(`DO $$ BEGIN
	  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname=%s) THEN CREATE ROLE %s LOGIN PASSWORD %s; END IF;
	END $$`, quote(name), pgx.Identifier{name}.Sanitize(), quote(password)))
}

// Member provisions a component the way the assembly's provisioning script does: an empty schema owned by a login
// owner role (sdk_<name>_owner), a runtime role (sdk_<name>_rw) with usage and default table privileges, and, when
// shellRole is set, the runtime role granted to the shell's login role WITH INHERIT FALSE, SET TRUE. The schema is
// dropped first.
func Member(t testing.TB, name, shellRole string) {
	t.Helper()
	c := Admin(t, Database)
	owner, rw := "sdk_"+name+"_owner", "sdk_"+name+"_rw"
	Role(t, c, owner, name+"-owner-pw")
	Role(t, c, rw, name+"-pw")
	Exec(t, c,
		"DROP SCHEMA IF EXISTS "+name+" CASCADE",
		"GRANT CONNECT ON DATABASE "+Database+" TO "+owner+", "+rw,
		"CREATE SCHEMA "+name+" AUTHORIZATION "+owner,
		"GRANT USAGE ON SCHEMA "+name+" TO "+rw,
		"ALTER DEFAULT PRIVILEGES FOR ROLE "+owner+" IN SCHEMA "+name+" GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "+rw)
	if shellRole != "" {
		Exec(t, c, "GRANT "+rw+" TO "+shellRole+" WITH INHERIT FALSE, SET TRUE")
	}
}

// MemberEnv is a member's database configuration as BrickKit would inject it.
func MemberEnv(host string, port int, name string) map[string]string {
	return map[string]string{"PG_HOST": host, "PG_PORT": strconv.Itoa(port), "PG_DATABASE": Database,
		"PG_USER": "sdk_" + name + "_rw", "PG_PASSWORD": name + "-pw", "PG_SCHEMA": name,
		"PG_OWNER_USER": "sdk_" + name + "_owner", "PG_OWNER_PASSWORD": name + "-owner-pw"}
}

func quote(s string) string { return "'" + s + "'" }
