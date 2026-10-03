package pg

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

// contractFS is widgetFS plus a contract migration that needs every version <= 3.0.0 gone.
func contractFS() fstest.MapFS {
	f := widgetFS()
	f["3_drop_body.up.sql"] = &fstest.MapFile{Data: []byte("-- be:contract after=3.0.0\nALTER TABLE note DROP COLUMN body;")}
	f["3_drop_body.down.sql"] = &fstest.MapFile{Data: []byte("ALTER TABLE note ADD COLUMN body text;")}
	return f
}

// fakeBackend holds one idle session of the same database whose session-level application_name is
// name, as a running SDK pool connection of another version would be (P11.4 gating).
func fakeBackend(t *testing.T, id testpg.Identity, name string) (pid int, closeFn func()) {
	t.Helper()
	u, err := url.Parse(id.DSN(id.User, id.Password))
	require.NoError(t, err)
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	db, err := sql.Open("pgx", u.String())
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(context.Background(), `SELECT pg_backend_pid()`).Scan(&pid))
	closeFn = func() { _ = conn.Close(); _ = db.Close() }
	t.Cleanup(closeFn)
	return pid, closeFn
}

func contractConfig(id testpg.Identity, fsys fstest.MapFS) MigrateConfig {
	c := migrateConfig(id, fsys, nil)
	c.Version = "3.1.0"
	c.contractProbe = 300 * time.Millisecond
	return c
}

// A contract migration stops, before its file and with the earlier files applied, while a backend of
// this component at a version <= after is connected; it runs once that backend is gone. Newer
// versions and other components never block (P11.4).
func TestContractMigrationWaitsForOldVersions(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			id := testpg.NewOn(t, major)
			fakeBackend(t, id, "conformance/widget@3.1.0")
			fakeBackend(t, id, "other/component@1.0.0")
			pid, stop := fakeBackend(t, id, "conformance/widget@3.0.0")

			r, err := MigrateUp(within(t, 60e9), contractConfig(id, contractFS()))
			require.Error(t, err)
			require.Contains(t, err.Error(), "3_drop_body.up.sql")
			require.Contains(t, err.Error(), "3.0.0")
			require.Contains(t, err.Error(), fmt.Sprint(pid))
			require.Equal(t, uint(2), r.To, "the files before the contract are applied")
			st, err := MigrateStatus(within(t, 60e9), contractConfig(id, contractFS()))
			require.NoError(t, err)
			require.Equal(t, uint(2), st.From)

			stop()
			r, err = MigrateUp(within(t, 60e9), contractConfig(id, contractFS()))
			require.NoError(t, err)
			require.Equal(t, uint(3), r.To)
		})
	}
}

// A backend seen only under the bare member ID is inside a transaction (SET LOCAL application_name):
// its version is unknown, so it blocks too (fail closed).
func TestContractMigrationTreatsAnUnknownVersionAsOld(t *testing.T) {
	id := testpg.New(t)
	pid, _ := fakeBackend(t, id, "conformance/widget")
	_, err := MigrateUp(within(t, 60e9), contractConfig(id, contractFS()))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown")
	require.Contains(t, err.Error(), fmt.Sprint(pid))
}

// A malformed header stops the step before any file runs.
func TestMalformedHeaderStopsBeforeAnything(t *testing.T) {
	id := testpg.New(t)
	f := widgetFS()
	f["3_bad.up.sql"] = &fstest.MapFile{Data: []byte("-- be:contract after=soon\nSELECT 1;")}
	f["3_bad.down.sql"] = &fstest.MapFile{Data: []byte("SELECT 1;")}
	_, err := MigrateUp(within(t, 60e9), contractConfig(id, f))
	require.ErrorContains(t, err, "3_bad.up.sql")
	require.Empty(t, tablesOf(t, id, id.Schema), "nothing ran")
}

// golang-migrate's pgx driver sends a file as one simple-protocol query: several statements form one
// implicit transaction, so CREATE INDEX CONCURRENTLY must be alone in its file. Such a file runs as is.
func TestNoTransactionFileCreatesAnIndexConcurrently(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			id := testpg.NewOn(t, major)
			f := widgetFS()
			f["3_note_body.up.sql"] = &fstest.MapFile{Data: []byte("-- be:no-transaction\nCREATE INDEX CONCURRENTLY IF NOT EXISTS note_body ON note (body);\n")}
			f["3_note_body.down.sql"] = &fstest.MapFile{Data: []byte("DROP INDEX IF EXISTS note_body;")}
			r, err := MigrateUp(within(t, 60e9), contractConfig(id, f))
			require.NoError(t, err)
			require.Equal(t, uint(3), r.To)
			var valid bool
			db := testpg.Open(t, id.SuperDSN)
			require.NoError(t, db.QueryRow(`SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			  JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = 'note_body'`, id.Schema).Scan(&valid))
			require.True(t, valid)

			two := widgetFS()
			two["3_two.up.sql"] = &fstest.MapFile{Data: []byte("CREATE INDEX CONCURRENTLY a ON note (body);\nCREATE INDEX CONCURRENTLY b ON note (id);")}
			two["3_two.down.sql"] = &fstest.MapFile{Data: []byte("SELECT 1;")}
			other := testpg.NewOn(t, major)
			_, err = MigrateUp(within(t, 60e9), contractConfig(other, two))
			require.Error(t, err, "without the marker the driver's implicit transaction refuses CONCURRENTLY")
		})
	}
}
