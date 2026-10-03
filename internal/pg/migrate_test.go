package pg

import (
	"io"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

// widgetFS is a component's migrations directory: two migrations plus lifecycle.yaml.
func widgetFS() fstest.MapFS {
	return fstest.MapFS{
		"1_widget.up.sql":   {Data: []byte(`CREATE TABLE widget (id uuid PRIMARY KEY, name text NOT NULL);`)},
		"1_widget.down.sql": {Data: []byte(`DROP TABLE widget;`)},
		"2_note.up.sql":     {Data: []byte(`CREATE TABLE note (id uuid PRIMARY KEY, body text NOT NULL);`)},
		"2_note.down.sql":   {Data: []byte(`DROP TABLE note;`)},
		"lifecycle.yaml":    {Data: []byte("tables:\n  widget: {class: master}\n  note: {class: master}\n")},
	}
}

func TestLatestVersion(t *testing.T) {
	v, err := LatestVersion(widgetFS())
	require.NoError(t, err)
	require.Equal(t, uint(2), v)
	v, err = LatestVersion(fstest.MapFS{"lifecycle.yaml": {Data: []byte("tables: {}\n")}})
	require.NoError(t, err)
	require.Zero(t, v, "no migrations: version 0")
}

func TestPlatformSQLConcatenatesTheReferenceDDL(t *testing.T) {
	sql, err := platformSQL("erp/o'k")
	require.NoError(t, err)
	order := []string{"01-platform-version", "02-outbox", "03-event-cursor", "04-idempotency", "05-jobs",
		"06-snapshots-and-backfills", "08-number-series", "09-lifecycle", "10-lifecycle-functions"} // 07: ensureAuthzProjection (CP-DB-04)
	last := -1
	for _, name := range order {
		i := strings.Index(sql, "-- be-sdk-go platform migration: ddl/"+name+".sql")
		require.Greater(t, i, last, name)
		last = i
	}
	require.NotContains(t, sql, "be_bus")
	upsert := `INSERT INTO besdk_platform_version (component, version) VALUES ('erp/o''k', 1)` +
		` ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, applied_at = now();`
	require.True(t, strings.HasSuffix(strings.TrimSpace(sql), upsert), "the version row comes last")
}

func TestPlatformSourceHasOneUpVersion(t *testing.T) {
	src, err := newPlatformSource("c/x")
	require.NoError(t, err)
	v, err := src.First()
	require.NoError(t, err)
	require.Equal(t, uint(PlatformVersion), v)
	_, err = src.Next(v)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = src.Prev(v)
	require.ErrorIs(t, err, os.ErrNotExist)
	r, ident, err := src.ReadUp(v)
	require.NoError(t, err)
	b, _ := io.ReadAll(r)
	require.Contains(t, string(b), "CREATE TABLE IF NOT EXISTS besdk_outbox")
	require.Equal(t, "besdk_platform", ident)
	_, _, err = src.ReadDown(v)
	require.ErrorIs(t, err, os.ErrNotExist, "the platform migration never goes down")
	require.NoError(t, src.Close())
}

func TestParseHeader(t *testing.T) {
	for line, want := range map[string]fileHeader{
		"-- be:contract after=3.1.0":      {Contract: true, After: "3.1.0"},
		"-- be:contract after=3.1.0-rc.1": {Contract: true, After: "3.1.0-rc.1"},
		"-- be:no-transaction":            {NoTx: true},
		"--be:no-transaction  ":           {NoTx: true},
		"CREATE TABLE x (id int);":        {},
		"-- a plain comment":              {},
	} {
		got, err := parseHeader(line)
		require.NoError(t, err, line)
		require.Equal(t, want, got, line)
	}
	for _, bad := range []string{"-- be:contract", "-- be:contract after=3.x", "-- be:contract after=", "-- be:contract before=1.0.0",
		"-- be:no-transactions", "-- be:whatever"} {
		_, err := parseHeader(bad)
		require.Error(t, err, bad)
	}
}

func TestCompareSemver(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want int
	}{
		{"3.0.0", "3.1.0", -1}, {"3.1.0", "3.1.0", 0}, {"3.10.0", "3.9.9", 1}, {"3.1.0-rc.1", "3.1.0", -1},
		{"3.1.0-rc.2", "3.1.0-rc.10", -1}, {"3.1.0-alpha", "3.1.0-alpha.1", -1}, {"3.1.0-rc.1", "3.1.0-beta", 1},
		{"3.1.0+build.5", "3.1.0", 0}, {"3.1.0-1", "3.1.0-a", -1},
	} {
		got, err := compareSemver(c.a, c.b)
		require.NoError(t, err)
		require.Equal(t, c.want, got, "%s vs %s", c.a, c.b)
	}
	for _, bad := range []string{"3.1", "v3.1.0", "3.01.0", "3.1.0-", "x"} {
		_, err := compareSemver(bad, "1.0.0")
		require.Error(t, err, bad)
	}
}

func TestMigrationHeadersAreValidatedUpFront(t *testing.T) {
	fsys := func(extra fstest.MapFS) fstest.MapFS {
		f := widgetFS()
		for k, v := range extra {
			f[k] = v
		}
		return f
	}
	contract := fsys(fstest.MapFS{"3_drop.up.sql": {Data: []byte("-- be:contract after=3.0.0\nALTER TABLE note DROP COLUMN body;")}})
	h, err := migrationHeaders(contract, "3.1.0")
	require.NoError(t, err)
	require.Equal(t, fileHeader{Contract: true, After: "3.0.0"}, h[3])
	require.Equal(t, fileHeader{}, h[1])

	_, err = migrationHeaders(contract, "3.0.0")
	require.ErrorContains(t, err, "3_drop.up.sql", "after must be below the component's own version")
	_, err = migrationHeaders(contract, "")
	require.ErrorContains(t, err, "version")
	_, err = migrationHeaders(fsys(fstest.MapFS{"3_x.up.sql": {Data: []byte("-- be:contract after=one")}}), "3.1.0")
	require.ErrorContains(t, err, "3_x.up.sql")
	_, err = migrationHeaders(fsys(fstest.MapFS{"3_idx.up.sql": {Data: []byte("-- be:no-transaction\nCREATE INDEX CONCURRENTLY a ON note (body);\nCREATE INDEX CONCURRENTLY b ON note (id);")}}), "3.1.0")
	require.ErrorContains(t, err, "3_idx.up.sql")
	h, err = migrationHeaders(fsys(fstest.MapFS{"3_idx.up.sql": {Data: []byte("-- be:no-transaction\n-- the body index\nCREATE INDEX CONCURRENTLY IF NOT EXISTS a ON note (body);\n")}}), "")
	require.NoError(t, err, "a no-transaction file needs no version")
	require.True(t, h[3].NoTx)
}
