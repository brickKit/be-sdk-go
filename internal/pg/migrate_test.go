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
		"06-snapshots-and-backfills", "07-authz-projection", "08-number-series", "09-lifecycle", "10-lifecycle-functions"}
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
