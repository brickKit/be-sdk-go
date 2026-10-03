package pg

import (
	"bytes"
	"context"
	"io/fs"
	"log/slog"
	"regexp"
	"sort"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var migrateNow = utc("2026-10-03T10:00:00Z")

func migrateConfig(id testpg.Identity, component fs.FS, log *slog.Logger) MigrateConfig {
	return MigrateConfig{Host: id.Host, Port: id.Port, Database: id.Database, SSLMode: "disable",
		Owner: id.Owner, OwnerPassword: id.OwnerPassword, Schema: id.Schema, ComponentID: "conformance/widget",
		Component: component, Logger: log, Now: func() time.Time { return migrateNow },
		AuthzProjection: true} // the widget declares resource types (CP-DB-04)
}

// referenceTables lists every table the reference DDL creates (be_bus.sql excluded).
func referenceTables(t *testing.T) []string {
	names, err := fs.Glob(beprotocol.FS, "ddl/[0-9]*.sql")
	require.NoError(t, err)
	re := regexp.MustCompile(`(?m)^CREATE TABLE IF NOT EXISTS (\w+)`)
	var out []string
	for _, n := range names {
		b, err := fs.ReadFile(beprotocol.FS, n)
		require.NoError(t, err)
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			out = append(out, m[1])
		}
	}
	return out
}

// tablesOf lists the tables (and partitions) a role owns in a schema.
func tablesOf(t *testing.T, id testpg.Identity, schema string) []string {
	db := testpg.Open(t, id.SuperDSN)
	rows, err := db.Query(`SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	  WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND pg_get_userbyid(c.relowner) = $2 ORDER BY 1`, schema, id.Owner)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		out = append(out, s)
	}
	return out
}

func TestMigrateUpTwice(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			id := testpg.NewOn(t, major)
			c := migrateConfig(id, widgetFS(), nil)
			r, err := MigrateUp(within(t, 60e9), c)
			require.NoError(t, err)
			require.Equal(t, MigrateResult{From: 0, To: 2, Latest: 2, PlatformVersion: 1,
				PartitionsCreated: []string{"besdk_outbox_2026w40", "besdk_outbox_2026w41", "besdk_outbox_2026w42"}}, r)

			want := append(referenceTables(t), "widget", "note", "schema_migrations_"+id.Schema, "besdk_migrations_"+id.Schema,
				"besdk_outbox_2026w40", "besdk_outbox_2026w41", "besdk_outbox_2026w42")
			sort.Strings(want)
			require.Equal(t, want, tablesOf(t, id, id.Schema))
			require.Empty(t, tablesOf(t, id, "public"), "nothing in public")

			r, err = MigrateUp(within(t, 60e9), c)
			require.NoError(t, err)
			require.Equal(t, MigrateResult{From: 2, To: 2, Latest: 2, PlatformVersion: 1}, r, "the second run changes nothing")
			require.Equal(t, want, tablesOf(t, id, id.Schema))

			// the runtime identity is sound and sees the versions; an outbox row goes in right away
			_, s := standalone(t, id, 1)
			pr := Probe(within(t, 10e9), s, id.Owner, false)
			require.NoError(t, pr.Err)
			require.Empty(t, pr.IdentityProblems)
			require.Equal(t, uint(2), pr.ComponentVersion)
			require.Equal(t, 1, pr.PlatformVersion)
			require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO besdk_outbox (id, created_at, subject, aggregate_type, aggregate_id,
				  aggregate_version, occurred_at, payload) VALUES ($1, now(), 'conformance.widget.created.v1',
				  'conformance.widget.widget', 'w1', 1, now(), '{}')`, uuid.Must(uuid.NewV7()))
				return err
			}), "the current week's partition exists")
		})
	}
}

func TestMigrateDownLeavesThePlatform(t *testing.T) {
	id := testpg.New(t)
	c := migrateConfig(id, widgetFS(), nil)
	_, err := MigrateUp(within(t, 60e9), c)
	require.NoError(t, err)
	r, err := MigrateDown(within(t, 60e9), c, 1)
	require.NoError(t, err)
	require.Equal(t, uint(2), r.From)
	require.Equal(t, uint(1), r.To)
	require.Equal(t, 1, r.PlatformVersion)
	require.NotContains(t, tablesOf(t, id, id.Schema), "note")
	require.Contains(t, tablesOf(t, id, id.Schema), "besdk_outbox")
	st, err := MigrateStatus(within(t, 60e9), c)
	require.NoError(t, err)
	require.Equal(t, MigrateResult{From: 1, To: 1, Latest: 2, PlatformVersion: 1}, st)
	_, err = MigrateDown(within(t, 60e9), c, 0)
	require.Error(t, err, "n must be positive")
}

func TestMigrateStatusOnEmptySchema(t *testing.T) {
	id := testpg.New(t)
	st, err := MigrateStatus(within(t, 60e9), migrateConfig(id, widgetFS(), nil))
	require.NoError(t, err)
	require.Equal(t, MigrateResult{Latest: 2}, st)
	require.Empty(t, tablesOf(t, id, id.Schema), "status creates nothing")
}

// A schema migrated by a newer image: MigrateUp logs WARN and succeeds without touching it (P1.8).
func TestMigrateUpOnNewerSchema(t *testing.T) {
	id := testpg.New(t)
	newer := widgetFS()
	newer["3_extra.up.sql"] = &fstest.MapFile{Data: []byte(`CREATE TABLE extra (id int);`)}
	newer["3_extra.down.sql"] = &fstest.MapFile{Data: []byte(`DROP TABLE extra;`)}
	_, err := MigrateUp(within(t, 60e9), migrateConfig(id, newer, nil))
	require.NoError(t, err)
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	r, err := MigrateUp(within(t, 60e9), migrateConfig(id, widgetFS(), log))
	require.NoError(t, err)
	require.True(t, r.Newer)
	require.Equal(t, uint(3), r.From)
	require.Equal(t, uint(2), r.Latest)
	require.Contains(t, buf.String(), `"level":"WARN"`)
	require.Contains(t, tablesOf(t, id, id.Schema), "extra")
}

// The running service extends the window through the SECURITY DEFINER function (P10.12, P16.6).
func TestEnsureOutboxWindowAsRuntimeRole(t *testing.T) {
	id := testpg.New(t)
	_, err := MigrateUp(within(t, 60e9), migrateConfig(id, widgetFS(), nil))
	require.NoError(t, err)
	_, s := standalone(t, id, 1)
	var created []string
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		created, err = EnsureOutboxWindow(ctx, tx, migrateNow.AddDate(0, 0, 14), 2)
		return err
	}))
	require.Equal(t, []string{"besdk_outbox_2026w43", "besdk_outbox_2026w44"}, created)
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		created, err = EnsureOutboxWindow(ctx, tx, migrateNow.AddDate(0, 0, 14), 2)
		return err
	}))
	require.Empty(t, created, "idempotent")
}

// Two components migrating one database at the same time both succeed: the lock is per schema (P11.1).
func TestMigrateConcurrentSchemas(t *testing.T) {
	a, b := testpg.New(t), testpg.New(t)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, id := range []testpg.Identity{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = MigrateUp(within(t, 60e9), migrateConfig(id, widgetFS(), nil))
		}()
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
}

// A schema at a version the image's files do not contain is an error, never a silent "up to date".
func TestMigrateUpRefusesAMissingVersion(t *testing.T) {
	id := testpg.New(t)
	_, err := MigrateUp(within(t, 60e9), migrateConfig(id, widgetFS(), nil))
	require.NoError(t, err)
	gap := fstest.MapFS{
		"1_widget.up.sql":   {Data: []byte(`CREATE TABLE widget (id uuid PRIMARY KEY, name text NOT NULL);`)},
		"1_widget.down.sql": {Data: []byte(`DROP TABLE widget;`)},
		"3_other.up.sql":    {Data: []byte(`CREATE TABLE other (id int);`)},
		"3_other.down.sql":  {Data: []byte(`DROP TABLE other;`)},
	}
	_, err = MigrateUp(within(t, 60e9), migrateConfig(id, gap, nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "version 2")
}

// fixtureFS is the widget fixture's reference schema in golang-migrate's layout, plus its
// lifecycle.yaml (be-protocol fixtures/widget).
func fixtureFS(t *testing.T) fstest.MapFS {
	sql, err := fs.ReadFile(beprotocol.FS, "fixtures/widget/migrations/0001_widget.sql")
	require.NoError(t, err)
	yml, err := fs.ReadFile(beprotocol.FS, "fixtures/widget/migrations/lifecycle.yaml")
	require.NoError(t, err)
	return fstest.MapFS{"0001_widget.up.sql": {Data: sql}, "0001_widget.down.sql": {Data: []byte("SELECT 1;")},
		"lifecycle.yaml": {Data: yml}}
}

// CP-LIFE-01: a database migrated on any day accepts writes that day. The platform migration creates,
// as the owner and after the platform DDL, the window of the outbox and of every partitioned table of
// lifecycle.yaml, followers included (P11.3, P16.6).
func TestMigrateCreatesTheDeclaredWindows(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			id := testpg.NewOn(t, major)
			fsys := fixtureFS(t)
			decl, err := lifecycle.Load(fsys)
			require.NoError(t, err)
			c := migrateConfig(id, fsys, nil)
			c.Lifecycle = decl
			r, err := MigrateUp(within(t, 60e9), c)
			require.NoError(t, err)
			for _, p := range []string{"besdk_outbox_2026w40", "widgets_2026_10_01", "widgets_2027_01_01", "widget_lines_2026_10_01",
				"widget_ledger_2026_12_01", "widget_audit_2026_11_01", "widget_jobs_2026_09_28", "widget_jobs_2026_10_12"} {
				require.Contains(t, r.PartitionsCreated, p)
			}
			require.Len(t, r.PartitionsCreated, 3+4*4+3)

			_, s := standalone(t, id, 1)
			require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO widget_jobs (id, created_at, widget_id, kind, state, updated_at)
				  VALUES ($1, now(), 'w1', 'approved', 'PENDING', now())`, uuid.Must(uuid.NewV7()))
				return err
			}), "a row for today goes in")

			r, err = MigrateUp(within(t, 60e9), c)
			require.NoError(t, err)
			require.Empty(t, r.PartitionsCreated, "the second run creates nothing")
		})
	}
}
