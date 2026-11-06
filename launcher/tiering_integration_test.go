//go:build integration

package launcher

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noRoutes struct{}

func (noRoutes) Routes(*httpx.Routes) error { return nil }

func tierClass() tiering.Class {
	return tiering.Class{Name: "demo.notes", Category: tiering.CategoryCompleted,
		Root: tiering.Table{Name: "notes", PK: "id", Columns: map[string]string{"id": tiering.TypeUUID,
			"closed_at": tiering.TypeTimestamptz, "created_at": tiering.TypeTimestamptz}},
		Children: []tiering.Child{{Table: tiering.Table{Name: "note_lines", PK: "id", Columns: map[string]string{
			"id": tiering.TypeUUID, "note_id": tiering.TypeUUID}}, FK: "note_id"}},
		Anchor: "closed_at", Created: "created_at", Terminal: "r.closed_at IS NOT NULL", ResidenceDays: 365,
		Retention: tiering.Retention{MinDays: 365, MaxDays: 3650, Basis: "test"}, Routes: []string{tiering.RoutePostgres},
		SchemaVersion: "1"}
}

// P15.13: the migrate entry installs the write barrier on the class's tables, again on every run (idempotent).
// P15.3: a configured residence below N refuses the start.
func TestTheMigrateEntryInstallsTheBarrier(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "tierl", "")
	def := component.Definition{ID: "demo/tierl", Version: "3.0.0", GRPCPort: portNow(), Recovery: "demo.tierl.recover",
		Authz: sectest.Defs("demo/tierl", "demo.tierl", "demo.tierl.recover"),
		Migrations: fstest.MapFS{"0001_notes.up.sql": {Data: []byte("CREATE TABLE notes (id uuid PRIMARY KEY, " +
			"closed_at timestamptz, created_at timestamptz NOT NULL); CREATE TABLE note_lines (id uuid PRIMARY KEY, " +
			"note_id uuid NOT NULL REFERENCES notes (id));")}},
		Tiering: []tiering.Class{tierClass()},
		New:     func(rt *runtime.Runtime) (component.Module, error) { return noRoutes{}, nil }}
	vars := secured(t, def, withIdentity(pgtest.MemberEnv(host, port, "tierl"), def))
	var logs syncBuffer
	for range 2 {
		require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(vars),
			Stdout: &logs, Stderr: &logs}), logs.String())
	}
	admin := pgtest.Admin(t, pgtest.Database)
	var triggers []string
	rows, err := admin.Query(context.Background(), "SELECT c.relname || ':' || t.tgname FROM pg_trigger t JOIN pg_class c "+
		"ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'tierl' AND t.tgname LIKE 'be_tier_%' "+
		"ORDER BY 1")
	require.NoError(t, err)
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		triggers = append(triggers, s)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"note_lines:be_tier_barrier_demo_notes", "notes:be_tier_barrier_demo_notes"}, triggers)

	short := map[string]string{}
	for k, v := range vars {
		short[k] = v
	}
	short[tiering.ResidenceKey("demo.notes")] = "30"
	short["PORT"] = strconv.Itoa(freePort(t))
	logs = syncBuffer{}
	code := Run(context.Background(), def, Options{Env: env(short), Stdout: &logs, Stderr: &logs})
	assert.NotEqual(t, ExitOK, code)
	assert.True(t, strings.Contains(logs.String(), "TIERING_RESIDENCE_DEMO_NOTES"), logs.String())
}
