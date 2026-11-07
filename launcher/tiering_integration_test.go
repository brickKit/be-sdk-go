//go:build integration

package launcher

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/jackc/pgx/v5"
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
		SchemaVersion: "1", HoldOperation: "demo.tierl.hold", ApproveOperation: "demo.tierl.approve"}
}

// P15.13: the migrate entry installs the write barrier on the class's tables, again on every run (idempotent).
// P15.3: a configured residence below N refuses the start.
func TestTheMigrateEntryInstallsTheBarrier(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "tierl", "")
	def := component.Definition{ID: "demo/tierl", Version: "3.0.0", GRPCPort: portNow(), Recovery: "demo.tierl.recover",
		Authz: sectest.Defs("demo/tierl", "demo.tierl", "demo.tierl.recover", "demo.tierl.hold", "demo.tierl.approve"),
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

// P15.1, P15.29, P15.30: a component with a class it can destroy starts (its hold and approval operations declared,
// the destructions' replay registered under its replay operation) and runs the SDK's tiering jobs.
func TestAComponentWithClassesServes(t *testing.T) {
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "tiers", "")
	def := component.Definition{ID: "demo/tiers", Version: "3.0.0", GRPCPort: portNow(), Recovery: "demo.tierl.recover",
		Replay: "demo.tierl.replay",
		Authz: sectest.Defs("demo/tiers", "demo.tierl", "demo.tierl.recover", "demo.tierl.replay", "demo.tierl.hold",
			"demo.tierl.approve"),
		Migrations: fstest.MapFS{"0001_notes.up.sql": {Data: []byte("CREATE TABLE notes (id uuid PRIMARY KEY, " +
			"closed_at timestamptz, created_at timestamptz NOT NULL); CREATE TABLE note_lines (id uuid PRIMARY KEY, " +
			"note_id uuid NOT NULL REFERENCES notes (id));")}},
		Tiering: []tiering.Class{tierClass()},
		New:     func(rt *runtime.Runtime) (component.Module, error) { return noRoutes{}, nil }}
	base, logs, stop := runLogged(t, def, withIdentity(pgtest.MemberEnv(host, port, "tiers"), def))
	admin := pgtest.Admin(t, pgtest.Database)
	var jobs []string
	require.Eventually(t, func() bool {
		rows, err := admin.Query(context.Background(), "SELECT job FROM tiers.be_work_item WHERE job LIKE 'be.tiering.%' ORDER BY job")
		if err != nil {
			return false
		}
		jobs, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err == nil && len(jobs) >= 5
	}, 10*time.Second, 50*time.Millisecond, logs.String())
	assert.Equal(t, []string{"be.tiering.cleanup", "be.tiering.destroy", "be.tiering.export.cleanup", "be.tiering.release",
		"be.tiering.transfer"},
		jobs, "the singletons (the candidates are a cron job, the export a queue)")

	// P15.40: the holds and approvals of the class on the user surface, under its declared operations
	w := worldOf(t)
	send := func(path, bearer, body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Requested-With", "test")
		req.Header.Set("Authorization", bearer)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	hold := `{"scope_kind": "class", "scope": "demo.notes", "source": "legal", "reason": "a court order"}`
	code, body := send("/be/tiering/demo.notes/holds", w.Bearer(sectest.Bob), hold)
	assert.True(t, code == http.StatusForbidden || code == http.StatusNotFound, "bob holds no hold operation: %d %s", code, body)
	code, body = send("/be/tiering/demo.notes/holds", w.Bearer(sectest.Alice), hold)
	require.Equal(t, http.StatusCreated, code, body)
	var created struct{ ID string }
	require.NoError(t, json.Unmarshal([]byte(body), &created))
	var held int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM tiers.be_tier_hold WHERE id = $1 "+
		"AND authorized_by = $2 AND released_at IS NULL", created.ID, sectest.Alice).Scan(&held))
	assert.Equal(t, 1, held)
	code, body = send("/be/tiering/demo.notes/holds/"+created.ID+"/lift", w.Bearer(sectest.Alice), "")
	assert.Equal(t, http.StatusNoContent, code, body)
	code, body = send("/be/tiering/demo.notes/approvals", w.Bearer(sectest.Alice),
		`{"scope_kind": "class", "scope": "demo.notes", "reason": "the retention has run"}`)
	require.Equal(t, http.StatusCreated, code, body)
	require.NoError(t, json.Unmarshal([]byte(body), &created))
	code, body = send("/be/tiering/demo.notes/approvals/"+created.ID+"/withdraw", w.Bearer(sectest.Alice), "")
	assert.Equal(t, http.StatusNoContent, code, body)
	stop()
}
