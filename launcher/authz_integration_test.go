//go:build integration

package launcher

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/audit"
	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ledgerDefs = &authz.Definitions{Component: "demo/ledger",
	Resources: []authz.Resource{{Type: "demo.entry", ViewOperation: "demo.entry.read",
		Dimensions: map[string]authz.Dimension{"owner": {Kind: authz.Principal, Column: "owner_id", PrincipalRole: "owner"}},
		Fields: map[string]authz.Field{"id": {Class: authz.Ordinary}, "amount": {Class: authz.Ordinary},
			"cost": {Class: authz.Restricted, ReadKey: "demo.entry.cost"}, "note": {Class: authz.Secret}}}},
	Operations: []authz.Operation{
		{Key: "demo.entry.read", ResourceType: "demo.entry", Purpose: "read", RiskClass: "low"},
		{Key: "demo.entry.approve", ResourceType: "demo.entry", Purpose: "action", RiskClass: "high"}}}

type ledger struct{ rt *runtime.Runtime }

func row(cols []string, vals []any) map[string]any {
	out := map[string]any{}
	for i, c := range cols {
		out[c] = vals[i]
	}
	return out
}

func (l *ledger) Routes(r *httpx.Routes) error {
	r.GET("/api/entries", "demo.entry.read", func(c *httpx.Context) error {
		s, _ := authz.From(c.Ctx())
		ctx, plan, err := s.List(c.Ctx(), "demo.entry.read", "e", 0)
		if err != nil {
			return err
		}
		var out []map[string]any
		err = l.rt.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, "SELECT e.id::text, e.amount::text, e.cost::text, e.note, "+strings.Join(plan.Hits, ", ")+
				" FROM entries e WHERE "+plan.Where+" ORDER BY e.id", plan.Args...)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					return err
				}
				hits := make([]bool, len(vals)-4)
				for i, v := range vals[4:] {
					hits[i], _ = v.(bool)
				}
				out = append(out, authz.Mask(row([]string{"id", "amount", "cost", "note"}, vals[:4]), plan.Fields(hits)))
			}
			return rows.Err()
		})
		if err != nil {
			return err
		}
		return c.JSON(200, out)
	})
	one := func(c *httpx.Context, op string) (map[string]any, error) {
		s, _ := authz.From(c.Ctx())
		var vals []any
		var owner string
		err := l.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			var id, amount, cost, note string
			if err := tx.QueryRow(ctx, "SELECT id::text, owner_id::text, amount::text, cost::text, note FROM entries WHERE id = $1",
				c.Param("id")).Scan(&id, &owner, &amount, &cost, &note); err != nil {
				return err
			}
			vals = []any{id, amount, cost, note}
			return nil
		})
		if err != nil {
			return nil, err
		}
		_, d, err := s.Check(c.Ctx(), op, authz.Facts{"owner": authz.Value(owner)})
		if err != nil {
			return nil, err
		}
		return authz.Mask(row([]string{"id", "amount", "cost", "note"}, vals), d.Fields), nil
	}
	r.GET("/api/entries/:id", "demo.entry.read", func(c *httpx.Context) error {
		out, err := one(c, "demo.entry.read")
		if err != nil {
			return err
		}
		return c.JSON(200, out)
	})
	r.POST("/api/entries/:id/approve", "demo.entry.approve", func(c *httpx.Context) error {
		if _, err := one(c, "demo.entry.approve"); err != nil {
			return err
		}
		u, _ := identity.UserFrom(c.Ctx())
		err := l.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
			_, err := l.rt.Audit.Write(ctx, tx, audit.Record{Category: audit.BusinessChange, Operation: "demo.entry.approve",
				Outcome: audit.Succeeded, Actor: u.Subject, Target: map[string]any{"type": "demo.entry", "id": c.Param("id")}})
			return err
		})
		if err != nil {
			return err
		}
		return c.NoContent(204)
	})
	return nil
}

func own(op string, fields ...string) authz.Segment {
	return authz.Segment{ResourceType: "demo.entry", Operation: op, Atoms: map[string]authz.Atom{"owner": {Kind: "self"}},
		ReadFields: fields, Valid: authz.Window{From: "2026-01-01T00:00:00Z"}}
}

func call(t *testing.T, method, url, bearer string) (int, string) {
	req, _ := http.NewRequest(method, url, nil)
	if bearer != "" {
		req.Header.Set("Authorization", bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestOthersSeeNothingAndFieldsFollowTheSegments(t *testing.T) { // T2.8 acceptance, CK-AZ-05, P8.15-P8.16
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "ledger", "")
	def := component.Definition{ID: "demo/ledger", Version: "3.0.0", Authz: ledgerDefs,
		Migrations: fstest.MapFS{"0001_entries.up.sql": {Data: []byte(`CREATE TABLE entries (id uuid PRIMARY KEY,
			owner_id uuid NOT NULL, amount numeric NOT NULL, cost numeric NOT NULL, note text NOT NULL);`)}},
		New: func(rt *runtime.Runtime) (component.Module, error) { return &ledger{rt: rt}, nil }}
	vars := pgtest.MemberEnv(host, port, "ledger")
	var out syncBuffer
	require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(vars), Stdout: &out, Stderr: &out}), out.String())
	admin := pgtest.Admin(t, pgtest.Database)
	pgtest.Exec(t, admin, "INSERT INTO ledger.entries VALUES "+
		"('0190f3a1-0000-7000-8000-00000000e001', '"+sectest.Alice+"', 10, 7, 'a1'), "+
		"('0190f3a1-0000-7000-8000-00000000e002', '"+sectest.Alice+"', 20, 8, 'a2'), "+
		"('0190f3a1-0000-7000-8000-00000000e003', '"+sectest.Bob+"', 30, 9, 'b1')")

	w := worldOf(t)
	w.Grant(sectest.Alice, own("demo.entry.read", "id", "amount", "cost", "note"), own("demo.entry.approve"))
	w.Grant(sectest.Bob, own("demo.entry.read", "id", "amount"))
	httpPort := freePort(t)
	serve := maps.Clone(vars)
	maps.Copy(serve, w.Env(t, def.ID))
	maps.Copy(serve, map[string]string{"COMPONENT_ID": def.ID, "COMPONENT_VERSION": def.Version, "PORT": strconv.Itoa(httpPort), "STOP_DRAIN_DELAY": "50ms"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- Run(ctx, def, Options{Env: env(serve), Stdout: &out, Stderr: &out}) }()
	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	alice, bob := w.Bearer(sectest.Alice), w.Bearer(sectest.Bob)
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 10*time.Second, 50*time.Millisecond)

	list := func(bearer string) []map[string]any {
		code, body := call(t, "GET", base+"/api/entries", bearer)
		require.Equal(t, 200, code, body)
		var rows []map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &rows))
		return rows
	}
	assert.Equal(t, []map[string]any{
		{"id": "0190f3a1-0000-7000-8000-00000000e001", "amount": "10", "cost": "7"},
		{"id": "0190f3a1-0000-7000-8000-00000000e002", "amount": "20", "cost": "8"}}, list(alice),
		"Alice sees her own entries with cost; the secret note never leaves")
	assert.Equal(t, []map[string]any{{"id": "0190f3a1-0000-7000-8000-00000000e003", "amount": "30"}}, list(bob),
		"Bob sees only his entry and no cost")

	code, body := call(t, "GET", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e003", alice)
	assert.Equal(t, 404, code, "another's entry is not found: %s", body)
	code, body = call(t, "GET", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e003", bob)
	assert.Equal(t, 200, code)
	assert.JSONEq(t, `{"id":"0190f3a1-0000-7000-8000-00000000e003","amount":"30"}`, body, "single object and list agree")
	code, _ = call(t, "POST", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e003/approve", bob)
	assert.Equal(t, 403, code, "visible without the operation")
	code, _ = call(t, "POST", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e003/approve", alice)
	assert.Equal(t, 404, code, "not visible: the command answers like the read")
	code, _ = call(t, "POST", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e001/approve", alice)
	assert.Equal(t, 204, code)
	require.Eventually(t, func() bool {
		for _, rec := range w.Audited() {
			if strings.Contains(string(rec), "demo.entry.approve") && strings.Contains(string(rec), sectest.Alice) {
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "the approval's audit record reached the audit component over mTLS")

	code, body = call(t, "GET", base+"/api/entries", "")
	assert.Equal(t, 401, code)
	assert.Contains(t, body, `"reason":"TOKEN_INVALID"`)
	code, _ = call(t, "GET", base+"/api/entries", "Bearer not.a.token")
	assert.Equal(t, 401, code)
	cancel()
	assert.Equal(t, ExitOK, <-done)
	assert.NotContains(t, out.String(), strings.TrimPrefix(alice, "Bearer "), "no credential in the logs")
}
