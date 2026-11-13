//go:build integration

package launcher

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptrace"
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
		{Key: "demo.entry.recover", ResourceType: "demo.entry", Purpose: "read", RiskClass: "low"},
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
			sel := append([]string{"e.id::text", "e.amount::text", "e.cost::text", "e.note"}, plan.Hits...)
			rows, err := tx.Query(ctx, "SELECT "+strings.Join(sel, ", ")+" FROM entries e WHERE "+plan.Where+" ORDER BY e.id",
				plan.Args...)
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
				out = append(out, authz.Mask(&ledgerDefs.Resources[0], row([]string{"id", "amount", "cost", "note"}, vals[:4]), plan.Fields(hits)))
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
		return authz.Mask(&ledgerDefs.Resources[0], row([]string{"id", "amount", "cost", "note"}, vals), d.Fields), nil
	}
	// who the request is: the verified subject only (TI05, TI08)
	r.GET("/api/whoami", "demo.entry.read", func(c *httpx.Context) error {
		u, _ := identity.UserFrom(c.Ctx())
		return c.JSON(200, map[string]string{"subject": u.Subject})
	})
	r.Add(httpx.Route{Method: "GET", Path: "/api/public/whoami", Public: true, Handle: func(c *httpx.Context) error {
		_, ok := identity.UserFrom(c.Ctx())
		_, subject := authz.From(c.Ctx())
		return c.JSON(200, map[string]bool{"user": ok, "subject": subject})
	}})
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

func TestOthersSeeNothingAndFieldsFollowTheSegments(t *testing.T) { // T2.8 acceptance, CK-AZ-05, P8.15-P8.16, CK-ID-05
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "ledger", "")
	def := component.Definition{PublicPrefix: testPrefix, ID: "demo/ledger", Version: "3.0.0", Authz: ledgerDefs, Recovery: "demo.entry.recover", GRPCPort: portNow(),
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
	// a segment without the owner dimension (P8.14, FX01): refused against the declarations, never read as "all owners"
	carol := "0190f3a1-0000-7000-8000-0000000000c3"
	noOwner := own("demo.entry.read", "id", "amount")
	noOwner.Atoms = map[string]authz.Atom{}
	w.Grant(carol, noOwner)
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
	// P8.31: the cost Bob may not read is null and named under _masked (not dropped)
	assert.Equal(t, []map[string]any{{"id": "0190f3a1-0000-7000-8000-00000000e003", "amount": "30", "cost": nil,
		"_masked": []any{"cost"}}}, list(bob), "Bob sees only his entry and no cost")

	code, body := call(t, "GET", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e003", alice)
	assert.Equal(t, 404, code, "another's entry is not found: %s", body)
	code, body = call(t, "GET", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e003", bob)
	assert.Equal(t, 200, code)
	assert.JSONEq(t, `{"id":"0190f3a1-0000-7000-8000-00000000e003","amount":"30","cost":null,"_masked":["cost"]}`, body,
		"single object and list agree")
	assert.Empty(t, list(w.Bearer(carol)), "a segment missing a dimension gives nothing")
	code, _ = call(t, "GET", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e003", w.Bearer(carol))
	assert.Equal(t, 404, code)
	assert.Contains(t, out.String(), "segment_refused", "the refusal is logged")
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

	// TI05: what a request says about itself (be-caller, an actor, a verified flag) is not identity; a public route
	// gets no user context even with a valid credential
	req, _ := http.NewRequest("GET", base+"/api/whoami", nil)
	req.Header.Set("Authorization", alice)
	for _, h := range []string{"be-caller", "be-actor", "X-BE-Actor", "X-User-Id", "be-verified"} {
		req.Header.Set(h, sectest.Bob)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.JSONEq(t, `{"subject":"`+sectest.Alice+`"}`, string(b))
	code, body = call(t, "GET", base+"/api/public/whoami", alice)
	assert.Equal(t, 200, code)
	assert.JSONEq(t, `{"user":false,"subject":false}`, body)

	// TI08: one reused connection, Alice and Bob alternating: each request keeps its own identity
	one := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1, MaxIdleConnsPerHost: 1}}
	reused := 0
	for i, bearer := range []string{alice, bob, alice, bob} {
		req, _ := http.NewRequest("GET", base+"/api/whoami", nil)
		req.Header.Set("Authorization", bearer)
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
			GotConn: func(ci httptrace.GotConnInfo) {
				if ci.Reused {
					reused++
				}
			}}))
		resp, err := one.Do(req)
		require.NoError(t, err)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := sectest.Alice
		if i%2 == 1 {
			want = sectest.Bob
		}
		assert.JSONEq(t, `{"subject":"`+want+`"}`, string(b), "request %d", i)
	}
	assert.Equal(t, 3, reused, "precondition: the requests shared one connection")

	// CK-ID-05, P8.25: after an identity-side security change the token issued before is stale at admission, before
	// any effect; the new token (equal versions) and a subject without a version in the view go through
	approvals := func() int {
		var n int
		require.NoError(t, admin.QueryRow(context.Background(),
			"SELECT count(*) FROM ledger.be_audit_log WHERE operation = 'demo.entry.approve'").Scan(&n))
		return n
	}
	w.SecurityChange(sectest.Alice)
	require.Eventually(t, func() bool { // a read: waiting must not write
		code, body = call(t, "GET", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e001", alice)
		return code == 401
	}, 10*time.Second, 100*time.Millisecond, "the renewed view carries the new version: %d %s", code, body)
	assert.Contains(t, body, `"reason":"TOKEN_STALE"`)
	before := approvals()
	require.Equal(t, 1, before, "precondition: only the approval above wrote its audit row")
	code, body = call(t, "POST", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e001/approve", alice)
	assert.Equal(t, 401, code)
	assert.Contains(t, body, `"reason":"TOKEN_STALE"`)
	assert.Equal(t, before, approvals(), "a stale token's write did nothing")
	code, body = call(t, "POST", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e001/approve", w.Bearer(sectest.Alice))
	assert.Equal(t, 204, code, "a token at the current version: %s", body)
	assert.Equal(t, before+1, approvals())
	code, _ = call(t, "GET", base+"/api/entries/0190f3a1-0000-7000-8000-00000000e003", bob)
	assert.Equal(t, 200, code, "Bob has no version in the view: not stale")

	code, body = call(t, "GET", base+"/api/entries", "")
	assert.Equal(t, 401, code)
	assert.Contains(t, body, `"reason":"TOKEN_INVALID"`)
	code, _ = call(t, "GET", base+"/api/entries", "Bearer not.a.token")
	assert.Equal(t, 401, code)
	cancel()
	assert.Equal(t, ExitOK, <-done)
	assert.NotContains(t, out.String(), strings.TrimPrefix(alice, "Bearer "), "no credential in the logs")
}
