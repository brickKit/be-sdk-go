//go:build integration

package launcher

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/echotest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/internal/sectest"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var noteDefs = &authz.Definitions{Component: "demo/notes",
	Resources: []authz.Resource{{Type: "demo.note", ViewOperation: "demo.note.read",
		Dimensions: map[string]authz.Dimension{"owner": {Kind: authz.Principal, Column: "owner_id", PrincipalRole: "owner"}},
		Fields: map[string]authz.Field{"id": {Class: authz.Ordinary}, "text": {Class: authz.Ordinary},
			"cost": {Class: authz.Restricted, ReadKey: "demo.note.cost"}}}},
	Operations: []authz.Operation{
		{Key: "demo.note.read", ResourceType: "demo.note", Purpose: "read", RiskClass: "low"},
		{Key: "demo.note.create", ResourceType: "demo.note", Purpose: "action", RiskClass: "high"},
		{Key: "demo.note.import", ResourceType: "demo.note", Purpose: "action", RiskClass: "low"}}}

type notesModule struct{ rt *runtime.Runtime }

// read answers with the note as the caller may see it now (P4.24: a replay is re-read, not the stored response).
func (n *notesModule) read(c *httpx.Context, status int, id string) error {
	s, _ := authz.From(c.Ctx())
	var owner, text, cost string
	if err := n.rt.DB.Tx(c.Ctx(), func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT owner_id::text, text, cost::text FROM notes WHERE id = $1", id).Scan(&owner, &text, &cost)
	}); err != nil {
		return err
	}
	_, d, err := s.Check(c.Ctx(), "demo.note.read", authz.Facts{"owner": authz.Value(owner)})
	if err != nil {
		return err
	}
	return c.JSON(status, authz.Mask(map[string]any{"id": id, "text": text, "cost": cost}, d.Fields))
}

func (n *notesModule) Routes(r *httpx.Routes) error {
	r.POST("/api/notes", "demo.note.create", func(c *httpx.Context) error {
		req, err := idem.FromHTTP(c)
		if err != nil {
			return err
		}
		var in struct{ Text, Cost string }
		if err := c.BindJSON(&in); err != nil {
			return err
		}
		s, _ := authz.From(c.Ctx())
		if _, _, err := s.Check(c.Ctx(), "demo.note.create", authz.Facts{"owner": authz.Value(s.Principal)}); err != nil {
			return err
		}
		out, _, err := idem.Do(c.Ctx(), n.rt.DB, req, func(ctx context.Context, tx db.Tx) (idem.Outcome, error) {
			id := ids.New().String()
			_, err := tx.Exec(ctx, "INSERT INTO notes VALUES ($1, $2, $3, $4)", id, s.Principal, in.Text, in.Cost)
			res, _ := json.Marshal(map[string]string{"id": id})
			return idem.Outcome{Status: 201, Result: res}, err
		})
		if err != nil {
			return err
		}
		var res struct{ ID string }
		_ = json.Unmarshal(out.Result, &res)
		return n.read(c, out.Status, res.ID)
	})
	return nil
}

func (n *notesModule) Services(r *grpcx.Services) error {
	r.Register(&echotest.Desc, echotest.Say(func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		req, err := idem.FromGRPC(ctx, "demo.note.import", in)
		if err != nil {
			return nil, err
		}
		out, _, err := idem.Do(ctx, n.rt.DB, req, func(ctx context.Context, tx db.Tx) (idem.Outcome, error) {
			id := ids.New().String()
			_, err := tx.Exec(ctx, "INSERT INTO notes VALUES ($1, $2, $3, 0)", id, sectest.Alice, in.Value)
			res, _ := json.Marshal(id)
			return idem.Outcome{Result: res}, err
		})
		if err != nil {
			return nil, err
		}
		var id string
		_ = json.Unmarshal(out.Result, &id)
		return wrapperspb.String(id), nil
	}), map[string]grpcx.Method{"Say": {Operation: "demo.note.import", Callers: []string{"demo/client"}}})
	return nil
}

func ownNote(op string, fields ...string) authz.Segment {
	return authz.Segment{ResourceType: "demo.note", Operation: op, Atoms: map[string]authz.Atom{"owner": {Kind: "self"}},
		ReadFields: fields, Valid: authz.Window{From: "2026-01-01T00:00:00Z"}}
}

func TestIdempotentCommandsOnBothSurfaces(t *testing.T) { // CK-IDEM-01..03
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "notes", "")
	grpcPort := freePort(t)
	def := component.Definition{ID: "demo/notes", Version: "3.0.0", Authz: noteDefs, GRPCPort: grpcPort,
		Migrations: fstest.MapFS{"0001_notes.up.sql": {Data: []byte(`CREATE TABLE notes (id uuid PRIMARY KEY,
			owner_id uuid NOT NULL, text text NOT NULL, cost numeric NOT NULL);`)}},
		New: func(rt *runtime.Runtime) (component.Module, error) { return &notesModule{rt: rt}, nil }}
	vars := pgtest.MemberEnv(host, port, "notes")
	var out syncBuffer
	require.Equal(t, ExitOK, Run(context.Background(), def, Options{Args: []string{"migrate"}, Env: env(vars), Stdout: &out, Stderr: &out}), out.String())
	w := worldOf(t)
	w.Grant(sectest.Alice, ownNote("demo.note.read", "id", "text", "cost"), ownNote("demo.note.create"))
	w.Grant(sectest.Bob, ownNote("demo.note.read", "id", "text"), ownNote("demo.note.create"))
	httpPort := freePort(t)
	serve := maps.Clone(vars)
	maps.Copy(serve, w.Env(t, def.ID))
	maps.Copy(serve, map[string]string{"COMPONENT_ID": def.ID, "COMPONENT_VERSION": def.Version, "PORT": strconv.Itoa(httpPort), "STOP_DRAIN_DELAY": "50ms"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() { done <- Run(ctx, def, Options{Env: env(serve), Stdout: &out, Stderr: &out}) }()
	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 10*time.Second, 50*time.Millisecond)

	post := func(bearer, key, body string) (int, map[string]any) {
		req, _ := http.NewRequest("POST", base+"/api/notes", bytes.NewBufferString(body))
		req.Header.Set("Authorization", bearer)
		if key != "" {
			req.Header.Set(calls.HeaderIdempotencyKey, key)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return resp.StatusCode, m
	}
	alice, bob := w.Bearer(sectest.Alice), w.Bearer(sectest.Bob)
	code, body := post(alice, "", `{"text":"a","cost":"5"}`)
	assert.Equal(t, 400, code)
	assert.Equal(t, "REQUEST_INVALID", body["reason"], "a command that requires a key refuses to run without one")
	code, body = post(alice, "has space", `{"text":"a","cost":"5"}`)
	assert.Equal(t, 400, code)

	code, first := post(alice, "note-1", `{"text":"a","cost":"5"}`)
	require.Equal(t, 201, code, first)
	assert.Equal(t, "5", first["cost"])
	code, again := post(alice, "note-1", `{"text":"a","cost":"5"}`)
	assert.Equal(t, 201, code)
	assert.Equal(t, first, again, "the replay answers with the same note")
	code, body = post(alice, "note-1", `{"text":"b","cost":"5"}`)
	assert.Equal(t, 422, code)
	assert.Equal(t, "IDEMPOTENCY_KEY_REUSED", body["reason"])
	code, bobs := post(bob, "note-1", `{"text":"a","cost":"5"}`)
	require.Equal(t, 201, code)
	assert.NotEqual(t, first["id"], bobs["id"], "Bob's same key is his own command")

	// Alice loses the cost field; her replay is read under what she may see now
	w.Revoke(sectest.Alice)
	w.Grant(sectest.Alice, ownNote("demo.note.read", "id", "text"), ownNote("demo.note.create"))
	require.Eventually(t, func() bool {
		code, now := post(alice, "note-1", `{"text":"a","cost":"5"}`)
		_, hasCost := now["cost"]
		return code == 201 && now["id"] == first["id"] && !hasCost
	}, 15*time.Second, 200*time.Millisecond, "the replay is masked by the current authorization")

	// the system surface: the key is scoped to the calling component
	cl, err := calls.NewMember(nil)
	require.NoError(t, err)
	defer cl.Close()
	cl.SetTLS(w.TLS(t, "demo/client"))
	conn, err := cl.GRPC("demo/notes", "127.0.0.1:"+strconv.Itoa(grpcPort))
	require.NoError(t, err)
	importNote := func(key, v string) (string, error) {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer ccancel()
		reply := new(wrapperspb.StringValue)
		opts := []grpc.CallOption{}
		if key != "" {
			opts = append(opts, calls.IdempotencyKey(key))
		}
		err := conn.Invoke(cctx, echotest.Method, wrapperspb.String(v), reply, opts...)
		return reply.Value, err
	}
	id1, err := importNote("imp-1", "x")
	require.NoError(t, err)
	id2, err := importNote("imp-1", "x")
	require.NoError(t, err)
	assert.Equal(t, id1, id2, "a retried system command returns the first outcome")
	_, err = importNote("imp-1", "y")
	assert.ErrorContains(t, err, "IDEMPOTENCY_KEY_REUSED")
	_, err = importNote("", "x")
	assert.ErrorContains(t, err, "Idempotency-Key")
	cancel()
	assert.Equal(t, ExitOK, <-done)
}
