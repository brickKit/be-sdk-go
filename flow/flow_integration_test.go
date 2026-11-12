//go:build integration

package flow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/audit"
	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/flow"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A component of its own: documents with a state and a key revision, and the events the kernel publishes (one row an
// emit, in the emit's transaction).
var tables = fstest.MapFS{"0001_demo.up.sql": {Data: []byte(`
CREATE TABLE doc (id uuid PRIMARY KEY, state text NOT NULL DEFAULT 'submitted', key_revision bigint NOT NULL DEFAULT 1,
	legal_entity text NOT NULL);
CREATE TABLE published (at timestamptz NOT NULL DEFAULT clock_timestamp(), type text NOT NULL, record_id text NOT NULL, version bigint NOT NULL,
	data jsonb NOT NULL);
`)}}

const (
	le        = "01900000-0000-7000-8000-0000000000e1"
	position  = "01900000-0000-7000-8000-00000000a001"
	initiator = "account-initiator"
	alice     = "account-alice"
	bob       = "account-bob"
	eve       = "account-eve" // the initiator's second account: the same person
	dave      = "account-dave"
)

var defs = &authz.Definitions{Component: "test/purchase", Resources: []authz.Resource{
	{Type: "demo.doc", ViewOperation: "demo.doc.read", Dimensions: map[string]authz.Dimension{
		"legal_entity": {Kind: authz.ValueSet, Column: "legal_entity"}}, Fields: map[string]authz.Field{"id": {Class: authz.Ordinary}}},
	{Type: "demo.flow", ViewOperation: "demo.flow.use", Dimensions: map[string]authz.Dimension{}, Fields: map[string]authz.Field{}}},
	Operations: []authz.Operation{
		{Key: "demo.doc.read", ResourceType: "demo.doc", Purpose: "read", RiskClass: "low"},
		{Key: "demo.doc.approve", ResourceType: "demo.doc", Purpose: "action", RiskClass: "low", SideEffects: []string{"business_write"}},
		{Key: "demo.flow.use", ResourceType: "demo.flow", Purpose: "read", RiskClass: "low"},
		{Key: "demo.flow.manage", ResourceType: "demo.flow", Purpose: "read", RiskClass: "low"}}}

// grants: alice, bob and eve approve (an action on a document needs its reading too: what one may not see answers
// 404); dave approves too (step 2 names him); the initiator only reads; alice manages the flows.
type grants struct{}

func seg(op, typ string) authz.Segment {
	atoms := map[string]authz.Atom{}
	if typ == "demo.doc" {
		atoms["legal_entity"] = authz.Atom{Kind: "all"}
	}
	return authz.Segment{ResourceType: typ, Operation: op, Atoms: atoms, ReadFields: []string{"id"},
		Valid: authz.Window{From: "2026-01-01T00:00:00Z"}}
}

func (grants) Verify(_ context.Context, req authz.VerifyRequest) (authz.VerifyAnswer, error) {
	approve, read := seg("demo.doc.approve", "demo.doc"), seg("demo.doc.read", "demo.doc")
	use, manage := seg("demo.flow.use", "demo.flow"), seg("demo.flow.manage", "demo.flow")
	s := authz.Shard{Shard: "s", Applicability: []string{"demo.doc", "demo.flow"}, Digest: "d",
		Subjects: map[string]authz.SubjectEntry{
			alice: {Segments: []authz.Segment{approve, read, use, manage}}, bob: {Segments: []authz.Segment{approve, read, use}},
			eve: {Segments: []authz.Segment{approve, read, use}}, dave: {Segments: []authz.Segment{approve, read, use}},
			initiator: {Segments: []authz.Segment{read, use}}}}
	return authz.VerifyAnswer{Nonce: req.Nonce, RecoveryGeneration: authz.Revision{Value: "1"},
		Shards: []authz.ShardAnswer{{Shard: "s", Status: "changed", View: &s}}}, nil
}

// directory: the position's holders and the people behind accounts; complete can be switched off.
type directory struct {
	mu       sync.Mutex
	holders  []string
	complete bool
	persons  map[string]string
}

func (d *directory) Holders(context.Context, db.Tx, string, string, string) ([]string, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.holders...), d.complete, nil
}

func (d *directory) Holds(_ context.Context, _ db.Tx, account, _, _, _ string) (bool, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, h := range d.holders {
		if h == account {
			return true, d.complete, nil
		}
	}
	return false, d.complete, nil
}

func (d *directory) PersonOf(_ context.Context, _ db.Tx, account string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if p, ok := d.persons[account]; ok {
		return p, nil
	}
	return account, nil
}

func (d *directory) set(holders []string, complete bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.holders, d.complete = holders, complete
}

// emitter writes every event into published, in the caller's transaction.
type emitter struct{}

func (emitter) Emit(ctx context.Context, tx db.Tx, _ string, e events.Event) (string, error) {
	b, err := json.Marshal(e.Data)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO published (type, record_id, version, data) VALUES ($1, $2, $3, $4)`, e.Type, e.RecordID,
		e.Version, b)
	return "", err
}

// auditor records nothing but counts the refusals' security records.
type auditor struct {
	mu      sync.Mutex
	denials []string
}

func (a *auditor) Write(context.Context, db.Tx, audit.Record) (string, error) { return "", nil }
func (a *auditor) Denial(_ context.Context, r audit.Record) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.denials = append(a.denials, r.Reason)
	return nil
}

type world struct {
	t   *testing.T
	ctx context.Context
	d   *db.DB
	k   *flow.Kernel
	dir *directory
	aud *auditor
	h   *authz.Holder
}

const (
	twoSteps = "demo.two" // a position (any), then a person; a rejection returns the document
	oneStep  = "demo.one" // a position (single); a rejection rejects it
)

func newWorld(t *testing.T, name string) *world {
	d, _ := dbtest.Schema(t, name, tables)
	w := &world{t: t, ctx: context.Background(), d: d, aud: &auditor{},
		dir: &directory{complete: true, holders: []string{alice, bob, eve}, persons: map[string]string{
			initiator: "person-1", eve: "person-1"}}}
	w.h = authz.NewHolder("test/purchase", grants{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, w.h.Verify(w.ctx))
	require.NoError(t, defs.Validate())
	w.k = &flow.Kernel{Component: "test/purchase", Stream: "test.purchase", TaskEvent: "test.purchase_task.changed.v1",
		HeartbeatEvent: "test.purchase_task.heartbeat.v1", Operation: "demo.flow.use", ManageOperation: "demo.flow.manage",
		Definitions: []flow.Definition{
			{Key: twoSteps, Version: 1, TaskType: "test.purchase.two.approval", DocumentType: "doc", Operation: "demo.doc.approve",
				ReadOperation: "demo.doc.read", OnReject: flow.Returned,
				Steps: []flow.Step{{Form: flow.Any, Rule: flow.Position}, {Form: flow.Single, Rule: flow.Person}}},
			{Key: oneStep, Version: 1, TaskType: "test.purchase.one.approval", DocumentType: "doc", Operation: "demo.doc.approve",
				ReadOperation: "demo.doc.read", OnReject: flow.Rejected, Steps: []flow.Step{{Form: flow.Single, Rule: flow.Position}}}},
		Hooks: flow.Hooks{
			Document: func(ctx context.Context, tx db.Tx, doc flow.Document) (int64, string, authz.Facts, error) {
				var rev int64
				var entity string
				err := tx.QueryRow(ctx, `SELECT key_revision, legal_entity FROM doc WHERE id = $1 FOR UPDATE`, doc.ID).Scan(&rev, &entity)
				return rev, "2026-11-12", authz.Facts{"legal_entity": authz.Value(entity)}, err
			},
			Ended: func(ctx context.Context, tx db.Tx, i flow.Instance) error {
				_, err := tx.Exec(ctx, `UPDATE doc SET state = $2 WHERE id = $1`, i.Document.ID, string(i.State))
				return err
			}},
		Directory: w.dir, Audit: w.aud, DB: d, Events: emitter{}}
	require.NoError(t, w.k.Check())
	return w
}

func (w *world) subject(p string) *authz.Subject {
	return &authz.Subject{Principal: p, Defs: defs, Holder: w.h, At: time.Now()}
}

// start makes a document and starts its approval; answers the document and the first task.
func (w *world) start(flowKey string) (string, string) {
	w.t.Helper()
	doc := ids.New().String()
	var task string
	require.NoError(w.t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO doc (id, legal_entity) VALUES ($1, $2)`, doc, le); err != nil {
			return err
		}
		steps := 1
		if flowKey == twoSteps {
			steps = 2
		}
		b := []flow.Binding{{LegalEntity: le, PositionID: position}, {Principal: dave}}[:steps]
		digests := []string{"sha256:step1", "sha256:step2"}[:steps]
		i, err := w.k.StartIn(ctx, tx, flow.Start{Flow: flowKey, Document: flow.Document{Type: "doc", ID: doc, Number: "PO-1"},
			LegalEntity: le, KeyRevision: 1, Digests: digests, Bindings: b, Initiator: initiator,
			Summary: flow.Summary{Title: "采购订单 PO-1", Fields: []flow.SummaryField{{Key: "total", Label: "合计", Value: "100"}}}})
		if err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT id::text FROM be_flow_task WHERE instance_id = $1`, i.ID).Scan(&task)
	}))
	return doc, task
}

func (w *world) task(id string) flow.Task {
	w.t.Helper()
	var data []byte
	require.NoError(w.t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT data FROM published WHERE record_id = $1 ORDER BY at DESC LIMIT 1`, id).Scan(&data)
	}))
	var t flow.Task
	require.NoError(w.t, json.Unmarshal(data, &t))
	return t
}

func (w *world) decide(who, task, key string, version int64, c flow.Choice) (flow.Task, error) {
	body := []byte(`{"decision":"` + string(c) + `"}`)
	return w.k.Decide(w.ctx, w.subject(who), idem.Request{Principal: who, Key: key, Operation: "demo.flow.use", Body: body,
		Target: "POST /be/flow/tasks/" + task + "/decisions"}, task, flow.DecisionIn{Version: version, Decision: c})
}

func reason(err error) string {
	var e *errs.Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

func (w *world) docState(doc string) string {
	var s string
	require.NoError(w.t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT state FROM doc WHERE id = $1`, doc).Scan(&s)
	}))
	return s
}

// P17.4, P17.8, P17.10-P17.13, P17.15: a two-step flow from start to approval, every refusal in order, the replay, the
// projection by the shared task contract.
func TestADecisionFromStartToApproval(t *testing.T) {
	w := newWorld(t, "flow_decide")
	doc, task := w.start(twoSteps)
	first := w.task(task)
	assert.Equal(t, flow.Pending, first.State)
	assert.Equal(t, []string{alice, bob}, first.Candidates, "the initiator's person is left out: eve is the same person (P17.8)")
	assert.Equal(t, "test/purchase", first.Component)

	// the refusals, in P17.10's order
	_, err := w.decide(bob, task, "k-old", first.Version+1, flow.Approve)
	assert.Equal(t, flow.ReasonVersionConflict, reason(err))
	_, err = w.decide(eve, task, "k-eve", first.Version, flow.Approve)
	assert.Equal(t, flow.ReasonSelfApproval, reason(err), "eve is the initiator's person (D10)")
	_, err = w.decide(dave, task, "k-dave", first.Version, flow.Approve)
	assert.Equal(t, flow.ReasonNotAssignee, reason(err), "dave approves, but step 1 is the position's")
	_, err = w.decide("account-stranger", task, "k-x", first.Version, flow.Approve)
	assert.Equal(t, errs.ReasonNotFound, reason(err), "one who may not see it")
	w.dir.set([]string{bob, eve}, true) // alice left the position
	_, err = w.decide(alice, task, "k-alice-gone", first.Version, flow.Approve)
	assert.Equal(t, flow.ReasonNotQualified, reason(err))
	w.dir.set([]string{alice, bob, eve}, false)
	_, err = w.decide(alice, task, "k-alice-unknown", first.Version, flow.Approve)
	assert.Equal(t, flow.ReasonQualificationUnknown, reason(err))
	w.dir.set([]string{alice, bob, eve}, true)
	assert.Len(t, w.aud.denials, 6, "every refused check has its security record")

	// alice approves step 1 (any): step 2's task goes to dave
	done, err := w.decide(alice, task, "k-alice", first.Version, flow.Approve)
	require.NoError(t, err)
	assert.Equal(t, flow.Decided, done.State)
	assert.Equal(t, alice, done.Decision.By)
	again, err := w.decide(alice, task, "k-alice", first.Version, flow.Approve)
	require.NoError(t, err, "the same key answers the first result (P17.11)")
	assert.Equal(t, done.Version, again.Version)
	_, err = w.decide(bob, task, "k-bob", done.Version, flow.Approve)
	assert.Equal(t, flow.ReasonTaskNotPending, reason(err), "one decision a task")
	var second string
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT id::text FROM be_flow_task WHERE instance_id = $1 AND step = 2`, done.Instance.ID).Scan(&second)
	}))
	s2 := w.task(second)
	assert.Equal(t, []string{dave}, s2.Candidates, "a person rule")
	assert.Equal(t, 2, s2.Instance.Step)

	// a key revision of the document since the start: the decision is refused
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE doc SET key_revision = 2 WHERE id = $1`, doc)
		return err
	}))
	_, err = w.decide(dave, second, "k-dave-rev", s2.Version, flow.Approve)
	assert.Equal(t, flow.ReasonRevisionChanged, reason(err))
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE doc SET key_revision = 1 WHERE id = $1`, doc)
		return err
	}))
	end, err := w.decide(dave, second, "k-dave-ok", s2.Version, flow.Approve)
	require.NoError(t, err)
	assert.Equal(t, flow.Approved, end.Instance.State)
	assert.Equal(t, "approved", w.docState(doc), "the end hook changed the document in the same transaction (P17.6)")

	// the release check (P17.7)
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		if err := w.k.ReleaseIn(ctx, tx, flow.Document{Type: "doc", ID: doc}, 1, []string{"sha256:step1", "sha256:step2"}); err != nil {
			return err
		}
		var nr flow.ErrNotReleased
		assert.ErrorAs(t, w.k.ReleaseIn(ctx, tx, flow.Document{Type: "doc", ID: doc}, 2, []string{"sha256:step1", "sha256:step2"}), &nr)
		assert.ErrorAs(t, w.k.ReleaseIn(ctx, tx, flow.Document{Type: "doc", ID: doc}, 1, []string{"sha256:other", "sha256:step2"}), &nr)
		return nil
	}))

	// every projection fits the shared task contract (schemas/flow/task.v1)
	c := jsonschema.NewCompiler()
	c.UseLoader(protocolLoader{})
	sch, err := c.Compile("https://brickkit.dev/be-protocol/schemas/flow/task.v1.schema.json")
	require.NoError(t, err)
	n := 0
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `SELECT data FROM published WHERE type = 'test.purchase_task.changed.v1'`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var data []byte
			if err := rows.Scan(&data); err != nil {
				return err
			}
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
			if err != nil {
				return err
			}
			assert.NoError(t, sch.Validate(doc), string(data))
			n++
		}
		return rows.Err()
	}))
	assert.GreaterOrEqual(t, n, 4)
}

// protocolLoader reads the protocol's schemas by their $id.
type protocolLoader struct{}

func (protocolLoader) Load(url string) (any, error) {
	const base = "https://brickkit.dev/be-protocol/"
	b, err := fs.ReadFile(protocol.Schemas, url[len(base):])
	if err != nil {
		return nil, err
	}
	return jsonschema.UnmarshalJSON(bytes.NewReader(b))
}

// P17.6, P17.13: a rejection ends the instance by the definition (rejected); a return lets a new instance start;
// withdraw and supersede end the live instance and its open tasks.
func TestEndsOfAnInstance(t *testing.T) {
	w := newWorld(t, "flow_ends")
	doc, task := w.start(oneStep)
	first := w.task(task)
	end, err := w.decide(bob, task, "k-reject", first.Version, flow.Reject)
	require.NoError(t, err)
	assert.Equal(t, flow.Rejected, end.Instance.State)
	assert.Equal(t, "rejected", w.docState(doc))

	doc2, task2 := w.start(twoSteps)
	r, err := w.decide(alice, task2, "k-return", w.task(task2).Version, flow.Reject)
	require.NoError(t, err)
	assert.Equal(t, flow.Returned, r.Instance.State, "the definition returns it")
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error { // resubmitted: a new instance
		_, err := w.k.StartIn(ctx, tx, flow.Start{Flow: twoSteps, Document: flow.Document{Type: "doc", ID: doc2}, LegalEntity: le,
			KeyRevision: 2, Digests: []string{"a", "b"}, Bindings: []flow.Binding{{LegalEntity: le, PositionID: position}, {Principal: dave}},
			Initiator: initiator})
		return err
	}))
	err = w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := w.k.StartIn(ctx, tx, flow.Start{Flow: twoSteps, Document: flow.Document{Type: "doc", ID: doc2}, LegalEntity: le,
			KeyRevision: 2, Digests: []string{"a", "b"}, Bindings: []flow.Binding{{LegalEntity: le, PositionID: position}, {Principal: dave}},
			Initiator: initiator})
		return err // the failed insert aborted the transaction: it rolls back
	})
	assert.ErrorIs(t, err, flow.ErrLive, "at most one running or approved instance (P17.4)")
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return w.k.WithdrawIn(ctx, tx, flow.Document{Type: "doc", ID: doc2})
	}))
	var states []string
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `SELECT t.state FROM be_flow_task t JOIN be_flow_instance i ON i.id = t.instance_id
			WHERE i.document_id = $1 AND i.state = 'withdrawn'`, doc2)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				return err
			}
			states = append(states, s)
		}
		return rows.Err()
	}))
	assert.Equal(t, []string{"superseded"}, states, "withdrawn: its open task superseded")
}

// P17.8: unprovable holders pause the task; nobody to decide pauses it too; the re-resolution makes it pending; the
// heartbeat while tasks wait (P17.18).
func TestPausedTasksResolveAgain(t *testing.T) {
	w := newWorld(t, "flow_paused")
	w.dir.set([]string{alice}, false)
	_, task := w.start(oneStep)
	p := w.task(task)
	assert.Equal(t, flow.Paused, p.State)
	assert.Equal(t, flow.ReasonProjectionIncomplete, *p.PausedReason)
	assert.Empty(t, p.Candidates)
	w.dir.set([]string{eve}, true) // the only holder is the initiator's person
	require.NoError(t, w.k.ResolvePaused(w.ctx))
	p = w.task(task)
	assert.Equal(t, flow.ReasonNoAssignee, *p.PausedReason)
	w.dir.set([]string{eve, bob}, true)
	require.NoError(t, w.k.ResolvePaused(w.ctx))
	p = w.task(task)
	assert.Equal(t, flow.Pending, p.State)
	assert.Equal(t, []string{bob}, p.Candidates)
	require.NoError(t, w.k.Heartbeat(w.ctx))
	var beats int
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM published WHERE type = 'test.purchase_task.heartbeat.v1'`).Scan(&beats)
	}))
	assert.Equal(t, 1, beats)
}
