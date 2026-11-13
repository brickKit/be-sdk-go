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
	"github.com/brickKit/be-sdk-go/v3/channel"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/flow"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/notify"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A component of its own: documents with a state and a key revision, and the events the kernel publishes (one row an
// emit, in the emit's transaction).
var tables = fstest.MapFS{"0001_demo.up.sql": {Data: []byte(`
CREATE TABLE doc (id uuid PRIMARY KEY, state text NOT NULL DEFAULT 'submitted', key_revision bigint NOT NULL DEFAULT 1,
	legal_entity text NOT NULL, decided_by text, comment text);
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
	twoSteps  = "demo.two"       // a position (any), then a person; a rejection returns the document
	oneStep   = "demo.one"       // a position (single); a rejection rejects it
	commented = "demo.commented" // a person (single); people approve it, a rejection says why
	either    = "demo.either"    // a position or a person, as the start binds it
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
				ReadOperation: "demo.doc.read", OnReject: flow.Rejected, Steps: []flow.Step{{Form: flow.Single, Rule: flow.Position}}},
			{Key: commented, Version: 1, TaskType: "test.purchase.commented.approval", DocumentType: "doc",
				Operation: "demo.doc.approve", ReadOperation: "demo.doc.read", OnReject: flow.Returned, RejectComment: true,
				Manual: true, Steps: []flow.Step{{Form: flow.Single, Rule: flow.Person}}},
			{Key: either, Version: 1, TaskType: "test.purchase.either.approval", DocumentType: "doc", Operation: "demo.doc.approve",
				ReadOperation: "demo.doc.read", OnReject: flow.Returned, Steps: []flow.Step{{Form: flow.Any, Rule: flow.PositionOrPerson}}}},
		Hooks: flow.Hooks{
			Document: func(ctx context.Context, tx db.Tx, doc flow.Document) (int64, string, authz.Facts, error) {
				var rev int64
				var entity string
				err := tx.QueryRow(ctx, `SELECT key_revision, legal_entity FROM doc WHERE id = $1 FOR UPDATE`, doc.ID).Scan(&rev, &entity)
				return rev, "2026-11-12", authz.Facts{"legal_entity": authz.Value(entity)}, err
			},
			Ended: func(ctx context.Context, tx db.Tx, i flow.Instance) error {
				var by, comment *string
				if i.Ending != nil {
					by, comment = &i.Ending.By, i.Ending.Comment
				}
				_, err := tx.Exec(ctx, `UPDATE doc SET state = $2, decided_by = $3, comment = $4 WHERE id = $1`, i.Document.ID,
					string(i.State), by, comment)
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
		if flowKey == commented {
			b = []flow.Binding{{Principal: dave}}
		}
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
	assert.Equal(t, flow.Approved, w.task(second).Instance.State, "the projection too (P17.15)")
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
	assert.Equal(t, flow.Rejected, w.task(task).Instance.State, "the decided task's projection carries the instance's end (P17.15)")
	assert.Equal(t, end.Version, w.task(task).Version)
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

func (w *world) docRow(doc string) (state string, by, comment *string) {
	w.t.Helper()
	require.NoError(w.t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT state, decided_by, comment FROM doc WHERE id = $1`, doc).Scan(&state, &by, &comment)
	}))
	return state, by, comment
}

func (w *world) byDocument(who, key, doc string, c flow.Choice, comment *string) (flow.Task, error) {
	return w.k.DecideDocument(w.ctx, w.subject(who), key, flow.Document{Type: "doc", ID: doc}, c, comment)
}

// P17.1, P17.6, P17.14: the end hook gets the deciding decision; a definition that wants a comment on a rejection
// refuses one without; the component's own approve and reject routes decide by the document.
func TestDecisionsByTheDocument(t *testing.T) {
	w := newWorld(t, "flow_bydoc")
	doc, _ := w.start(oneStep)
	_, err := w.byDocument("account-stranger", "", doc, flow.Approve, nil)
	assert.Equal(t, errs.ReasonNotFound, reason(err), "one who may not see it")
	_, err = w.byDocument(dave, "", doc, flow.Approve, nil)
	assert.Equal(t, flow.ReasonNotAssignee, reason(err), "dave is not a holder: the open task answers why")
	done, err := w.byDocument(alice, "", doc, flow.Approve, nil)
	require.NoError(t, err)
	assert.Equal(t, flow.Approved, done.Instance.State)
	again, err := w.byDocument(alice, "", doc, flow.Approve, nil)
	require.NoError(t, err, "the same caller and decision without a key: the first result (P17.14)")
	assert.Equal(t, done.Version, again.Version)
	_, err = w.byDocument(bob, "", doc, flow.Approve, nil)
	assert.Equal(t, flow.ReasonTaskNotPending, reason(err), "decided already")
	_, err = w.byDocument(alice, "", doc, flow.Reject, nil)
	assert.Equal(t, flow.ReasonTaskNotPending, reason(err), "another decision by the same caller is another key")

	two, _ := w.start(twoSteps) // alice approves step 1; her repeat replays it although step 2 is open
	first, err := w.byDocument(alice, "", two, flow.Approve, nil)
	require.NoError(t, err)
	replay, err := w.byDocument(alice, "", two, flow.Approve, nil)
	require.NoError(t, err)
	assert.Equal(t, first.ID, replay.ID)
	assert.Equal(t, first.Version, replay.Version)
	state, by, _ := w.docRow(doc)
	assert.Equal(t, "approved", state)
	require.NotNil(t, by, "the end hook got the decision (P17.6)")
	assert.Equal(t, alice, *by)

	_, err = w.byDocument(alice, "", ids.New().String(), flow.Approve, nil)
	assert.Equal(t, flow.ReasonTaskNotPending, reason(err), "a document without an instance")

	doc2, task2 := w.start(commented)
	_, err = w.decide(dave, task2, "k-bare", w.task(task2).Version, flow.Reject)
	assert.Equal(t, errs.ReasonRequestInvalid, reason(err), "the definition wants a comment on a rejection")
	why := "the price is wrong"
	r, err := w.byDocument(dave, "k-why", doc2, flow.Reject, &why)
	require.NoError(t, err)
	assert.Equal(t, flow.Returned, r.Instance.State)
	state, by, comment := w.docRow(doc2)
	assert.Equal(t, "returned", state)
	require.NotNil(t, comment)
	assert.Equal(t, why, *comment)
	assert.Equal(t, dave, *by)
}

// P17.5, P17.12: a flow choice without manual approval leaves an approved instance the release check passes; a
// definition people must approve refuses it; the person behind an account.
func TestAnAutomaticApproval(t *testing.T) {
	w := newWorld(t, "flow_auto")
	doc := ids.New().String()
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO doc (id, legal_entity, state) VALUES ($1, $2, 'approved')`, doc, le); err != nil {
			return err
		}
		i, err := w.k.StartIn(ctx, tx, flow.Start{Flow: oneStep, Document: flow.Document{Type: "doc", ID: doc}, LegalEntity: le,
			KeyRevision: 1, Digests: []string{"sha256:auto"}, Initiator: initiator, Automatic: true})
		if err != nil {
			return err
		}
		assert.Equal(t, flow.Approved, i.State)
		var tasks int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM be_flow_task WHERE instance_id = $1`, i.ID).Scan(&tasks); err != nil {
			return err
		}
		assert.Zero(t, tasks, "no task")
		return w.k.ReleaseIn(ctx, tx, flow.Document{Type: "doc", ID: doc}, 1, []string{"sha256:auto"})
	}))
	err := w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := w.k.StartIn(ctx, tx, flow.Start{Flow: commented, Document: flow.Document{Type: "doc", ID: ids.New().String()},
			LegalEntity: le, KeyRevision: 1, Digests: []string{"sha256:x"}, Initiator: initiator, Automatic: true})
		return err
	})
	assert.ErrorIs(t, err, flow.ErrManual)
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		p, err := w.k.PersonOf(ctx, tx, eve)
		assert.Equal(t, "person-1", p)
		return err
	}))
}

// startBound makes a document and starts the either flow with the binding; answers the document, the task, the error.
func (w *world) startBound(b flow.Binding) (string, string, error) {
	doc := ids.New().String()
	var task string
	err := w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO doc (id, legal_entity) VALUES ($1, $2)`, doc, le); err != nil {
			return err
		}
		i, err := w.k.StartIn(ctx, tx, flow.Start{Flow: either, Document: flow.Document{Type: "doc", ID: doc}, LegalEntity: le,
			KeyRevision: 1, Digests: []string{"sha256:e"}, Bindings: []flow.Binding{b}, Initiator: initiator})
		if err != nil {
			return err
		}
		return tx.QueryRow(ctx, `SELECT id::text FROM be_flow_task WHERE instance_id = $1`, i.ID).Scan(&task)
	})
	return doc, task, err
}

// P17.3: a step of a position or a person resolves and checks by what the start bound.
func TestAStepBoundAtTheStart(t *testing.T) {
	w := newWorld(t, "flow_either")
	_, byPosition, err := w.startBound(flow.Binding{LegalEntity: le, PositionID: position})
	require.NoError(t, err)
	assert.Equal(t, []string{alice, bob}, w.task(byPosition).Candidates, "the position's holders")
	doc, byPerson, err := w.startBound(flow.Binding{Principal: dave})
	require.NoError(t, err)
	assert.Equal(t, []string{dave}, w.task(byPerson).Candidates, "the principal")
	_, err = w.decide(dave, byPerson, "k-dave", w.task(byPerson).Version, flow.Approve)
	require.NoError(t, err, "a person binding has no position to hold")
	assert.Equal(t, "approved", w.docState(doc))
	_, _, err = w.startBound(flow.Binding{LegalEntity: le, PositionID: position, Principal: dave})
	assert.Error(t, err, "both")
	_, _, err = w.startBound(flow.Binding{})
	assert.Error(t, err, "neither")
}

// enqueuer records the queued items.
type enqueuer struct{ jobs []string }

func (e *enqueuer) Enqueue(_ context.Context, _ db.Tx, job string, _ any) (string, error) {
	e.jobs = append(e.jobs, job)
	return "", nil
}

// P17.8: a change of the employee projection queues a re-resolution while tasks wait paused (and none is queued
// already); without paused tasks it queues nothing.
func TestTheEmployeeProjectionsChangeResolvesAgain(t *testing.T) {
	w := newWorld(t, "flow_changed")
	q := &enqueuer{}
	changed := func() {
		t.Helper()
		require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error { return w.k.EmployeesChanged(ctx, tx, q) }))
	}
	changed()
	assert.Empty(t, q.jobs, "nothing waits")
	w.dir.set([]string{alice}, false)
	_, _ = w.start(oneStep)
	changed()
	assert.Equal(t, []string{flow.JobResolveNow}, q.jobs, "a paused task: resolved again at once")
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error { // one queued already
		_, err := tx.Exec(ctx, `INSERT INTO be_work_item (id, job, kind, state, max_attempts, effect_class, operation_id,
			args, run_at) VALUES ($1, $2, 'queue', 'pending', 1, 'local', $1, '{}', now())`, ids.New().String(), flow.JobResolveNow)
		return err
	}))
	changed()
	assert.Len(t, q.jobs, 1, "not a second one while one waits")
}

// P17.8, P17.10: a position task decided after the deployment lost its employee directory is not qualified (nothing
// proves the holder), it does not fail.
func TestAPositionTaskWithoutTheDirectory(t *testing.T) {
	w := newWorld(t, "flow_nodir")
	_, task := w.start(oneStep)
	w.k.Directory = nil
	_, err := w.decide(alice, task, "k-nodir", w.task(task).Version, flow.Approve)
	assert.Equal(t, flow.ReasonNotQualified, reason(err))
}

// intents reads the notification intents of a task, in the order they were emitted (P18.10).
func (w *world) intents(task string) []notify.Intent {
	w.t.Helper()
	var out []notify.Intent
	require.NoError(w.t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `SELECT data FROM published WHERE type = 'test.purchase_notification.intent.v1'
			AND data->>'collapse_key' = $1 ORDER BY at`, "task:"+task)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var b []byte
			var in notify.Intent
			if err := rows.Scan(&b); err != nil {
				return err
			}
			require.NoError(w.t, json.Unmarshal(b, &in))
			out = append(out, in)
		}
		return rows.Err()
	}))
	return out
}

// P18.10: a pending task is delivered to its candidates with its version as the sequence; deciding it withdraws it
// (a newer sequence); the next step's task is delivered to its own candidates; a paused task delivers nothing until
// it is resolved again.
func TestTheTasksNotificationIntents(t *testing.T) {
	w := newWorld(t, "flow_intents")
	_, task := w.start(twoSteps)
	first := w.task(task)
	got := w.intents(task)
	require.Len(t, got, 1)
	in := got[0]
	assert.Equal(t, notify.Deliver, in.Kind)
	assert.Equal(t, "test/purchase", in.Sender)
	assert.Equal(t, notify.ApprovalTask, in.Category)
	assert.Equal(t, []notify.Recipient{{Principal: alice}, {Principal: bob}}, in.Recipients)
	assert.Equal(t, first.Version, in.CollapseSeq)
	assert.Equal(t, "/workflow/tasks/"+task, in.Link)
	assert.Equal(t, flow.TaskTemplate, in.Template)
	assert.Equal(t, first.TaskType, in.Params["task_type"])
	assert.NotEmpty(t, in.Title)
	require.NoError(t, notify.Check(in), "it follows the shared contract")

	_, err := w.decide(bob, task, "k-intent", first.Version, flow.Approve)
	require.NoError(t, err)
	got = w.intents(task)
	require.Len(t, got, 2)
	assert.Equal(t, notify.Withdraw, got[1].Kind, "a decided task withdraws its message")
	assert.Equal(t, w.task(task).Version, got[1].CollapseSeq)
	assert.Greater(t, got[1].CollapseSeq, got[0].CollapseSeq)
	assert.Empty(t, got[1].Recipients)

	w.dir.set([]string{alice}, false)
	_, paused := w.start(oneStep)
	got = w.intents(paused)
	require.Len(t, got, 1)
	assert.Equal(t, notify.Withdraw, got[0].Kind, "a paused task delivers nothing")
	w.dir.set([]string{bob}, true)
	require.NoError(t, w.k.ResolvePaused(w.ctx))
	got = w.intents(paused)
	require.Len(t, got, 2)
	assert.Equal(t, notify.Deliver, got[1].Kind)
	assert.Equal(t, []notify.Recipient{{Principal: bob}}, got[1].Recipients)
}

const dingtalk = "integration/im-dingtalk"

// act hands one action event of the channel member to the kernel's subscription, in its own transaction, as the
// consumer would (P17.20).
func (w *world) act(a channel.Action, source string) error {
	w.t.Helper()
	subs := w.k.ActionSubs()
	require.Len(w.t, subs, 1)
	b, _ := json.Marshal(a)
	return w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return subs[0].Handle(ctx, tx, events.Received{Stream: "integration.im_dingtalk", Source: "urn:be:" + source,
			Type: channel.ActionType(dingtalk), RecordID: a.ID, Data: b})
	})
}

func (w *world) action(principal, grant, act string) channel.Action {
	return channel.Action{ID: ids.New().String(), Channel: dingtalk, Audience: "test/purchase", Principal: principal,
		GrantID: grant, Action: act, ClickedAt: time.Now().UTC().Format(time.RFC3339Nano)}
}

func (w *world) grantResult(grant string) (action, result *string) {
	require.NoError(w.t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT result_action, result FROM be_flow_action_grant WHERE id = $1`, grant).Scan(&action, &result)
	}))
	return
}

func (w *world) decisions(task string) int {
	var n int
	require.NoError(w.t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM be_flow_decision WHERE task_id = $1`, task).Scan(&n)
	}))
	return n
}

// told is the in-app intents the kernel wrote to a holder whose action was not carried out (P20.8).
func (w *world) told(principal string) int {
	var n int
	require.NoError(w.t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM published WHERE type = 'test.purchase_notification.intent.v1'
			AND data->>'collapse_key' IS NULL AND data->'recipients'->0->>'principal' = $1`, principal).Scan(&n)
	}))
	return n
}

// P17.19, P17.20, P20.5, P20.7, P20.8: a pending task's intent carries one grant per principal candidate and the
// channels that take actions; an action on it decides the task once; the refusals in order, audited, recorded and
// told to the holder when they are the grant's own; a view that cannot decide records nothing.
func TestInteractiveActions(t *testing.T) {
	w := newWorld(t, "flow_actions")
	w.k.ActionSources, w.k.Subject = []string{dingtalk}, w.subject

	doc, task := w.start(oneStep)
	in := w.intents(task)[0]
	require.NoError(t, notify.Check(in))
	assert.Equal(t, []string{dingtalk}, in.ActionChannels)
	require.Contains(t, in.Actions, alice)
	require.Contains(t, in.Actions, bob)
	assert.NotContains(t, in.Actions, eve, "only the candidates (eve is the initiator's person)")
	ga, gb := in.Actions[alice], in.Actions[bob]
	assert.Equal(t, []notify.Choice{{Action: "approve", Label: "同意"}, {Action: "reject", Label: "驳回"}}, ga.Actions,
		"a flow whose rejection needs no comment has both buttons")

	// the refusals before the grant is the holder's: audited, nothing recorded, nobody told
	other := w.action(alice, ga.ID, "approve")
	other.Audience = "erp/payment"
	require.NoError(t, w.act(other, dingtalk))
	assert.Empty(t, w.aud.denials, "another owner's action is acknowledged silently")
	require.NoError(t, w.act(w.action(alice, ga.ID, "approve"), "test/impostor"))
	require.NoError(t, w.act(w.action(alice, ids.New().String(), "approve"), dingtalk))
	require.NoError(t, w.act(w.action(bob, ga.ID, "approve"), dingtalk))
	require.NoError(t, w.act(w.action(alice, ga.ID, "delegate"), dingtalk))
	assert.Equal(t, []string{"not_the_sender", "unknown_grant", "not_the_holder", "action_not_allowed"}, w.aud.denials)
	a, _ := w.grantResult(ga.ID)
	assert.Nil(t, a)
	assert.Equal(t, "pending", string(w.task(task).State))

	// a view that cannot decide: nacked, nothing recorded (P20.8)
	w.k.Subject = func(p string) *authz.Subject {
		return &authz.Subject{Principal: p, Defs: defs, Holder: authz.NewHolder("test/purchase", grants{},
			slog.New(slog.NewTextHandler(io.Discard, nil))), At: time.Now()}
	}
	assert.Error(t, w.act(w.action(alice, ga.ID, "approve"), dingtalk), "retried: the event is nacked")
	a, _ = w.grantResult(ga.ID)
	assert.Nil(t, a)
	w.k.Subject = w.subject

	// executed once: the clicked button decides the task as its holder; the same again answers it, the other action
	// is refused (GRANT_USED)
	require.NoError(t, w.act(w.action(alice, ga.ID, "approve"), dingtalk))
	assert.Equal(t, "approved", w.docState(doc))
	a, r := w.grantResult(ga.ID)
	require.NotNil(t, a)
	assert.Equal(t, "approve", *a)
	assert.Equal(t, "approved", *r)
	require.NoError(t, w.act(w.action(alice, ga.ID, "approve"), dingtalk))
	assert.Equal(t, 1, w.decisions(task), "the same action again: one decision")
	require.NoError(t, w.act(w.action(alice, ga.ID, "reject"), dingtalk))
	assert.Equal(t, "GRANT_USED", w.aud.denials[len(w.aud.denials)-1])
	assert.Equal(t, 1, w.told(alice))

	// the task decided meanwhile: bob's grant is refused with the decision's reason, recorded, and bob is told
	require.NoError(t, w.act(w.action(bob, gb.ID, "approve"), dingtalk))
	_, r = w.grantResult(gb.ID)
	require.NotNil(t, r)
	assert.Equal(t, "TASK_NOT_PENDING", *r)
	assert.Equal(t, 1, w.told(bob))

	// expired: clicked after the grant's expiry
	_, task2 := w.start(oneStep)
	g2 := w.intents(task2)[0].Actions[alice]
	require.NoError(t, w.d.Tx(w.ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE be_flow_action_grant SET expires_at = now() - interval '2 hours' WHERE id = $1`, g2.ID)
		return err
	}))
	require.NoError(t, w.act(w.action(alice, g2.ID, "approve"), dingtalk))
	_, r = w.grantResult(g2.ID)
	require.NotNil(t, r)
	assert.Equal(t, "expired", *r)
	assert.Equal(t, "pending", string(w.task(task2).State))

	// a flow whose rejection must say why has only the approve button
	_, task3 := w.start(commented)
	assert.Equal(t, []notify.Choice{{Action: "approve", Label: "同意"}}, w.intents(task3)[0].Actions[dave].Actions)
}
