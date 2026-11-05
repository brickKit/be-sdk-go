//go:build integration

package extcmd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeExternal is an external system: sends answer by a scripted class, queries by what it holds.
type fakeExternal struct {
	mu        sync.Mutex
	classes   []string          // what each send answers, in turn (succeeded when the script is over)
	held      map[string]string // reference -> state
	sends     []string          // the references sent, in order
	queries   []string          // the references queried, in order
	queryDown bool
}

func (f *fakeExternal) Send(_ context.Context, c Command) (Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sends = append(f.sends, c.Reference)
	class := Succeeded
	if len(f.classes) > 0 {
		class, f.classes = f.classes[0], f.classes[1:]
	}
	if class == "error" {
		return Result{}, errors.New("read timeout")
	}
	if class != NotReached && class != MaybeReached {
		f.held[c.Reference] = class
	}
	return Result{Class: class, ExternalID: "ext-" + c.Reference, Response: "ok"}, nil
}

func (f *fakeExternal) Query(_ context.Context, c Command) (Answer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, c.Reference)
	if f.queryDown {
		return Answer{}, errors.New("query endpoint down")
	}
	s, ok := f.held[c.Reference]
	return Answer{Found: ok, State: s}, nil
}

func (f *fakeExternal) sent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sends...)
}

type world struct {
	t     *testing.T
	d     *db.DB
	admin *pgx.Conn
	ext   *fakeExternal
	c     *Commands
}

func newWorld(t *testing.T, name string, ops map[string]Capabilities) *world {
	d, admin := dbtest.Schema(t, name, nil)
	ext := &fakeExternal{held: map[string]string{}}
	return &world{t: t, d: d, admin: admin, ext: ext, c: &Commands{DB: d, Component: "demo/pay", Adapter: ext, Ops: ops}}
}

func (w *world) request(ref string, params map[string]any) (Command, string, error) {
	var cmd Command
	var state string
	err := w.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		var err error
		cmd, state, err = w.c.Request(ctx, tx, "demo/sales", ref, "pay", params, nil)
		return err
	})
	return cmd, state, err
}

func (w *world) run() {
	w.t.Helper()
	_, err := w.c.Run(context.Background())
	require.NoError(w.t, err)
}

func (w *world) state(id string) (s string) {
	w.t.Helper()
	require.NoError(w.t, w.admin.QueryRow(context.Background(), "SELECT state FROM be_external_command WHERE id = $1", id).Scan(&s))
	return s
}

func (w *world) due(id string) { // the backoff over: due now
	pgtest.Exec(w.t, w.admin, "UPDATE be_external_command SET next_at = now() - interval '1 second' WHERE id = '"+id+"'")
}

var (
	plain  = Capabilities{Query: true}
	dedupe = Capabilities{Query: true, DedupeWindow: 24 * time.Hour}
	amount = map[string]any{"amount": "100.00", "currency": "EUR"}
)

// P13.2, EI05: idempotent by the requester and its reference; other parameters under the same reference refused.
func TestARequestIsIdempotentByItsReference(t *testing.T) {
	w := newWorld(t, "extcmd_req", map[string]Capabilities{"pay": plain})
	a, s, err := w.request("r1", amount)
	require.NoError(t, err)
	assert.Equal(t, Prepared, s)
	b, _, err := w.request("r1", amount)
	require.NoError(t, err)
	assert.Equal(t, a.ID, b.ID, "the same command")
	_, _, err = w.request("r1", map[string]any{"amount": "120.00", "currency": "EUR"})
	assert.True(t, errs.Is(err, "be", errs.ReasonIdempotencyKeyReused), "EI05: %v", err)
	var n int
	require.NoError(t, w.admin.QueryRow(context.Background(), "SELECT count(*) FROM be_external_command").Scan(&n))
	assert.Equal(t, 1, n)
	err = w.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, _, err := w.c.Request(ctx, tx, "demo/sales", "r2", "refund", amount, nil)
		return err
	})
	assert.Error(t, err, "an operation the adapter does not declare")
}

// A send that succeeds: one attempt that may have reached, final.
func TestASuccessfulSend(t *testing.T) {
	w := newWorld(t, "extcmd_ok", map[string]Capabilities{"pay": plain})
	c, _, err := w.request("r1", amount)
	require.NoError(t, err)
	w.run()
	assert.Equal(t, Succeeded, w.state(c.ID))
	var reached bool
	var ext string
	require.NoError(t, w.admin.QueryRow(context.Background(), `SELECT a.reached, c.external_id FROM be_external_attempt a
		JOIN be_external_command c ON c.id = a.command WHERE a.command = $1`, c.ID).Scan(&reached, &ext))
	assert.True(t, reached)
	assert.Equal(t, "ext-r1", ext)
	w.run()
	assert.Equal(t, []string{"r1"}, w.ext.sent(), "a final command is not processed again")
}

// EI01, EI04: a read timeout is unknown and queried first; "not found" with dedupe sends again, same reference.
func TestAnUnknownResultIsQueriedFirst(t *testing.T) {
	w := newWorld(t, "extcmd_unknown", map[string]Capabilities{"pay": dedupe})
	w.ext.classes = []string{"error"}
	c, _, err := w.request("r1", amount)
	require.NoError(t, err)
	w.run()
	assert.Equal(t, Unknown, w.state(c.ID), "EI01")
	assert.Equal(t, []string{"r1"}, w.ext.sent())
	w.run()
	assert.Equal(t, Succeeded, w.state(c.ID), "not found, the external system deduplicates: sent again (EI04)")
	assert.Equal(t, []string{"r1", "r1"}, w.ext.sent(), "the same reference")
	var deadline *time.Time
	require.NoError(t, w.admin.QueryRow(context.Background(), "SELECT resend_deadline FROM be_external_command WHERE id = $1",
		c.ID).Scan(&deadline))
	assert.NotNil(t, deadline, "P13.7: written after a send that may have reached")
}

// EI03: not found, no dedupe, no expiry, no longest processing time: manual, not sent again; EI02: the query down
// keeps it unknown; a person registers the outcome (P13.8).
func TestNotFoundWithoutDedupeGoesToManual(t *testing.T) {
	w := newWorld(t, "extcmd_manual", map[string]Capabilities{"pay": plain})
	w.ext.classes = []string{MaybeReached}
	c, _, err := w.request("r1", amount)
	require.NoError(t, err)
	w.run()
	require.Equal(t, Unknown, w.state(c.ID))
	w.ext.queryDown = true
	w.run()
	assert.Equal(t, Unknown, w.state(c.ID), "EI02: the query unavailable: unknown, backoff")
	var next time.Time
	require.NoError(t, w.admin.QueryRow(context.Background(), "SELECT next_at FROM be_external_command WHERE id = $1", c.ID).Scan(&next))
	assert.True(t, next.After(time.Now().Add(30*time.Second)), "backed off")
	w.ext.queryDown = false
	w.due(c.ID)
	w.run()
	assert.Equal(t, Manual, w.state(c.ID), "EI03")
	assert.Equal(t, []string{"r1"}, w.ext.sent(), "never sent again")
	require.NoError(t, w.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		return w.c.Register(ctx, tx, c.ID, Failed, "the bank confirms it never arrived and is closed")
	}))
	assert.Equal(t, Failed, w.state(c.ID))
}

// A connection failure retries later; a send whose claimant is gone is unknown, never sent again (EI30).
func TestRetriesAndTakeovers(t *testing.T) {
	w := newWorld(t, "extcmd_retry", map[string]Capabilities{"pay": plain})
	w.ext.classes = []string{NotReached}
	c, _, err := w.request("r1", amount)
	require.NoError(t, err)
	w.run()
	assert.Equal(t, RetryWait, w.state(c.ID))
	w.run()
	assert.Len(t, w.ext.sent(), 1, "not before the backoff")
	w.due(c.ID)
	w.run()
	assert.Equal(t, Succeeded, w.state(c.ID))
	var deadline *time.Time
	require.NoError(t, w.admin.QueryRow(context.Background(), "SELECT first_reached_at FROM be_external_command WHERE id = $1",
		c.ID).Scan(&deadline))
	require.NotNil(t, deadline, "the second send may have reached")

	d, _, err := w.request("r2", amount)
	require.NoError(t, err)
	pgtest.Exec(t, w.admin, "UPDATE be_external_command SET state = 'sending', next_at = now() - interval '1 second' WHERE id = '"+d.ID+"'")
	w.run()
	assert.Equal(t, Unknown, w.state(d.ID), "EI30: the claimant gone while sending")
	assert.NotContains(t, w.ext.sent(), "r2")
}

// P13.4, EI40: past the resend deadline at the claim: unknown, not sent.
func TestPastTheDeadlineNothingIsSent(t *testing.T) {
	w := newWorld(t, "extcmd_deadline", map[string]Capabilities{"pay": dedupe})
	c, _, err := w.request("r1", amount)
	require.NoError(t, err)
	pgtest.Exec(t, w.admin, "UPDATE be_external_command SET state = 'retry_wait', resend_deadline = now() - interval '1 minute' WHERE id = '"+c.ID+"'")
	w.run()
	assert.Equal(t, Unknown, w.state(c.ID))
	assert.Empty(t, w.ext.sent())
}

// P13.10: query-only sends nothing; an unknown command is still queried.
func TestQueryOnlySendsNothing(t *testing.T) {
	w := newWorld(t, "extcmd_qo", map[string]Capabilities{"pay": plain})
	w.c.QueryOnly = true
	c, _, err := w.request("r1", amount)
	require.NoError(t, err)
	w.run()
	assert.Equal(t, Prepared, w.state(c.ID))
	assert.Empty(t, w.ext.sent())
	w.ext.held["r2"] = Succeeded
	d, _, err := w.request("r2", amount)
	require.NoError(t, err)
	pgtest.Exec(t, w.admin, "UPDATE be_external_command SET state = 'unknown' WHERE id = '"+d.ID+"'")
	w.run()
	assert.Equal(t, Succeeded, w.state(d.ID), "queried")

	// "not found" where the table would send again (dedupe, before the deadline): query-only keeps it unknown
	w.c.Ops = map[string]Capabilities{"pay": dedupe}
	e, _, err := w.request("r3", amount)
	require.NoError(t, err)
	pgtest.Exec(t, w.admin, "UPDATE be_external_command SET state = 'unknown', first_reached_at = now() WHERE id = '"+e.ID+"'")
	w.run()
	assert.Equal(t, Unknown, w.state(e.ID), "not sent again in query-only")
	assert.Empty(t, w.ext.sent())
}

type store struct {
	mu   sync.Mutex
	down bool
	recs []evidence.Sealed
}

func (s *store) Append(_ context.Context, recs []evidence.Sealed) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return nil, errors.New("evidence store down")
	}
	s.recs = append(s.recs, recs...)
	out := make([]string, len(recs))
	for i := range out {
		out[i] = "accepted"
	}
	return out, nil
}

// P13.4: an irrevocable command is sent only after its issue record reached the evidence store (or the alert
// channel); neither, or no evidence at all: not sent, retried later.
func TestAnIrrevocableCommandNeedsItsIssueRecord(t *testing.T) {
	w := newWorld(t, "extcmd_irrev", map[string]Capabilities{"pay": {Query: true, Irrevocable: true}})
	c, _, err := w.request("r1", amount)
	require.NoError(t, err)
	w.run()
	assert.Equal(t, RetryWait, w.state(c.ID), "no evidence writer: never sent")
	assert.Empty(t, w.ext.sent())

	st := &store{down: true}
	w.c.Evidence = &evidence.Writer{DB: w.d, Component: "demo/pay", Instance: "i1", Store: st}
	w.due(c.ID)
	w.run()
	assert.Equal(t, RetryWait, w.state(c.ID), "the store down and no alert channel: not sent")
	assert.Empty(t, w.ext.sent())

	st.down = false
	w.due(c.ID)
	w.run()
	assert.Equal(t, Succeeded, w.state(c.ID))
	assert.Equal(t, []string{"r1"}, w.ext.sent())
	require.Len(t, st.recs, 1, "the issue record before the send")
	assert.Contains(t, string(st.recs[0].Record), `"external_command_issued"`)
	assert.Contains(t, string(st.recs[0].Record), `"phase":"intent"`)
}
