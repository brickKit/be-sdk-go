//go:build integration

package evidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// A tightening in two phases (P12.16, P12.17): the intent reaches the store before the transaction; the conclusion
// and the change history are written in it, at the commit sequence; the transfer job sends the conclusion and
// deletes it; the change is reflected by the record's id (P12.18).
func TestATighteningIsWrittenInTwoPhases(t *testing.T) {
	d, admin := dbtest.Schema(t, "evidence1", nil)
	store := &stubStore{}
	var logs bytes.Buffer
	w := &Writer{DB: d, Store: store, Component: "demo/orders", Instance: "orders-1", Log: slog.New(slog.NewJSONHandler(&logs, nil))}
	ctx := context.Background()
	p, err := w.Intent(ctx, "security_tightening", sectestBob, order, map[string]any{"termination": "revoke"})
	require.NoError(t, err)
	assert.Equal(t, "store", p.Intent)
	require.Len(t, store.got, 1)
	in := fields(t, store.got[0])
	assert.Equal(t, "intent", in["phase"])
	assert.Nil(t, in["position"], "an intent has no position")
	assert.Equal(t, map[string]any{"component": "demo/orders", "instance": "orders-1"}, in["recorder"])
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error { return p.Effective(ctx, tx) }))
	var epoch string
	var seq int64
	require.NoError(t, admin.QueryRow(ctx, "SELECT epoch, commit_seq FROM be_change_history WHERE evidence_id = $1", p.ID).Scan(&epoch, &seq))
	assert.EqualValues(t, 1, seq)
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		ok, err := Reflected(ctx, tx, p.ID)
		assert.True(t, ok, "reflected by its id")
		other, _ := Reflected(ctx, tx, "0190f3a1-0000-7000-8000-00000000dead")
		assert.False(t, other)
		return err
	}))
	n, err := Transfer(ctx, d, store, w.log())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.Len(t, store.got, 2)
	eff := fields(t, store.got[1])
	assert.Equal(t, "effective", eff["phase"])
	assert.Equal(t, p.ID, eff["id"])
	assert.Equal(t, map[string]any{"epoch": epoch, "commit_seq": float64(seq)}, eff["position"])
	var left int
	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM be_evidence_outbox").Scan(&left))
	assert.Zero(t, left, "deleted after the receipt")
}

const sectestBob = "0190f3a1-0000-7000-8000-0000000000b2"

// The evidence store is down: the intent goes to the alert channel within the time limit and the change goes on;
// the alert channel down too: the double failure is logged and the change still goes on (P12.16, RC17, RC22).
func TestAnUnreachableStoreDoesNotHoldATighteningBack(t *testing.T) {
	d, _ := dbtest.Schema(t, "evidence2", nil)
	store := &stubStore{delay: 5 * time.Second}
	alert := &stubAlert{}
	var logs bytes.Buffer
	w := &Writer{DB: d, Store: store, Alert: alert, Component: "demo/orders", Instance: "orders-1",
		Log: slog.New(slog.NewJSONHandler(&logs, nil))}
	ctx := context.Background()
	start := time.Now()
	p, err := w.Intent(ctx, "security_tightening", sectestBob, order, map[string]any{"termination": "revoke"})
	require.NoError(t, err)
	assert.Less(t, time.Since(start), IntentTimeout+time.Second, "no longer than the time limit")
	assert.Equal(t, "alert", p.Intent)
	require.Len(t, alert.got, 1)
	assert.Equal(t, "intent", alert.got[0].(map[string]any)["phase"])
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error { return p.Effective(ctx, tx) }))

	alert.fail, store.delay, store.down = true, 0, true
	p, err = w.Intent(ctx, "security_tightening", sectestBob, order, map[string]any{"termination": "narrow"})
	require.NoError(t, err)
	assert.Equal(t, "lost", p.Intent)
	assert.Contains(t, logs.String(), `"event":"evidence_double_failure"`)
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error { return p.Effective(ctx, tx) }),
		"the tightening goes on")
	_, err = Transfer(ctx, d, store, w.log())
	assert.Error(t, err, "the store is down: the conclusions stay local")

	// P12.23: the double failure is itself a record, committed locally and sent once the store answers again: it
	// names the lost intent (identity, category, time), not its change
	store.mu.Lock()
	store.down = false
	store.mu.Unlock()
	_, err = Transfer(ctx, d, store, w.log())
	require.NoError(t, err)
	var lost map[string]any
	for _, r := range store.got {
		var m map[string]any
		require.NoError(t, json.Unmarshal(r.Record, &m))
		if m["category"] == DoubleFailure {
			lost = m
		}
	}
	require.NotNil(t, lost, "the double failure's record reached the store")
	assert.Equal(t, "general", lost["phase"])
	assert.Equal(t, sectestBob, lost["subject"])
	change := lost["change"].(map[string]any)
	assert.Equal(t, p.ID, change["record"])
	assert.Equal(t, "security_tightening", change["category"])
	assert.NotEmpty(t, change["at"])
	assert.NotContains(t, change, "termination", "not the lost record's change")
}

// A failed business transaction writes the cancelled conclusion, without a position (P12.9, P12.16); a record the
// store refuses is stopped and not sent again (P12.15).
func TestACancelledConclusionAndARefusedRecord(t *testing.T) {
	d, admin := dbtest.Schema(t, "evidence3", nil)
	store := &stubStore{}
	var logs bytes.Buffer
	w := &Writer{DB: d, Store: store, Component: "demo/orders", Instance: "orders-1", Log: slog.New(slog.NewJSONHandler(&logs, nil))}
	ctx := context.Background()
	p, err := w.Intent(ctx, "security_tightening", sectestBob, order, map[string]any{"termination": "revoke"})
	require.NoError(t, err)
	failed := d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := p.Effective(ctx, tx); err != nil {
			return err
		}
		return errors.New("a check failed")
	})
	require.Error(t, failed)
	require.NoError(t, p.Cancelled(ctx))
	var hist int
	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM be_change_history").Scan(&hist))
	assert.Zero(t, hist, "the history rolled back with the change")
	store.reply = func(s Sealed) string {
		if s.Phase == "cancelled" {
			return "conflict"
		}
		return "accepted"
	}
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := w.General(ctx, tx, "security_widening", sectestBob, order, map[string]any{"grant": "g1"})
		return err
	}))
	n, err := Transfer(ctx, d, store, w.log())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	var cancelledRec map[string]any
	for _, s := range store.got {
		if s.Phase == "cancelled" {
			cancelledRec = fields(t, s)
		}
	}
	require.NotNil(t, cancelledRec)
	assert.Nil(t, cancelledRec["position"], "a cancelled conclusion never committed")
	var stopped string
	require.NoError(t, admin.QueryRow(ctx, "SELECT stopped FROM be_evidence_outbox WHERE phase = 'cancelled'").Scan(&stopped))
	assert.Equal(t, "conflict", stopped)
	assert.Contains(t, logs.String(), `"event":"evidence_refused"`)
	before := len(store.got)
	_, err = Transfer(ctx, d, store, w.log())
	require.NoError(t, err)
	assert.Len(t, store.got, before, "a stopped row is not sent again")
}

// Evidence transactions are serialized from the commit sequence to the commit (P12.17): while one holds its number,
// another waits for it to commit and gets the next; so the sequence's order is the commit order.
func TestTheCommitSequenceFollowsTheCommitOrder(t *testing.T) {
	d, admin := dbtest.Schema(t, "evidence4", nil)
	w := &Writer{DB: d, Component: "demo/orders", Instance: "orders-1"}
	ctx := context.Background()
	numbered, aDone := make(chan struct{}), make(chan time.Time, 1)
	go func() {
		assert.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := w.General(ctx, tx, "security_widening", sectestBob, order, map[string]any{"grant": "a"})
			close(numbered)
			time.Sleep(400 * time.Millisecond) // A holds its number before committing
			return err
		}))
		aDone <- time.Now()
	}()
	<-numbered
	var bID string
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		bID, err = w.General(ctx, tx, "security_widening", sectestBob, order, map[string]any{"grant": "b"})
		return err
	}))
	bDone := time.Now()
	aAt := <-aDone
	assert.False(t, bDone.Before(aAt.Add(-50*time.Millisecond)), "B committed after A")
	var bSeq int64
	require.NoError(t, admin.QueryRow(ctx, "SELECT commit_seq FROM be_change_history WHERE evidence_id = $1", bID).Scan(&bSeq))
	assert.EqualValues(t, 2, bSeq, "B is numbered after A")
}

// Through the system surface: GRPCStore against the test deployment's evidence store, duplicates answered as such.
func TestTheGRPCStoreTalksToTheEvidenceStore(t *testing.T) {
	d, _ := dbtest.Schema(t, "evidence5", nil)
	world := sectest.New(t)
	tls := world.TLS(t, "demo/orders")
	conn, err := grpc.NewClient(world.AuditAddr, grpc.WithTransportCredentials(credentials.NewTLS(tls.ClientConfig("infra/audit"))))
	require.NoError(t, err)
	defer conn.Close()
	w := &Writer{DB: d, Store: GRPCStore{Conn: conn}, Component: "demo/orders", Instance: "orders-1"}
	ctx := context.Background()
	p, err := w.Intent(ctx, "security_tightening", sectestBob, order, map[string]any{"termination": "revoke"})
	require.NoError(t, err)
	assert.Equal(t, "store", p.Intent)
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error { return p.Effective(ctx, tx) }))
	_, err = Transfer(ctx, d, w.Store, w.log())
	require.NoError(t, err)
	kept := world.Evidence()
	assert.Contains(t, kept, p.ID+"/intent")
	assert.Contains(t, kept, p.ID+"/effective")
	world.EvidenceDown(true)
	p2, err := w.Intent(ctx, "security_tightening", sectestBob, order, map[string]any{"termination": "narrow"})
	require.NoError(t, err)
	assert.Equal(t, "lost", p2.Intent, "no store, no alert channel: a double failure")
}
