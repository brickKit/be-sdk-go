//go:build integration

package evidence

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/evidencepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The owner's replay (P12.19-P12.21): a record re-executed by its category's function with the change history in one
// transaction, then reflected by its identity (nothing done again); an object that is gone is dangling (rolled back,
// nothing done); a category without a function is unsupported; another recorder's record or a phase that is not
// replayed is refused.
func TestTheOwnerReplaysItsEvidence(t *testing.T) {
	ctx := context.Background()
	d, admin := dbtest.Schema(t, "evreplay", fstest.MapFS{"0001_orders.up.sql": {Data: []byte(
		"CREATE TABLE orders (id text PRIMARY KEY, state text NOT NULL); CREATE TABLE touched (id text);")}})
	_, err := admin.Exec(ctx, "INSERT INTO evreplay.orders VALUES ('0190f3a1-0000-7000-8000-0000000000f1', 'open')")
	require.NoError(t, err)
	var replays Replays
	replays.Add("security_tightening", func(ctx context.Context, tx db.Tx, r Replayed) error {
		if _, err := tx.Exec(ctx, "INSERT INTO touched VALUES ($1)", r.ID); err != nil { // a write before the check
			return err
		}
		tag, err := tx.Exec(ctx, "UPDATE orders SET state = $2 WHERE id = $1", r.Object.ID, r.Change["termination"])
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrDangling
		}
		return nil
	})
	s := &ReplayServer{DB: d, Component: "demo/orders", Replays: &replays}
	w := &Writer{DB: d, Component: "demo/orders", Instance: "orders-1"}
	record := func(phase, category string, obj Object, component string) []byte {
		r := w.record("0190f3a1-0000-7000-8000-00000000e"+map[string]string{"effective": "001", "intent": "002",
			"general": "003", "cancelled": "004"}[phase], category, phase, sectestBob, obj, map[string]any{"termination": "revoked"}, nil)
		r["recorder"] = map[string]any{"component": component, "instance": "orders-1"}
		s, err := Seal(r)
		require.NoError(t, err)
		return s.Record
	}
	ask := func(raw []byte) (string, error) {
		out, err := s.Replay(ctx, &evidencepb.ReplayRequest{Record: raw, Recovery: "r1"})
		return out.GetOutcome(), err
	}
	state := func() string {
		var st string
		require.NoError(t, admin.QueryRow(ctx, "SELECT state FROM evreplay.orders").Scan(&st))
		return st
	}

	eff := record("effective", "security_tightening", order, "demo/orders")
	got, err := ask(eff)
	require.NoError(t, err)
	assert.Equal(t, Applied, got)
	assert.Equal(t, "revoked", state())
	var hist int
	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM evreplay.be_change_history WHERE evidence_id = $1",
		"0190f3a1-0000-7000-8000-00000000e001").Scan(&hist))
	assert.Equal(t, 1, hist, "the change history has the record's own identity")
	_, err = admin.Exec(ctx, "UPDATE evreplay.orders SET state = 'open'")
	require.NoError(t, err)
	got, err = ask(eff)
	require.NoError(t, err)
	assert.Equal(t, AlreadyReflected, got, "again: reflected, nothing done")
	assert.Equal(t, "open", state())

	gone := record("intent", "security_tightening", Object{Type: "demo.order", ID: "0190f3a1-0000-7000-8000-000000000999"}, "demo/orders")
	got, err = ask(gone)
	require.NoError(t, err)
	assert.Equal(t, Dangling, got)
	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM evreplay.be_change_history WHERE evidence_id = $1",
		"0190f3a1-0000-7000-8000-00000000e002").Scan(&hist))
	assert.Zero(t, hist, "a dangling replay leaves no history")
	var touched int
	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM evreplay.touched WHERE id = $1",
		"0190f3a1-0000-7000-8000-00000000e002").Scan(&touched))
	assert.Zero(t, touched, "nor anything the function wrote before it found the object gone: rolled back")

	got, err = ask(record("general", "security_widening", order, "demo/orders"))
	require.NoError(t, err)
	assert.Equal(t, Unsupported, got)

	// what the re-execution depends on has not arrived (P12.20): deferred, rolled back, no history; asked again later
	replays.Add("security_hold_release", func(ctx context.Context, tx db.Tx, r Replayed) error {
		if _, err := tx.Exec(ctx, "INSERT INTO touched VALUES ($1)", r.ID); err != nil {
			return err
		}
		return fmt.Errorf("the signal is not applied yet: %w", ErrDeferred)
	})
	got, err = ask(record("general", "security_hold_release", order, "demo/orders"))
	require.NoError(t, err)
	assert.Equal(t, Deferred, got)
	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM evreplay.be_change_history WHERE evidence_id = $1",
		"0190f3a1-0000-7000-8000-00000000e003").Scan(&hist))
	assert.Zero(t, hist, "a deferred replay leaves no history")
	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM evreplay.touched WHERE id = $1",
		"0190f3a1-0000-7000-8000-00000000e003").Scan(&touched))
	assert.Zero(t, touched, "nor anything it wrote: rolled back")

	_, err = ask(record("effective", "security_tightening", order, "demo/other"))
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "another recorder's record: %v", err)
	_, err = ask(record("cancelled", "security_tightening", order, "demo/orders"))
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "a cancelled record is not replayed: %v", err)
	_, err = ask([]byte(`{"record_type":"other"}`))
	assert.Error(t, err)
}

// A category whose effect lies outside the recovery group is checked, not re-executed (P12.20): outside any
// transaction, consistent or inconsistent (an error asking is inconsistent too), no change history written, so it is
// checked again on the next run; a record whose identity the change history has is reflected and not checked.
func TestTheOwnerChecksWhatLiesOutsideTheRecoveryGroup(t *testing.T) {
	ctx := context.Background()
	d, admin := dbtest.Schema(t, "evcheck", nil)
	var replays Replays
	var asked []Replayed
	replays.Check("credential_tightening", func(ctx context.Context, r Replayed) (bool, string, error) {
		asked = append(asked, r)
		if db.InTx(ctx) {
			return false, "", errors.New("the check runs in a transaction")
		}
		switch r.Change["kind"] {
		case "password":
			return true, "", nil
		case "second_factor":
			return false, "the factors are back", nil
		}
		return false, "", errors.New("the product cannot be reached")
	})
	require.NoError(t, replays.Err())
	s := &ReplayServer{DB: d, Component: "demo/iam", Replays: &replays}
	w := &Writer{DB: d, Component: "demo/iam", Instance: "iam-1"}
	at := time.Date(2026, 11, 5, 21, 0, 0, 123000000, time.UTC)
	w.Now = func() time.Time { return at }
	record := func(id, kind string) []byte {
		r := w.record("0190f3a1-0000-7000-8000-00000000c"+id, "credential_tightening", "intent", sectestBob,
			Object{Type: "demo.account", ID: sectestBob, Version: "3"}, map[string]any{"kind": kind}, nil)
		s, err := Seal(r)
		require.NoError(t, err)
		return s.Record
	}
	ask := func(raw []byte) (string, string) {
		out, err := s.Replay(ctx, &evidencepb.ReplayRequest{Record: raw, Recovery: "r1"})
		require.NoError(t, err)
		return out.GetOutcome(), out.GetDetail()
	}
	history := func() int {
		var n int
		require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM evcheck.be_change_history").Scan(&n))
		return n
	}

	got, _ := ask(record("001", "password"))
	assert.Equal(t, Consistent, got)
	require.Len(t, asked, 1)
	assert.True(t, asked[0].At.Equal(at), "the check sees the record's time: %v", asked[0].At)
	assert.Equal(t, sectestBob, asked[0].Object.ID)
	got, _ = ask(record("001", "password"))
	assert.Equal(t, Consistent, got, "checked again: nothing written that would make it reflected")
	assert.Len(t, asked, 2)
	got, detail := ask(record("002", "second_factor"))
	assert.Equal(t, Inconsistent, got)
	assert.Equal(t, "the factors are back", detail)
	got, detail = ask(record("003", "unknown"))
	assert.Equal(t, Inconsistent, got, "could not be asked: inconsistent, for review")
	assert.Contains(t, detail, "cannot be reached")
	assert.Zero(t, history(), "a check writes no change history")

	_, err := admin.Exec(ctx, `INSERT INTO evcheck.be_change_history (evidence_id, object_type, object_id, epoch, commit_seq)
		VALUES ('0190f3a1-0000-7000-8000-00000000c004', 'demo.account', $1, 1, 1)`, sectestBob)
	require.NoError(t, err)
	n := len(asked)
	got, _ = ask(record("004", "password"))
	assert.Equal(t, AlreadyReflected, got, "in the restored change history: reflected, not checked")
	assert.Len(t, asked, n)
}

// One category is either re-executed or checked (P12.20): registering both is a mistake the host refuses.
func TestACategoryIsReplayedOneWay(t *testing.T) {
	var r Replays
	r.Add("credential_tightening", func(context.Context, db.Tx, Replayed) error { return nil })
	r.Check("credential_tightening", func(context.Context, Replayed) (bool, string, error) { return true, "", nil })
	assert.Error(t, r.Err())
	var ok Replays
	ok.Add("a", func(context.Context, db.Tx, Replayed) error { return nil })
	ok.Check("b", func(context.Context, Replayed) (bool, string, error) { return true, "", nil })
	assert.NoError(t, ok.Err())
}
