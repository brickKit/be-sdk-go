//go:build integration

package evidence

import (
	"context"
	"testing"
	"testing/fstest"

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

	_, err = ask(record("effective", "security_tightening", order, "demo/other"))
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "another recorder's record: %v", err)
	_, err = ask(record("cancelled", "security_tightening", order, "demo/orders"))
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "a cancelled record is not replayed: %v", err)
	_, err = ask([]byte(`{"record_type":"other"}`))
	assert.Error(t, err)
}
