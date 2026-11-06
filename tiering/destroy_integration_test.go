//go:build integration

package tiering_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/internal/evidencepb"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keeper is the evidence store: it keeps what it is sent.
type keeper struct {
	mu   sync.Mutex
	recs []map[string]any
}

func (k *keeper) Append(_ context.Context, recs []evidence.Sealed) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]string, len(recs))
	for i, r := range recs {
		var m map[string]any
		_ = json.Unmarshal(r.Record, &m)
		k.recs = append(k.recs, m)
		out[i] = "accepted"
	}
	return out, nil
}

func (k *keeper) phases() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	var out []string
	for _, r := range k.recs {
		out = append(out, r["phase"].(string))
	}
	return out
}

// archiveGrants: alice may set holds and approvals; bob may not.
type archiveGrants struct{}

func (archiveGrants) Verify(_ context.Context, req authz.VerifyRequest) (authz.VerifyAnswer, error) {
	var segs []authz.Segment
	for _, op := range []string{"sales.order.hold", "sales.order.approve_destruction"} {
		segs = append(segs, authz.Segment{ResourceType: "sales.archive", Operation: op, Atoms: map[string]authz.Atom{},
			Valid: authz.Window{From: "2026-01-01T00:00:00Z"}})
	}
	s := authz.Shard{Shard: "s", Applicability: []string{"sales.archive"}, Digest: "d",
		Subjects: map[string]authz.SubjectEntry{alice: {Segments: segs}}}
	return authz.VerifyAnswer{Nonce: req.Nonce, RecoveryGeneration: authz.Revision{Value: "1"},
		Shards: []authz.ShardAnswer{{Shard: "s", Status: "changed", View: &s}}}, nil
}

var archiveDefs = &authz.Definitions{Component: "test/sales", Resources: []authz.Resource{{Type: "sales.archive",
	ViewOperation: "sales.order.hold", Dimensions: map[string]authz.Dimension{}, Fields: map[string]authz.Field{}}},
	Operations: []authz.Operation{
		{Key: "sales.order.hold", ResourceType: "sales.archive", Purpose: "manage", RiskClass: "low"},
		{Key: "sales.order.approve_destruction", ResourceType: "sales.archive", Purpose: "manage", RiskClass: "low"}}}

type destroyWorld struct {
	env
	kept       *keeper
	alice, bob *authz.Subject
}

// destroyable: a released unit of two orders closed 400 days ago, on the warm route; the retention is a year.
func destroyable(t *testing.T, name string) (*destroyWorld, string) {
	t.Helper()
	w := &destroyWorld{env: setup(t, name), kept: &keeper{}}
	cl := w.cat.Classes["sales.orders"]
	cl.Retention.MinDays = 365
	w.cat.Classes["sales.orders"] = cl
	unit, _, _ := published(t, w.env, tiering.RoutePostgres)
	w.cat.Evidence = &evidence.Writer{DB: w.d, Store: w.kept, Component: "test/sales", Instance: "test-1",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	h := authz.NewHolder("test/sales", archiveGrants{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, h.Verify(w.ctx))
	require.NoError(t, archiveDefs.Validate())
	w.alice = &authz.Subject{Principal: alice, Defs: archiveDefs, Holder: h, At: time.Now()}
	w.bob = &authz.Subject{Principal: bob, Defs: archiveDefs, Holder: h, At: time.Now()}
	return w, unit
}

func (w *destroyWorld) state(t *testing.T, unit string) string {
	t.Helper()
	var s string
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT state FROM be_tier_unit WHERE id = $1", unit).Scan(&s))
	return s
}

func (w *destroyWorld) outbox(t *testing.T) []string {
	t.Helper()
	rows, err := w.admin.Query(w.ctx, "SELECT phase FROM be_evidence_outbox ORDER BY phase")
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		require.NoError(t, rows.Scan(&p))
		out = append(out, p)
	}
	return out
}

// P15.28, P15.29, TR13, TR24: a destruction waits for its retention, an approval (of the unit, or the class's
// standing one until withdrawn), no live hold on the unit, its class or one of its records, and a retention that is
// not pending; only the declared operations set holds and approvals; then the unit is destroyed with its tombstone,
// its evidence in two phases, its data gone, its members kept.
func TestTheDestructionConditions(t *testing.T) {
	w, unit := destroyable(t, "tier_destroy")
	cl := w.cat.Classes["sales.orders"]
	ten := cl
	ten.Retention.MinDays = 3650
	w.cat.Classes["sales.orders"] = ten
	why, err := w.cat.Destroy(w.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyNotDue}, why, "kept ten years")
	w.cat.Classes["sales.orders"] = cl

	why, err = w.cat.Destroy(w.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyApproval}, why)
	assert.Empty(t, w.kept.phases(), "no intent while the decision cannot hold")

	_, err = w.cat.Approve(w.ctx, w.bob, "sales.orders", tiering.Scope{Kind: tiering.ScopeClass, Value: "sales.orders"}, "x")
	assert.Error(t, err, "bob holds no approval operation")
	standing, err := w.cat.Approve(w.ctx, w.alice, "sales.orders", tiering.Scope{Kind: tiering.ScopeClass, Value: "sales.orders"},
		"the commercial-law retention has run")
	require.NoError(t, err)
	require.NoError(t, w.cat.Withdraw(w.ctx, w.alice, "sales.orders", standing))
	why, err = w.cat.Destroy(w.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyApproval}, why, "a withdrawn approval does not count")
	_, err = w.cat.Approve(w.ctx, w.alice, "sales.orders", tiering.Scope{Kind: tiering.ScopeUnit, Value: unit}, "reviewed")
	require.NoError(t, err)

	var root string
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT root_id FROM be_tier_member WHERE unit = $1 LIMIT 1", unit).Scan(&root))
	_, err = w.cat.Hold(w.ctx, w.bob, "sales.orders", tiering.RecordScope("sales.orders", root), "legal", "x")
	assert.Error(t, err, "bob holds no hold operation")
	hold, err := w.cat.Hold(w.ctx, w.alice, "sales.orders", tiering.RecordScope("sales.orders", root), "legal", "a court order")
	require.NoError(t, err)
	why, err = w.cat.Destroy(w.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyHeld}, why, "one of its orders is held (TR13)")
	for _, s := range []tiering.Scope{{Kind: tiering.ScopeUnit, Value: unit}, {Kind: tiering.ScopeClass, Value: "sales.orders"}} {
		other, err := w.cat.Hold(w.ctx, w.alice, "sales.orders", s, "dispute", "a dispute")
		require.NoError(t, err)
		require.NoError(t, w.cat.LiftHold(w.ctx, w.alice, "sales.orders", hold))
		why, err = w.cat.Destroy(w.ctx, unit)
		require.NoError(t, err)
		assert.Equal(t, []string{tiering.WhyHeld}, why, "held by its %s", s.Kind)
		hold = other
	}
	require.NoError(t, w.cat.LiftHold(w.ctx, w.alice, "sales.orders", hold))
	assert.True(t, errs.Is(w.cat.LiftHold(w.ctx, w.alice, "sales.orders", hold), "be", errs.ReasonNotFound), "lifted once")

	pending := cl
	pending.Retention = tiering.Retention{Pending: true}
	w.cat.Classes["sales.orders"] = pending
	why, err = w.cat.Destroy(w.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyPending}, why, "no basis: never destroyed (TR24)")
	w.cat.Classes["sales.orders"] = cl

	before := count(t, w.env, "SELECT version FROM be_tier_catalog")
	why, err = w.cat.Destroy(w.ctx, unit)
	require.NoError(t, err)
	assert.Empty(t, why)
	assert.Equal(t, tiering.StateDestroyed, w.state(t, unit))
	assert.Equal(t, before+1, count(t, w.env, "SELECT version FROM be_tier_catalog"))
	var tomb map[string]any
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT tombstone FROM be_tier_unit WHERE id = $1", unit).Scan(&tomb))
	assert.Equal(t, unit, tomb["unit"])
	assert.Equal(t, "test", tomb["basis"])
	assert.NotEmpty(t, tomb["digest"])
	assert.Zero(t, count(t, w.env, "SELECT count(*) FROM be_tier_row WHERE unit = $1", unit), "the warm rows are gone")
	assert.Equal(t, 2, count(t, w.env, "SELECT count(*) FROM be_tier_member WHERE unit = $1", unit), "the members stay")
	assert.Equal(t, []string{"intent"}, w.kept.phases(), "the intent before the decision")
	assert.Equal(t, []string{"effective"}, w.outbox(t), "the conclusion in it")
	_, found, err := w.cat.Get(w.ctx, "sales.orders", root)
	require.NoError(t, err)
	assert.False(t, found, "read by key: not found")
}

// TR14, P15.29: a hold set while the destruction decides (its transaction open before the decision's lock) wins: the
// decision waits for it, sees it, and cancels its intent.
func TestAHoldSetDuringTheDecisionWins(t *testing.T) {
	w, unit := destroyable(t, "tier_destroy_race")
	_, err := w.cat.Approve(w.ctx, w.alice, "sales.orders", tiering.Scope{Kind: tiering.ScopeUnit, Value: unit}, "reviewed")
	require.NoError(t, err)
	tx, err := w.admin.Begin(w.ctx)
	require.NoError(t, err)
	_, err = tx.Exec(w.ctx, "INSERT INTO be_tier_hold (id, scope_kind, scope, source, authorized_by, reason) VALUES "+
		"(gen_random_uuid(), 'unit', $1, 'legal', $2, 'a court order')", unit, alice)
	require.NoError(t, err)
	done := make(chan []string, 1)
	go func() {
		why, err := w.cat.Destroy(w.ctx, unit)
		assert.NoError(t, err)
		done <- why
	}()
	select {
	case <-done:
		t.Fatal("the decision did not wait for the hold being set")
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(w.ctx))
	assert.Equal(t, []string{tiering.WhyHeld}, <-done)
	assert.Equal(t, tiering.StateReleased, w.state(t, unit))
	assert.Equal(t, []string{"intent"}, w.kept.phases())
	assert.Equal(t, []string{"cancelled"}, w.outbox(t), "the intent is cancelled")
}

// TR16, RC08, P15.30: a destruction after the recovery's point is done again from its evidence: the unit, back in
// the restored database with its rows, turns destroyed without the preconditions (no approval any more), its data goes
// with the next destruction run, and a second replay finds it reflected.
func TestADestructionAfterTheRecoveryPointIsReplayed(t *testing.T) {
	w, unit := destroyable(t, "tier_destroy_replay")
	_, err := w.cat.Approve(w.ctx, w.alice, "sales.orders", tiering.Scope{Kind: tiering.ScopeUnit, Value: unit}, "reviewed")
	require.NoError(t, err)
	why, err := w.cat.Destroy(w.ctx, unit)
	require.NoError(t, err)
	require.Empty(t, why)
	var effective []byte
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT record FROM be_evidence_outbox WHERE phase = 'effective'").Scan(&effective))

	// the database goes back to before the destruction: the unit released, its rows there, no approval, no history
	for _, sql := range []string{
		"UPDATE be_tier_unit SET state = 'released', destroyed_at = NULL, tombstone = NULL",
		"INSERT INTO be_tier_row VALUES ('" + unit + "', 1, 'orders', 'restored', '\\x00')",
		"DELETE FROM be_tier_approval", "DELETE FROM be_change_history", "DELETE FROM be_evidence_outbox"} {
		_, err := w.admin.Exec(w.ctx, sql)
		require.NoError(t, err)
	}
	var replays evidence.Replays
	w.cat.Replays(&replays)
	require.NoError(t, replays.Err())
	server := &evidence.ReplayServer{DB: w.d, Component: "test/sales", Replays: &replays}
	resp, err := server.Replay(w.ctx, &evidencepb.ReplayRequest{Record: effective})
	require.NoError(t, err)
	assert.Equal(t, evidence.Applied, resp.GetOutcome(), resp.GetDetail())
	assert.Equal(t, tiering.StateDestroyed, w.state(t, unit))

	why, err = w.cat.Destroy(w.ctx, unit) // the next destruction run deletes its data again
	require.NoError(t, err)
	assert.Empty(t, why)
	assert.Zero(t, count(t, w.env, "SELECT count(*) FROM be_tier_row WHERE unit = $1", unit), "the restored rows are gone")
	resp, err = server.Replay(w.ctx, &evidencepb.ReplayRequest{Record: effective})
	require.NoError(t, err)
	assert.Equal(t, evidence.AlreadyReflected, resp.GetOutcome())
}
