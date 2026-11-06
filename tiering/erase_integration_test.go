//go:build integration

package tiering_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/evidencepb"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// erasable: the orders' note is personal data; two orders with notes sealed in one unit (not transferred).
func erasable(t *testing.T, name string) (*destroyWorld, string, [2]string, *bytes.Buffer) {
	t.Helper()
	w := &destroyWorld{env: setup(t, name), kept: &keeper{}}
	cl := w.cat.Classes["sales.orders"]
	cl.Erasable = []string{"note"}
	w.cat.Classes["sales.orders"] = cl
	var orders [2]string
	for i := range orders {
		orders[i] = w.order(t, 400)
		_, err := w.admin.Exec(w.ctx, "UPDATE orders SET note = $2 WHERE id = $1", orders[i], "call me at +34 600 000 00"+string(rune('0'+i)))
		require.NoError(t, err)
	}
	groups, err := w.cat.Groupings(w.ctx, "sales.orders")
	require.NoError(t, err)
	res, err := w.cat.Seal(w.ctx, "sales.orders", groups[0])
	require.NoError(t, err)
	var logs bytes.Buffer
	w.cat.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	w.cat.Evidence = &evidence.Writer{DB: w.d, Store: w.kept, Component: "test/sales", Instance: "test-1",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return w, res.Unit, orders, &logs
}

func (w *destroyWorld) unitAt(t *testing.T, unit string) (state, digest string, version int) {
	t.Helper()
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT state, digest, manifest_version FROM be_tier_unit WHERE id = $1", unit).
		Scan(&state, &digest, &version))
	return
}

// TR15, P15.31: an order in a published or released unit is erased as a new manifest version on the unit's route,
// its state unchanged: read by key its note is gone, the other order's is kept, a published unit's online row is
// erased too, the thawed copy dropped; evidence in two phases. An order under a live hold is deferred with an alert.
func TestAnArchivedOrderIsErasedAsANewVersion(t *testing.T) {
	for _, route := range []string{tiering.RoutePostgres, tiering.RouteObject} {
		t.Run(route, func(t *testing.T) {
			w, unit, orders, logs := erasable(t, "tier_erase_"+strings.ReplaceAll(route, "-", "_"))
			store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
			ensureBucket(t, admin, tierBucket, false)
			w.cat.Store = store
			withRoute(w.env, route)
			_, err := w.cat.Transfer(w.ctx, unit)
			require.NoError(t, err)
			released := route == tiering.RouteObject
			if released {
				w.cat.Grace, w.cat.Backups = 0, backupAt(time.Now().Add(time.Minute))
				why, err := w.cat.Release(w.ctx, unit)
				require.NoError(t, err)
				require.Empty(t, why)
			}
			require.NoError(t, w.cat.Thaw(w.ctx, unit, time.Hour))
			state, before, version := w.unitAt(t, unit)

			outcome, err := w.cat.Erase(w.ctx, "sales.orders", orders[0], "request ER-1")
			require.NoError(t, err)
			assert.Equal(t, tiering.Erased, outcome)
			nowState, after, nowVersion := w.unitAt(t, unit)
			assert.Equal(t, state, nowState, "the state is unchanged")
			assert.Equal(t, version+1, nowVersion, "a new manifest version")
			assert.NotEqual(t, before, after, "the content's digest is the erased one")
			erased, found, err := w.cat.Get(w.ctx, "sales.orders", orders[0])
			require.NoError(t, err)
			require.True(t, found)
			assert.NotContains(t, erased.Root, "note")
			assert.Equal(t, route, erased.Route)
			kept, _, err := w.cat.Get(w.ctx, "sales.orders", orders[1])
			require.NoError(t, err)
			assert.Equal(t, "call me at +34 600 000 001", kept.Root["note"])
			if !released {
				assert.Equal(t, 1, count(t, w.env, "SELECT count(*) FROM orders WHERE id = $1 AND note IS NULL", orders[0]),
					"a published unit's online row is erased too")
			}
			assert.Zero(t, count(t, w.env, "SELECT count(*) FROM be_tier_thaw"), "the thawed copy is dropped")
			assert.Equal(t, []string{"intent"}, w.kept.phases())
			assert.Equal(t, []string{"effective"}, w.outbox(t))

			w.alice = holder(t)
			_, err = w.cat.Hold(w.ctx, w.alice, "sales.orders", tiering.RecordScope("sales.orders", orders[1]), "legal", "a court order")
			require.NoError(t, err)
			outcome, err = w.cat.Erase(w.ctx, "sales.orders", orders[1], "request ER-2")
			require.NoError(t, err)
			assert.Equal(t, tiering.ErasureDeferred, outcome, "TR15")
			assert.Contains(t, logs.String(), "tiering_erasure_deferred")
			kept, _, err = w.cat.Get(w.ctx, "sales.orders", orders[1])
			require.NoError(t, err)
			assert.Equal(t, "call me at +34 600 000 001", kept.Root["note"], "kept while held")
			assert.Equal(t, []string{"intent"}, w.kept.phases(), "no intent for a deferred erasure")
		})
	}
}

// TR27, P15.31: an erasure in a sealed unit not yet published erases the online rows and computes the unit's digest
// again; a transfer copied before it fails to publish, and the next transfer publishes the erased content.
func TestAnErasureInASealedUnitRestartsItsTransfer(t *testing.T) {
	w, unit, orders, _ := erasable(t, "tier_erase_sealed")
	withRoute(w.env, tiering.RoutePostgres)
	claim, err := w.cat.ClaimUnit(w.ctx, unit)
	require.NoError(t, err)
	m, err := w.cat.Copy(w.ctx, claim, tiering.RoutePostgres)
	require.NoError(t, err)
	_, before, _ := w.unitAt(t, unit)

	outcome, err := w.cat.Erase(w.ctx, "sales.orders", orders[0], "request ER-3")
	require.NoError(t, err)
	assert.Equal(t, tiering.Erased, outcome)
	state, after, _ := w.unitAt(t, unit)
	assert.Equal(t, tiering.StateSealed, state)
	assert.NotEqual(t, before, after)
	assert.Equal(t, 1, count(t, w.env, "SELECT count(*) FROM orders WHERE id = $1 AND note IS NULL", orders[0]))
	assert.ErrorIs(t, w.cat.Publish(w.ctx, claim, m), tiering.ErrStaleClaim, "the copy from before the erasure")

	_, err = w.cat.Transfer(w.ctx, unit)
	require.NoError(t, err, "the transfer starts again")
	erased, found, err := w.cat.Get(w.ctx, "sales.orders", orders[0])
	require.NoError(t, err)
	require.True(t, found)
	assert.NotContains(t, erased.Root, "note")
}

// P15.30: an erasure after the recovery's point is done again: the restored database has the unit at its old version
// with the note; the replay queues the erase job, which writes the erased version again; a second replay is reflected.
func TestAnErasureAfterTheRecoveryPointIsReplayed(t *testing.T) {
	w, unit, orders, _ := erasable(t, "tier_erase_replay")
	withRoute(w.env, tiering.RoutePostgres)
	_, err := w.cat.Transfer(w.ctx, unit)
	require.NoError(t, err)
	_, old, _ := w.unitAt(t, unit)
	_, err = w.cat.Erase(w.ctx, "sales.orders", orders[0], "request ER-4")
	require.NoError(t, err)
	var effective []byte
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT record FROM be_evidence_outbox WHERE phase = 'effective'").Scan(&effective))

	// back to before the erasure: version 1 with its digest, the note online, no history
	for _, sql := range []string{
		"UPDATE be_tier_unit SET manifest_version = 1, digest = '" + old + "'", "DELETE FROM be_tier_manifest WHERE version = 2",
		"DELETE FROM be_change_history", "DELETE FROM be_evidence_outbox"} {
		_, err := w.admin.Exec(w.ctx, sql)
		require.NoError(t, err)
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := work.NewRunner(w.d, "test/sales", quiet)
	require.NoError(t, runner.Add(w.cat.EraseJobOf(quiet)))
	w.cat.Enqueue = runner.Enqueue
	var replays evidence.Replays
	w.cat.Replays(&replays)
	server := &evidence.ReplayServer{DB: w.d, Component: "test/sales", Replays: &replays}
	resp, err := server.Replay(w.ctx, &evidencepb.ReplayRequest{Record: effective})
	require.NoError(t, err)
	assert.Equal(t, evidence.Applied, resp.GetOutcome(), resp.GetDetail())

	ctx, cancel := context.WithCancel(w.ctx)
	require.NoError(t, runner.Start(ctx))
	defer func() { cancel(); runner.Stop() }()
	require.Eventually(t, func() bool { _, _, v := w.unitAt(t, unit); return v == 2 }, 20*time.Second, 100*time.Millisecond)
	erased, _, err := w.cat.Get(w.ctx, "sales.orders", orders[0])
	require.NoError(t, err)
	assert.NotContains(t, erased.Root, "note", "erased again")
	resp, err = server.Replay(w.ctx, &evidencepb.ReplayRequest{Record: effective})
	require.NoError(t, err)
	assert.Equal(t, evidence.AlreadyReflected, resp.GetOutcome())
}

// TR15, P15.31: a hold set while the erasure is under way (open before its publish locks the holds) wins: the publish
// waits for it, sees it, writes nothing, cancels the intent and defers the erasure.
func TestAHoldSetDuringAnErasureWins(t *testing.T) {
	w, unit, orders, _ := erasable(t, "tier_erase_race")
	withRoute(w.env, tiering.RoutePostgres)
	_, err := w.cat.Transfer(w.ctx, unit)
	require.NoError(t, err)
	_, _, version := w.unitAt(t, unit)
	tx, err := w.admin.Begin(w.ctx)
	require.NoError(t, err)
	_, err = tx.Exec(w.ctx, "INSERT INTO be_tier_hold (id, scope_kind, scope, source, authorized_by, reason) VALUES "+
		"(gen_random_uuid(), 'record', $1, 'legal', $2, 'a court order')", "sales.orders/"+orders[0], alice)
	require.NoError(t, err)
	done := make(chan string, 1)
	go func() {
		outcome, err := w.cat.Erase(w.ctx, "sales.orders", orders[0], "request ER-5")
		assert.NoError(t, err)
		done <- outcome
	}()
	select {
	case <-done:
		t.Fatal("the erasure did not wait for the hold being set")
	case <-time.After(500 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(w.ctx))
	assert.Equal(t, tiering.ErasureDeferred, <-done)
	_, _, now := w.unitAt(t, unit)
	assert.Equal(t, version, now, "no new version")
	assert.Equal(t, []string{"cancelled"}, w.outbox(t), "the intent is cancelled")
}
