//go:build integration

package tiering_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// P15.19, P15.39 (found by the long simulation, T11a): a release reads the latest backup before its transaction. The
// source is the run profile's object store in a deployment; here it reads the database, which inside a transaction
// is a nested one, refused (NESTED_TX).
func TestAReleaseReadsTheBackupOutsideItsTransaction(t *testing.T) {
	e := setup(t, "tier_backup_outside")
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	e.cat.Store = store
	withRoute(e, tiering.RouteObject)
	unit := sealedUnit(t, e)
	_, err := e.cat.Transfer(e.ctx, unit)
	require.NoError(t, err)
	e.cat.Grace = 0
	e.cat.Backups = func(ctx context.Context) (time.Time, bool, error) {
		var now time.Time
		err := e.d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT now() + interval '1 hour'").Scan(&now)
		})
		return now, err == nil, err
	}
	why, err := e.cat.Release(e.ctx, unit)
	require.NoError(t, err)
	assert.Empty(t, why)
	_, err = e.cat.Reroute(e.ctx, unit, tiering.RoutePostgres)
	require.NoError(t, err)
	why, err = e.cat.ReleaseOld(e.ctx, unit)
	require.NoError(t, err, "the old version's release reads it outside its transaction too")
	assert.Empty(t, why)
}

// P15.31 (found by the long simulation, T11a): an erasure whose unit another writer claims while it rewrites it (a
// route change, a transfer, another erasure) starts again from the unit's state now, rather than failing with a
// stale claim.
func TestAnErasureStartsAgainWhenTheUnitMovesOn(t *testing.T) {
	w, unit, orders, _ := erasable(t, "tier_erase_moved")
	withRoute(w.env, tiering.RoutePostgres)
	_, err := w.cat.Transfer(w.ctx, unit)
	require.NoError(t, err)
	won := false
	tiering.SetCrash(func(point string) {
		if point == "claim/done" && !won { // the erasure has its claim: another writer takes the unit
			won = true
			_, err := w.cat.ClaimAgain(context.Background(), unit)
			require.NoError(t, err)
		}
	})
	defer tiering.SetCrash(func(string) {})
	out, err := w.cat.Erase(w.ctx, "sales.orders", orders[0], "a data subject's request")
	require.NoError(t, err)
	require.True(t, won, "another writer won the unit during the erasure")
	assert.Equal(t, tiering.Erased, out)
	a, found, err := w.cat.Get(w.ctx, "sales.orders", orders[0])
	require.NoError(t, err)
	require.True(t, found)
	assert.NotContains(t, a.Root, "note", "erased after starting again")
}

// P15.22, P15.24, P15.29 (found by the long simulation, T11a): from the destruction's decision on, its records are
// not found by key and an online query under a snapshot leaves them out, while their online rows are still being
// deleted (a read in between found a document with its grandchildren gone and its children still there).
func TestADestroyedRecordIsGoneFromTheDecisionOn(t *testing.T) {
	w := &destroyWorld{env: setup(t, "tier_destroy_window"), kept: &keeper{}}
	cl := w.cat.Classes["sales.orders"]
	cl.Retention.MinDays = 365
	w.cat.Classes["sales.orders"] = cl
	withRoute(w.env, tiering.RoutePostgres)
	unit := sealedUnit(t, w.env) // sealed: its online rows are all there
	w.cat.Evidence = &evidence.Writer{DB: w.d, Store: w.kept, Component: "test/sales", Instance: "test-1",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	w.alice = holder(t)
	_, err := w.cat.Approve(w.ctx, w.alice, "sales.orders", tiering.Scope{Kind: tiering.ScopeUnit, Value: unit}, "reviewed")
	require.NoError(t, err)
	var root string
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT root_id FROM be_tier_member WHERE unit = $1 LIMIT 1", unit).Scan(&root))
	checked := false
	tiering.SetCrash(func(point string) {
		if point != "destroy.decide/done" || checked {
			return
		}
		checked = true
		assert.Equal(t, 1, count(t, w.env, "SELECT count(*) FROM orders WHERE id = $1", root), "its rows not deleted yet")
		_, found, err := w.cat.Get(w.ctx, "sales.orders", root)
		assert.True(t, errs.Is(err, "be", errs.ReasonNotFound), "by key: NOT_FOUND from the decision on: %v", err)
		assert.False(t, found)
		s, err := w.cat.Snapshot(w.ctx, "a list")
		require.NoError(t, err)
		assert.Zero(t, count(t, w.env, "SELECT count(*) FROM orders r WHERE r.id = $1 AND "+s.OnlineExclusion(cl, "r"), root),
			"an online list leaves it out")
		require.NoError(t, w.cat.End(w.ctx, s))
	})
	defer tiering.SetCrash(func(string) {})
	why, err := w.cat.Destroy(w.ctx, unit)
	require.NoError(t, err)
	require.Empty(t, why)
	require.True(t, checked, "the decision was taken")
	assert.Zero(t, count(t, w.env, "SELECT count(*) FROM orders WHERE id = $1", root), "then its rows go")
}
