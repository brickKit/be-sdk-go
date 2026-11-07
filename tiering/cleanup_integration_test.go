//go:build integration

package tiering_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// P15.20, P15.27: the daily cleanup removes what an interrupted transfer left of every unit of the class at once (the
// objects no manifest names and the warm rows of no manifest version), once past the window; the published copies,
// the rows of the version a transfer may be writing and a live thaw stay. Until alpha.116 nothing ran the cleanup.
func TestTheDailyCleanupRemovesEveryUnitsOrphans(t *testing.T) {
	e := setup(t, "tier_cleanup")
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	e.cat.Store = store
	withRoute(e, tiering.RouteObject)

	// two units on the object store, each with the copy of a worker whose claim was taken over
	var olds []tiering.Manifest
	var currents []tiering.Manifest
	for i := 0; i < 2; i++ {
		ei := e
		ei.le = ids.New().String() // another legal entity: another grouping
		unit := sealedUnit(t, ei)
		old, err := e.cat.ClaimUnit(e.ctx, unit)
		require.NoError(t, err)
		oldCopy, err := e.cat.Copy(e.ctx, old, tiering.RouteObject)
		require.NoError(t, err)
		current, err := e.cat.ClaimUnit(e.ctx, unit)
		require.NoError(t, err)
		m, err := e.cat.Copy(e.ctx, current, tiering.RouteObject)
		require.NoError(t, err)
		require.NoError(t, e.cat.Verify(e.ctx, m))
		require.NoError(t, e.cat.Publish(e.ctx, current, m))
		olds, currents = append(olds, oldCopy), append(currents, m)
	}
	// a third on the warm route: an orphan row of a version no manifest names, and a row of the next version (a
	// transfer writing it now)
	withRoute(e, tiering.RoutePostgres)
	e3 := e
	e3.le = ids.New().String()
	warm := sealedUnit(t, e3)
	_, err := e.cat.Transfer(e.ctx, warm)
	require.NoError(t, err)
	pgtest.Exec(t, e.admin, "INSERT INTO be_tier_row VALUES ('"+warm+"', 7, 'orders', 'orphan', '\\x00')",
		"INSERT INTO be_tier_row VALUES ('"+warm+"', 2, 'orders', 'writing', '\\x00')")

	removed, err := e.cat.CleanOrphans(e.ctx, olds[0].Unit, "sales.orders", time.Hour)
	require.NoError(t, err)
	assert.Zero(t, removed, "younger than the window: not yet")

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	e.cat.OrphanAge = time.Nanosecond
	runner := work.NewRunner(e.d, "test/sales", quiet)
	require.NoError(t, runner.Add(e.cat.CleanupJob(quiet)))
	ctx, cancel := context.WithCancel(e.ctx)
	require.NoError(t, runner.Start(ctx))
	require.Eventually(t, func() bool {
		return count(t, e, "SELECT count(*) FROM be_tier_row WHERE pk = 'orphan'") == 0
	}, 20*time.Second, 100*time.Millisecond, "the daily job ran")
	cancel()
	runner.Stop()

	for i, old := range olds {
		for _, o := range old.Tables[0].Objects {
			_, err := store.Get(e.ctx, o.Key)
			assert.ErrorIs(t, err, tiering.ErrMissing, "unit %d: the old worker's part", i)
		}
		require.NoError(t, e.cat.Verify(e.ctx, currents[i]), "unit %d: the published copy stays", i)
	}
	assert.Equal(t, 1, count(t, e, "SELECT count(*) FROM be_tier_row WHERE pk = 'writing'"), "the next version stays")
	assert.Positive(t, count(t, e, "SELECT count(*) FROM be_tier_row WHERE unit = $1 AND version = 1", warm),
		"the published warm rows stay")
}
