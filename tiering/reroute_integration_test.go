//go:build integration

package tiering_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (w *destroyWorld) objectsUnder(t *testing.T, store *tiering.Store, unit string) int {
	t.Helper()
	objects, err := store.List(w.ctx, fmt.Sprintf("%s/sales.orders/%s/", store.Prefix, unit))
	require.NoError(t, err)
	return len(objects)
}

// TR20, TR26, P15.35, P15.39: a released unit on the warm route moves to the object store when its class's first route
// changes (the transfer job): released still, a new manifest version on the new route, read back as before; the old
// version's warm rows go only past the grace, the leases from before the change and a backup after it.
func TestARouteChangeMovesAReleasedUnit(t *testing.T) {
	w := &destroyWorld{env: setup(t, "tier_reroute"), kept: &keeper{}}
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	w.cat.Store = store
	unit, roots, lines := published(t, w.env, tiering.RoutePostgres)
	_, _, version := w.unitAt(t, unit)
	var routeVersion int64
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT route_version FROM be_tier_unit WHERE id = $1", unit).Scan(&routeVersion))

	withRoute(w.env, tiering.RouteObject)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := work.NewRunner(w.d, "test/sales", quiet)
	require.NoError(t, runner.Add(w.cat.TransferJob(quiet)))
	ctx, cancel := context.WithCancel(w.ctx)
	require.NoError(t, runner.Start(ctx))
	require.Eventually(t, func() bool {
		var route string
		require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT route FROM be_tier_unit WHERE id = $1", unit).Scan(&route))
		return route == tiering.RouteObject
	}, 20*time.Second, 100*time.Millisecond, "the transfer job moves it")
	cancel()
	runner.Stop()

	state, _, now := w.unitAt(t, unit)
	assert.Equal(t, tiering.StateReleased, state, "released still (TR26)")
	assert.Equal(t, version+1, now)
	var moved int64
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT route_version FROM be_tier_unit WHERE id = $1", unit).Scan(&moved))
	assert.Greater(t, moved, routeVersion, "the route version moves with the route")
	for id := range roots {
		agg, found, err := w.cat.Get(w.ctx, "sales.orders", id)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, tiering.RouteObject, agg.Route)
		assert.Equal(t, roots[id], agg.Root, "read back as it was online")
		assert.Equal(t, sortLines(lines[id]), sortLines(agg.Children["order_lines"]))
	}
	assert.Positive(t, w.objectsUnder(t, store, unit))
	old := count(t, w.env, "SELECT count(*) FROM be_tier_row WHERE unit = $1 AND version = $2", unit, version)
	require.Equal(t, 6, old, "the old route's rows are still there")

	w.cat.Grace = 7 * 24 * time.Hour
	why, err := w.cat.ReleaseOld(w.ctx, unit)
	require.NoError(t, err)
	assert.Contains(t, why, tiering.WhyGrace)
	w.cat.Grace = 0
	pgtest.Exec(t, w.admin, fmt.Sprintf("INSERT INTO be_tier_lease VALUES ('%s', %d, 'export', now() + interval '15 minutes')",
		ids.New(), moved-1))
	why, err = w.cat.ReleaseOld(w.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyLease}, why, "an export reading the old route")
	pgtest.Exec(t, w.admin, "UPDATE be_tier_lease SET expires_at = now() - interval '1 second'")
	w.cat.Backups = backupAt(time.Now().Add(-time.Hour))
	why, err = w.cat.ReleaseOld(w.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyNoBackup}, why, "a restore before the change still needs the old rows")
	w.cat.Backups = backupAt(time.Now().Add(time.Minute))
	why, err = w.cat.ReleaseOld(w.ctx, unit)
	require.NoError(t, err)
	assert.Empty(t, why)
	assert.Zero(t, count(t, w.env, "SELECT count(*) FROM be_tier_row WHERE unit = $1 AND version = $2", unit, version))
	assert.Equal(t, 1, count(t, w.env, "SELECT count(*) FROM be_tier_manifest WHERE unit = $1 AND superseded_at IS NOT NULL", unit))
	why, err = w.cat.ReleaseOld(w.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyNothingOld}, why)
	for id, root := range roots {
		agg, _, err := w.cat.Get(w.ctx, "sales.orders", id)
		require.NoError(t, err)
		assert.Equal(t, root, agg.Root, "the new route answers alone")
	}
}

// P15.31, P15.39: an erasure on the object route leaves the old version's objects (with the erased data) until they
// are released; then only the new version's objects are left.
func TestAnErasedVersionsOldObjectsAreReleased(t *testing.T) {
	w, unit, orders, _ := erasable(t, "tier_erase_old")
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	w.cat.Store = store
	withRoute(w.env, tiering.RouteObject)
	_, err := w.cat.Transfer(w.ctx, unit)
	require.NoError(t, err)
	first := w.objectsUnder(t, store, unit)
	_, err = w.cat.Erase(w.ctx, "sales.orders", orders[0], "request ER-6")
	require.NoError(t, err)
	require.Equal(t, 2*first, w.objectsUnder(t, store, unit), "both versions")
	w.cat.Grace, w.cat.Backups = 0, backupAt(time.Now().Add(time.Minute))
	why, err := w.cat.ReleaseOld(w.ctx, unit)
	require.NoError(t, err)
	require.Empty(t, why)
	assert.Equal(t, first, w.objectsUnder(t, store, unit), "the old version's objects are gone")
	erased, _, err := w.cat.Get(w.ctx, "sales.orders", orders[0])
	require.NoError(t, err)
	assert.NotContains(t, erased.Root, "note")
}

// TR19, P15.33, P15.34: the class's structure moves on (note renamed remark, version 2); a unit written in version 1
// is read through the converter registered for it, the result checked; without one, QUERY_NOT_COMPLETABLE and the
// readability check names the version; a route change writes it in the current structure.
func TestAnOlderStructureIsReadThroughItsConverter(t *testing.T) {
	for _, route := range []string{tiering.RoutePostgres, tiering.RouteObject} {
		t.Run(route, func(t *testing.T) { olderStructure(t, route) })
	}
}

func olderStructure(t *testing.T, route string) {
	w, unit, orders, _ := erasable(t, "tier_schema_"+strings.ReplaceAll(route, "-", "_"))
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	w.cat.Store = store
	withRoute(w.env, route)
	_, err := w.cat.Transfer(w.ctx, unit)
	require.NoError(t, err)

	cl := w.cat.Classes["sales.orders"]
	cl.SchemaVersion, cl.Erasable = "2", nil
	cl.Root.Columns = map[string]string{}
	for k, v := range class().Root.Columns {
		cl.Root.Columns[k] = v
	}
	delete(cl.Root.Columns, "note")
	cl.Root.Columns["remark"] = tiering.TypeText
	w.cat.Classes["sales.orders"] = cl

	_, _, err = w.cat.Get(w.ctx, "sales.orders", orders[0])
	assert.True(t, errs.Is(err, "be", errs.ReasonQueryNotCompletable), "no converter: %v", err)
	failed, err := w.cat.CheckReadable(w.ctx, "sales.orders")
	require.NoError(t, err)
	assert.Contains(t, failed, "1")

	same := &tiering.Catalog{DB: w.d, Classes: w.cat.Classes, Registry: tiering.NewRegistry(), Store: store}
	require.NoError(t, same.Convert("sales.orders", "1", func(_ string, row map[string]any) (map[string]any, error) {
		return row, nil
	}))
	_, _, err = same.Get(w.ctx, "sales.orders", orders[0])
	assert.ErrorContains(t, err, `"note"`, "a converted row with a column the class no longer has is refused")
	bad := &tiering.Catalog{DB: w.d, Classes: w.cat.Classes, Registry: tiering.NewRegistry(), Store: store}
	require.NoError(t, bad.Convert("sales.orders", "1", func(table string, row map[string]any) (map[string]any, error) {
		if table == "orders" { // renamed as it should be, but the locate column lost
			row["remark"] = row["note"]
			delete(row, "note")
			delete(row, "legal_entity")
		}
		return row, nil
	}))
	_, _, err = bad.Get(w.ctx, "sales.orders", orders[0])
	assert.ErrorContains(t, err, "legal_entity", "a converted root without its locate column is refused")

	require.NoError(t, w.cat.Convert("sales.orders", "1", func(table string, row map[string]any) (map[string]any, error) {
		if table == "orders" {
			if v, ok := row["note"]; ok {
				row["remark"] = v
				delete(row, "note")
			}
		}
		return row, nil
	}))
	agg, found, err := w.cat.Get(w.ctx, "sales.orders", orders[0])
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "call me at +34 600 000 000", agg.Root["remark"])
	assert.NotContains(t, agg.Root, "note")
	failed, err = w.cat.CheckReadable(w.ctx, "sales.orders")
	require.NoError(t, err)
	assert.Empty(t, failed)

	_, err = w.cat.Reroute(w.ctx, unit, route) // written again: in the current structure
	require.NoError(t, err)
	var schema string
	require.NoError(t, w.admin.QueryRow(w.ctx, "SELECT convert_from(m.manifest, 'UTF8')::jsonb ->> 'schema_version' FROM "+
		"be_tier_manifest m JOIN be_tier_unit u ON u.id = m.unit AND m.version = u.manifest_version WHERE u.id = $1", unit).Scan(&schema))
	assert.Equal(t, "2", schema)
	agg, _, err = w.cat.Get(w.ctx, "sales.orders", orders[0])
	require.NoError(t, err)
	assert.Equal(t, "call me at +34 600 000 000", agg.Root["remark"])
}
