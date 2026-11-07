//go:build integration

package tiering_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// G4 (assembly protocol 11's replacement checks: every read implementation answers the same rows, before the seal,
// after it and after the read-back): the same history read answers the same orders, field for field, online, sealed,
// published, released, moved to the other route and with the old version released; the archived orders read by their
// key are the same from either route.
func TestTheSameReadAnswersTheSameAtEveryStep(t *testing.T) {
	e := setup(t, "tier_same_read")
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	e.cat.Store = store
	withRoute(e, tiering.RouteObject)
	h := authz.NewHolder("test/sales", &grants{entities: []string{e.le}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, h.Verify(e.ctx))
	require.NoError(t, orderDefs.Validate())
	sub := &authz.Subject{Principal: alice, Defs: orderDefs, Holder: h, At: time.Now()}
	var slow time.Duration
	require.NoError(t, e.cat.Register("sales.orders", orderReader(&slow)))
	e.order(t, -1)
	archived := []string{e.order(t, 400), e.order(t, 400)}
	e.order(t, 150) // closed, not yet past the residence

	read := func(step string) []string {
		t.Helper()
		out, pending, err := e.cat.History(e.ctx, sub, "sales.orders", nil)
		require.NoError(t, err, step)
		require.Nil(t, pending, step)
		var rows []string
		for _, o := range out {
			b, err := json.Marshal(o)
			require.NoError(t, err)
			rows = append(rows, string(b))
		}
		sort.Strings(rows)
		return rows
	}
	byKey := func(route string) map[string]tiering.Aggregate {
		t.Helper()
		out := map[string]tiering.Aggregate{}
		for _, id := range archived {
			a, found, err := e.cat.Get(e.ctx, "sales.orders", id)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, route, a.Route)
			a.Unit, a.Route = "", ""
			a.Children["order_lines"] = sortLines(a.Children["order_lines"])
			out[id] = a
		}
		return out
	}

	want := read("online")
	require.Len(t, want, 4)
	groups, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	sealed, err := e.cat.Seal(e.ctx, "sales.orders", groups[0])
	require.NoError(t, err)
	require.Equal(t, 2, sealed.Members)
	assert.Equal(t, want, read("sealed"))
	_, err = e.cat.Transfer(e.ctx, sealed.Unit)
	require.NoError(t, err)
	assert.Equal(t, want, read("published"))
	fromObjects := byKey(tiering.RouteObject)

	e.cat.Grace, e.cat.Backups = 0, backupAt(time.Now().Add(time.Hour))
	why, err := e.cat.Release(e.ctx, sealed.Unit)
	require.NoError(t, err)
	require.Empty(t, why)
	assert.Equal(t, want, read("released"))

	_, err = e.cat.Reroute(e.ctx, sealed.Unit, tiering.RoutePostgres)
	require.NoError(t, err)
	assert.Equal(t, want, read("moved to the warm route"))
	assert.Equal(t, fromObjects, byKey(tiering.RoutePostgres), "the same orders by key from either route")
	why, err = e.cat.ReleaseOld(e.ctx, sealed.Unit)
	require.NoError(t, err)
	require.Empty(t, why)
	assert.Equal(t, want, read("the old version released"))
	assert.Equal(t, fromObjects, byKey(tiering.RoutePostgres))
}
