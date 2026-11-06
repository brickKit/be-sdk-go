//go:build integration

package tiering_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// onlineCanonical reads one order and its lines online as canonical rows (the reference every route must match).
func onlineCanonical(t *testing.T, e env, id string) (map[string]any, []map[string]any) {
	t.Helper()
	c := class()
	read := func(tb tiering.Table, where string) []map[string]any {
		names := make([]string, 0, len(tb.Columns))
		exprs := []string{}
		for n := range tb.Columns {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			ex := "t." + n
			if tb.Columns[n] == tiering.TypeNumeric || tb.Columns[n] == tiering.TypeJSONB {
				ex += "::text"
			}
			exprs = append(exprs, ex)
		}
		rows, err := e.admin.Query(e.ctx, fmt.Sprintf("SELECT %s FROM %s t WHERE %s", strings.Join(exprs, ", "), tb.Name, where), id)
		require.NoError(t, err)
		defer rows.Close()
		var out []map[string]any
		for rows.Next() {
			vals, err := rows.Values()
			require.NoError(t, err)
			m := map[string]any{}
			for i, n := range names {
				m[n] = vals[i]
			}
			b, err := tiering.Row(tb.Columns, m)
			require.NoError(t, err)
			var obj map[string]any
			require.NoError(t, json.Unmarshal(b, &obj))
			out = append(out, obj)
		}
		return out
	}
	roots := read(c.Root, "t.id = $1")
	require.Len(t, roots, 1)
	return roots[0], read(c.Children[0].Table, "t.order_id = $1")
}

func sortLines(ls []map[string]any) []map[string]any {
	sort.Slice(ls, func(i, j int) bool { return fmt.Sprint(ls[i]["line"]) < fmt.Sprint(ls[j]["line"]) })
	return ls
}

// published moves a sealed unit of two orders to a route and releases its online rows; it answers the unit and the
// orders' canonical rows as they were online.
func published(t *testing.T, e env, route string) (string, map[string]map[string]any, map[string][]map[string]any) {
	t.Helper()
	withRoute(e, route)
	a, b := e.order(t, 400), e.order(t, 400)
	roots, lines := map[string]map[string]any{}, map[string][]map[string]any{}
	for _, id := range []string{a, b} {
		roots[id], lines[id] = onlineCanonical(t, e, id)
	}
	groups, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	res, err := e.cat.Seal(e.ctx, "sales.orders", groups[0])
	require.NoError(t, err)
	_, err = e.cat.Transfer(e.ctx, res.Unit)
	require.NoError(t, err)
	e.cat.Grace, e.cat.Backups = 0, backupAt(time.Now().Add(time.Minute))
	why, err := e.cat.Release(e.ctx, res.Unit)
	require.NoError(t, err)
	require.Empty(t, why)
	return res.Unit, roots, lines
}

// TR08, G4: a released order read by its key through the locate index comes back from either route exactly as it
// was online; an order still online is the caller's to read (found=false).
func TestReadsByKeyFromEveryRoute(t *testing.T) {
	for _, route := range []string{tiering.RoutePostgres, tiering.RouteObject} {
		t.Run(route, func(t *testing.T) {
			e := setup(t, "tier_get_"+strings.ReplaceAll(route, "-", "_"))
			store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
			ensureBucket(t, admin, tierBucket, false)
			e.cat.Store = store
			open := e.order(t, -1)
			_, roots, lines := published(t, e, route)
			for id := range roots {
				agg, found, err := e.cat.Get(e.ctx, "sales.orders", id)
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, roots[id], agg.Root)
				assert.Equal(t, sortLines(lines[id]), sortLines(agg.Children["order_lines"]))
				assert.Equal(t, route, agg.Route)
			}
			_, found, err := e.cat.Get(e.ctx, "sales.orders", open)
			require.NoError(t, err)
			assert.False(t, found, "still online: the caller reads it")
		})
	}
}

// TR09: the catalog points at the object store and the object is gone: "cannot be read now", never "not found".
func TestAMissingObjectIsUnavailableNotAbsent(t *testing.T) {
	e := setup(t, "tier_missing")
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	e.cat.Store = store
	_, roots, _ := published(t, e, tiering.RouteObject)
	objects, err := store.List(e.ctx, store.Prefix+"/")
	require.NoError(t, err)
	for _, o := range objects {
		_, err := admin.DeleteObject(e.ctx, &s3.DeleteObjectInput{Bucket: aws.String(tierBucket), Key: aws.String(o.Key)})
		require.NoError(t, err)
	}
	for id := range roots {
		_, _, err := e.cat.Get(e.ctx, "sales.orders", id)
		assert.True(t, errs.Is(err, "be", errs.ReasonDependencyUnavailable), "%v", err)
		assert.False(t, errs.Is(err, "be", errs.ReasonNotFound))
	}
}

// TR07, TR12, P15.24-P15.26: a list under a snapshot reads every order once, whether its unit left before the snapshot
// (read from its route, its online rows already excluded) or after it (its online rows stay for the lease); the cold
// rows are filtered and masked by the current authorization; a lease that ran out says start again.
func TestAListUnderASnapshotReadsEachOrderOnce(t *testing.T) {
	e := setup(t, "tier_list")
	withRoute(e, tiering.RoutePostgres)
	e.cat.Grace, e.cat.Backups = 0, backupAt(time.Now().Add(time.Hour))
	first := sealedUnit(t, e)
	_, err := e.cat.Transfer(e.ctx, first) // published before the snapshot, online rows still there (the grace)
	require.NoError(t, err)

	s, err := e.cat.Snapshot(e.ctx, "test list")
	require.NoError(t, err)
	e.le = ids.New().String() // another legal entity: its unit published after the snapshot
	second := sealedUnit(t, e)
	_, err = e.cat.Transfer(e.ctx, second)
	require.NoError(t, err)
	why, err := e.cat.Release(e.ctx, second)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyLease}, why, "the snapshot's lease keeps its online rows")

	cl := class()
	seen := map[string]int{}
	require.NoError(t, e.d.Tx(e.ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT r.id::text FROM orders r WHERE r.status = 'closed' AND "+s.OnlineExclusion(cl, "r"))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			seen[id]++
		}
		return rows.Err()
	}))
	units, err := e.cat.ColdUnits(e.ctx, s, "sales.orders")
	require.NoError(t, err)
	assert.Equal(t, []string{first}, units)
	for _, u := range units {
		aggs, err := e.cat.ReadUnit(e.ctx, s, u, nil)
		require.NoError(t, err)
		for _, a := range aggs {
			seen[fmt.Sprint(a.Root["id"])]++
		}
	}
	require.Len(t, seen, 4, "two units of two orders")
	for id, n := range seen {
		assert.Equal(t, 1, n, "order %s read once", id)
	}

	// the current authorization: one order not visible, the others masked to two fields (TR12)
	firsts, err := e.cat.ReadUnit(e.ctx, s, first, nil)
	require.NoError(t, err)
	require.Len(t, firsts, 2)
	hidden := fmt.Sprint(firsts[0].Root["id"])
	aggs, err := e.cat.ReadUnit(e.ctx, s, first, func(root map[string]any) (bool, []string, error) {
		return root["id"] != hidden, []string{"id", "code"}, nil
	})
	require.NoError(t, err)
	require.Len(t, aggs, 1, "the hidden order is left out")
	for _, a := range aggs {
		assert.NotEqual(t, hidden, a.Root["id"])
		assert.Len(t, a.Root, 2, "masked to the readable fields")
	}

	_, err = e.admin.Exec(e.ctx, "UPDATE be_tier_lease SET expires_at = now() - interval '1 second' WHERE id = $1", s.Lease)
	require.NoError(t, err)
	_, err = e.cat.ReadUnit(e.ctx, s, first, nil)
	assert.True(t, errs.Is(err, "be", errs.ReasonQueryPlanChanged), "%v", err)
}

// TR18: a thawed unit reads from its read-only copy (here even with the objects gone); writes stay refused.
func TestAThawedUnitReadsFromItsCopy(t *testing.T) {
	e := setup(t, "tier_thaw")
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	e.cat.Store = store
	unit, roots, _ := published(t, e, tiering.RouteObject)
	require.NoError(t, e.cat.Thaw(e.ctx, unit, 30*24*time.Hour))
	objects, err := store.List(e.ctx, store.Prefix+"/")
	require.NoError(t, err)
	for _, o := range objects {
		_, err := admin.DeleteObject(e.ctx, &s3.DeleteObjectInput{Bucket: aws.String(tierBucket), Key: aws.String(o.Key)})
		require.NoError(t, err)
	}
	for id, root := range roots {
		agg, found, err := e.cat.Get(e.ctx, "sales.orders", id)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, root, agg.Root)
		err = e.write("INSERT INTO orders VALUES ($1, $2, 'x', NULL, 1, 'closed', now(), now(), NULL)", id, e.le)
		assert.True(t, errs.Is(err, "be", errs.ReasonRecordSealed), "a thaw reopens no write: %v", err)
	}
	removed, err := e.cat.CleanOrphans(e.ctx, unit, "sales.orders", time.Hour)
	require.NoError(t, err)
	assert.Zero(t, removed, "a live thaw is not an orphan")
}
