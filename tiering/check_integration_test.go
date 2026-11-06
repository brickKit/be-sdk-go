//go:build integration

package tiering_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/recovery"
	"github.com/brickKit/be-sdk-go/v3/recoverypb"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tieringCheck runs the member's CheckService and answers the be.tiering result.
func tieringCheck(t *testing.T, w *destroyWorld) *recoverypb.CheckResult {
	t.Helper()
	reg := &recovery.Checks{}
	require.NoError(t, reg.Add(w.cat.RecoveryCheck()))
	resp, err := recovery.NewServer(w.d, false, reg).Check(w.ctx, &recoverypb.CheckRequest{Recovery: "r-1"})
	require.NoError(t, err)
	for _, r := range resp.GetResults() {
		if r.GetCheck() == tiering.CheckName {
			return r
		}
	}
	t.Fatal("no be.tiering result")
	return nil
}

func differences(r *recoverypb.CheckResult) map[string]string {
	out := map[string]string{}
	for _, d := range r.GetDifferences() {
		k := d.GetObjectType() + " " + d.GetObjectId()
		out[k] += d.GetDetail() + "; "
	}
	return out
}

// P15.36, RC11, TR17: after a recovery the catalog, the manifests, the warm rows and the objects are compared: a
// route's copy missing or short, objects and warm rows of no live version (exports aside), a released unit with online
// rows (which the daily destruction job then deletes) and an older structure without a converter are differences;
// object units without a store are unprovable.
func TestTheTieringRecoveryCheck(t *testing.T) {
	w := &destroyWorld{env: setup(t, "tier_check"), kept: &keeper{}}
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	w.cat.Store = store
	warmUnit, warmRoots, _ := published(t, w.env, tiering.RoutePostgres)
	w.env.le = ids.New().String()
	coldUnit, _, _ := published(t, w.env, tiering.RouteObject)
	require.NoError(t, store.Put(w.ctx, store.Prefix+"/exports/x.ndjson", []byte("{}\n")))
	r := tieringCheck(t, w)
	require.Equal(t, recovery.Pass, r.GetOutcome(), "%v", r.GetDifferences())

	objects, err := store.List(w.ctx, fmt.Sprintf("%s/sales.orders/%s/", store.Prefix, coldUnit))
	require.NoError(t, err)
	var part string
	for _, o := range objects {
		if o.Key[len(o.Key)-8:] == ".parquet" {
			part = o.Key
		}
	}
	_, err = admin.DeleteObject(w.ctx, &s3.DeleteObjectInput{Bucket: aws.String(tierBucket), Key: aws.String(part)})
	require.NoError(t, err)
	_, err = w.admin.Exec(w.ctx, "DELETE FROM be_tier_row WHERE ctid IN (SELECT ctid FROM be_tier_row WHERE unit = $1 LIMIT 1)", warmUnit)
	require.NoError(t, err)
	orphan := fmt.Sprintf("%s/sales.orders/%s/99.9/orders-0.parquet", store.Prefix, coldUnit)
	require.NoError(t, store.Put(w.ctx, orphan, []byte("x")))
	_, err = w.admin.Exec(w.ctx, "INSERT INTO be_tier_row VALUES ($1, 7, 'orders', 'k', '\\x00')", warmUnit)
	require.NoError(t, err)
	var root string
	for id := range warmRoots {
		root = id
	}
	tx, err := w.admin.Begin(w.ctx) // an online row of a released unit back (the barrier lets its own unit through)
	require.NoError(t, err)
	_, err = tx.Exec(w.ctx, "SELECT set_config('be.tier_unit', $1, true)", warmUnit)
	require.NoError(t, err)
	_, err = tx.Exec(w.ctx, "INSERT INTO orders VALUES ($1, $2, 'x', NULL, 1, 'closed', now(), now(), NULL)", root, ids.New().String())
	require.NoError(t, err)
	require.NoError(t, tx.Commit(w.ctx))

	r = tieringCheck(t, w)
	assert.Equal(t, recovery.Diff, r.GetOutcome())
	diffs := differences(r)
	assert.Contains(t, diffs["tier_unit "+coldUnit], part, "RC11: an object of the manifest missing")
	assert.Contains(t, diffs["tier_unit "+warmUnit], "the warm route has 5 of the manifest's 6 rows")
	assert.Contains(t, diffs, "tier_object "+orphan, "TR17: an object of no manifest")
	assert.Contains(t, diffs, "tier_rows "+warmUnit+"/7")
	assert.NotContains(t, diffs, "tier_object "+store.Prefix+"/exports/x.ndjson", "exports are not the catalog's")
	found := false
	for _, d := range r.GetDifferences() {
		if d.GetObjectId() == warmUnit && d.GetDetail() == "released with 1 online roots left: the daily destruction job deletes them" {
			found = true
		}
	}
	assert.True(t, found, "%v", r.GetDifferences())
	assert.NotEmpty(t, r.GetDisposal())

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	runner := work.NewRunner(w.d, "test/sales", quiet)
	require.NoError(t, runner.Add(w.cat.DestroyJob(quiet)))
	ctx, cancel := context.WithCancel(w.ctx)
	require.NoError(t, runner.Start(ctx))
	require.Eventually(t, func() bool {
		return count(t, w.env, "SELECT count(*) FROM orders WHERE id = $1", root) == 0
	}, 20*time.Second, 100*time.Millisecond, "the daily destruction job deletes it")
	cancel()
	runner.Stop()

	cl := w.cat.Classes["sales.orders"]
	cl.SchemaVersion = "2"
	w.cat.Classes["sales.orders"] = cl
	assert.Contains(t, differences(tieringCheck(t, w)), "tier_schema sales.orders@1", "P15.34")
	cl.SchemaVersion = "1"
	w.cat.Classes["sales.orders"] = cl

	w.cat.Store = nil
	assert.Equal(t, recovery.Unprovable, tieringCheck(t, w).GetOutcome(), "object units and no store")
}
