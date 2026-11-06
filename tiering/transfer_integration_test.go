//go:build integration

package tiering_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/internal/s3test"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const tierBucket = "be-tiering-test"

// storeFor is a cold store over the test bucket (or another), the three roles on the test credentials.
func storeFor(t *testing.T, bucket, prefix string) (*tiering.Store, *s3.Client) {
	t.Helper()
	url, access, secret := os.Getenv("BE_SDK_TEST_S3_URL"), os.Getenv("BE_SDK_TEST_S3_ACCESS_KEY"), os.Getenv("BE_SDK_TEST_S3_SECRET_KEY")
	require.NotEmpty(t, url, "BE_SDK_TEST_S3_URL")
	admin := s3test.Client(url, access, secret)
	env := map[string]string{tiering.KeyStoreURL: url, tiering.KeyBucket: bucket}
	for _, r := range []string{"WRITE", "READ", "DELETE"} {
		env["TIERING_"+r+"_ACCESS_KEY"], env["TIERING_"+r+"_SECRET_KEY"] = access, secret
	}
	values, problems := config.Parse(tiering.StoreKeys(), config.Map(env))
	require.Empty(t, problems)
	s, err := tiering.StoreFrom(values, prefix)
	require.NoError(t, err)
	return s, admin
}

func ensureBucket(t *testing.T, admin *s3.Client, bucket string, locked bool) {
	t.Helper()
	_, err := admin.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket),
		ObjectLockEnabledForBucket: aws.Bool(locked)})
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if err != nil && !assert.ErrorAs(t, err, &owned) && !assert.ErrorAs(t, err, &exists) {
		t.Fatal(err)
	}
}

// sealedUnit seals two closed orders and answers the unit.
func sealedUnit(t *testing.T, e env) string {
	t.Helper()
	e.order(t, 400)
	e.order(t, 400)
	groups, err := e.cat.Groupings(e.ctx, "sales.orders")
	require.NoError(t, err)
	require.Len(t, groups, 1)
	res, err := e.cat.Seal(e.ctx, "sales.orders", groups[0])
	require.NoError(t, err)
	require.Equal(t, 2, res.Members)
	return res.Unit
}

func withRoute(e env, route string) {
	c := e.cat.Classes["sales.orders"]
	c.Routes = []string{route}
	e.cat.Classes["sales.orders"] = c
}

func unitRow(t *testing.T, e env, unit string) (state, route string, version int, catalog int64) {
	t.Helper()
	require.NoError(t, e.admin.QueryRow(e.ctx, "SELECT state, route, manifest_version, (SELECT version FROM be_tier_catalog) "+
		"FROM be_tier_unit WHERE id = $1", unit).Scan(&state, &route, &version, &catalog))
	return
}

// P15.14-P15.18 on both routes: claimed, copied, verified and published; the route and the manifest version change in
// one step with the catalog version; the object route's parts and manifest.json are in the bucket.
func TestATransferPublishesAVerifiedCopy(t *testing.T) {
	for _, route := range []string{tiering.RouteObject, tiering.RoutePostgres} {
		t.Run(route, func(t *testing.T) {
			e := setup(t, "tier_xfer_"+strings.ReplaceAll(route, "-", "_"))
			store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
			ensureBucket(t, admin, tierBucket, false)
			e.cat.Store = store
			withRoute(e, route)
			unit := sealedUnit(t, e)
			_, _, _, before := unitRow(t, e, unit)

			m, err := e.cat.Transfer(e.ctx, unit)
			require.NoError(t, err)
			state, got, version, after := unitRow(t, e, unit)
			assert.Equal(t, tiering.StatePublished, state)
			assert.Equal(t, route, got)
			assert.Equal(t, 1, version)
			assert.Equal(t, before+1, after, "the catalog version moves with the publish")
			assert.Equal(t, "2", m.Tables[0].Rows+"") // orders
			require.NoError(t, e.cat.Verify(e.ctx, m), "the published copy verifies again")
			if route == tiering.RouteObject {
				for _, o := range append(m.Tables[0].Objects, m.Tables[1].Objects...) {
					_, err := store.Get(e.ctx, o.Key)
					require.NoError(t, err, o.Key)
				}
				raw, err := store.Get(e.ctx, tiering.ManifestKey(store.Prefix, "sales.orders", unit, m.Generation, 1))
				require.NoError(t, err)
				want, _ := m.Canonical()
				assert.Equal(t, string(want), string(raw), "manifest.json beside the parts")
			}
			_, err = e.cat.ClaimUnit(e.ctx, unit)
			assert.Error(t, err, "a published unit is not claimed for its first transfer again")
		})
	}
}

// TR04, M1, P15.20: a worker whose claim was taken over cannot publish; the one that holds the claim can; what the
// first wrote is an orphan the cleanup removes, the published copy stays. A claim from another database epoch (a
// recovery advanced the generation) cannot publish either.
func TestOnlyTheCurrentClaimPublishes(t *testing.T) {
	e := setup(t, "tier_claims")
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	e.cat.Store = store
	withRoute(e, tiering.RouteObject)
	unit := sealedUnit(t, e)

	old, err := e.cat.ClaimUnit(e.ctx, unit)
	require.NoError(t, err)
	oldCopy, err := e.cat.Copy(e.ctx, old, tiering.RouteObject)
	require.NoError(t, err)
	current, err := e.cat.ClaimUnit(e.ctx, unit) // the lease ran out; another worker took over
	require.NoError(t, err)
	require.NoError(t, e.cat.Verify(e.ctx, oldCopy), "the old copy is complete: only the claim tells")
	assert.ErrorIs(t, e.cat.Publish(e.ctx, old, oldCopy), tiering.ErrStaleClaim)
	state, _, _, _ := unitRow(t, e, unit)
	assert.Equal(t, tiering.StateSealed, state, "nothing changed")

	m, err := e.cat.Copy(e.ctx, current, tiering.RouteObject)
	require.NoError(t, err)
	require.NoError(t, e.cat.Verify(e.ctx, m))
	require.NoError(t, e.cat.Publish(e.ctx, current, m))
	assert.ErrorIs(t, e.cat.Publish(e.ctx, current, m), tiering.ErrStaleClaim, "a published unit is not published again")

	removed, err := e.cat.CleanOrphans(e.ctx, unit, "sales.orders", 0)
	require.NoError(t, err)
	assert.Equal(t, 3, removed, "the old worker's two parts and its manifest.json")
	for _, o := range oldCopy.Tables[0].Objects {
		_, err := store.Get(e.ctx, o.Key)
		assert.ErrorIs(t, err, tiering.ErrMissing)
	}
	require.NoError(t, e.cat.Verify(e.ctx, m), "the published copy stays")

	// another unit: a recovery's advance between the claim and the publish (M1)
	e2 := setup(t, "tier_epoch")
	e2.cat.Store = store
	withRoute(e2, tiering.RoutePostgres)
	unit2 := sealedUnit(t, e2)
	c2, err := e2.cat.ClaimUnit(e2.ctx, unit2)
	require.NoError(t, err)
	m2, err := e2.cat.Copy(e2.ctx, c2, tiering.RoutePostgres)
	require.NoError(t, err)
	pgtest.Exec(t, e2.admin, "UPDATE be_recovery_generation SET generation = generation + 1")
	assert.ErrorIs(t, e2.cat.Publish(e2.ctx, c2, m2), tiering.ErrStaleClaim)
}

// TR05: a copy whose content differs from what was sealed (same row count) does not verify, and so is not published.
func TestATamperedCopyDoesNotVerify(t *testing.T) {
	e := setup(t, "tier_tamper")
	store, admin := storeFor(t, tierBucket, "sdk-test-"+ids.New().String())
	ensureBucket(t, admin, tierBucket, false)
	e.cat.Store = store
	withRoute(e, tiering.RoutePostgres)
	unit := sealedUnit(t, e)
	claim, err := e.cat.ClaimUnit(e.ctx, unit)
	require.NoError(t, err)
	m, err := e.cat.Copy(e.ctx, claim, tiering.RoutePostgres)
	require.NoError(t, err)
	pgtest.Exec(t, e.admin, "UPDATE be_tier_row SET row = convert_to(replace(convert_from(row, 'UTF8'), '\"3\"', '\"30\"'), 'UTF8') "+
		"WHERE tbl = 'order_lines' AND convert_from(row, 'UTF8') LIKE '%\"qty\":\"3\"%'")
	assert.ErrorIs(t, e.cat.Verify(e.ctx, m), tiering.ErrVerification)
	_, err = e.cat.Transfer(e.ctx, unit)
	require.NoError(t, err, "a new claim copies again and verifies")

	// the object route: an object replaced behind the manifest's back
	e2 := setup(t, "tier_tamper_obj")
	e2.cat.Store = store
	withRoute(e2, tiering.RouteObject)
	unit2 := sealedUnit(t, e2)
	c2, err := e2.cat.ClaimUnit(e2.ctx, unit2)
	require.NoError(t, err)
	m2, err := e2.cat.Copy(e2.ctx, c2, tiering.RouteObject)
	require.NoError(t, err)
	key := m2.Tables[1].Objects[0].Key
	_, err = admin.PutObject(e2.ctx, &s3.PutObjectInput{Bucket: aws.String(tierBucket), Key: aws.String(key),
		Body: bytes.NewReader([]byte("not the part"))})
	require.NoError(t, err)
	assert.ErrorIs(t, e2.cat.Verify(e2.ctx, m2), tiering.ErrVerification)

	// one byte changed, the size the same: only the object's hash tells, and it says so
	key = m2.Tables[0].Objects[0].Key
	data, err := store.Get(e2.ctx, key)
	require.NoError(t, err)
	again := append([]byte(nil), data...)
	again[len(again)/2] ^= 0xff
	m3 := m2
	m3.Tables = append([]tiering.ManifestTable(nil), m2.Tables...)
	m3.Tables[1].Objects = nil // leave the tampered lines out of this check: only the orders' object differs
	m3.Tables = m3.Tables[:1]
	_, err = admin.PutObject(e2.ctx, &s3.PutObjectInput{Bucket: aws.String(tierBucket), Key: aws.String(key),
		Body: bytes.NewReader(again)})
	require.NoError(t, err)
	err = e2.cat.Verify(e2.ctx, m3)
	assert.ErrorIs(t, err, tiering.ErrVerification)
	assert.ErrorContains(t, err, "is not what the manifest says")
}

// P15.32: an object-locked bucket refuses a class with erasable columns at start.
func TestALockedBucketRefusesErasableData(t *testing.T) {
	_, admin := storeFor(t, tierBucket, "x")
	locked := "be-tiering-test-locked"
	ensureBucket(t, admin, locked, true)
	e := setup(t, "tier_locked")
	url, access, secret := os.Getenv("BE_SDK_TEST_S3_URL"), os.Getenv("BE_SDK_TEST_S3_ACCESS_KEY"), os.Getenv("BE_SDK_TEST_S3_SECRET_KEY")
	env := map[string]string{tiering.KeyStoreURL: url, tiering.KeyBucket: locked}
	for _, r := range []string{"WRITE", "READ", "DELETE"} {
		env["TIERING_"+r+"_ACCESS_KEY"], env["TIERING_"+r+"_SECRET_KEY"] = access, secret
	}
	c := class()
	c.Erasable = []string{"note"}
	values, problems := config.Parse(tiering.Keys([]tiering.Class{c}), config.Map(env))
	require.Empty(t, problems)
	_, err := tiering.NewCatalog(e.ctx, e.d, []tiering.Class{c}, values, "sdk-test")
	assert.ErrorContains(t, err, "P15.32")
	c.Erasable = nil
	_, err = tiering.NewCatalog(e.ctx, e.d, []tiering.Class{c}, values, "sdk-test")
	assert.NoError(t, err, "a class without erasable columns may go there")
}
