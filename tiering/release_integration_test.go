//go:build integration

package tiering_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/tiering"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func backupAt(at time.Time) tiering.BackupSource {
	return func(context.Context) (time.Time, bool, error) { return at, true, nil }
}

func count(t *testing.T, e env, sql string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, e.admin.QueryRow(e.ctx, sql, args...).Scan(&n))
	return n
}

// P15.19, TR06, TR25: the release waits for the grace, a backup after the publish, every lease taken before the
// route changed, and the unit's newest record to be N days old; then only the unit's rows go (children first), the
// unit is released and the catalog version moves.
func TestTheReleaseConditions(t *testing.T) {
	e := setup(t, "tier_release")
	withRoute(e, tiering.RoutePostgres)
	e.cat.N, e.cat.Grace = 90*24*time.Hour, 7*24*time.Hour
	open := e.order(t, -1)
	unit := sealedUnit(t, e)
	_, err := e.cat.Transfer(e.ctx, unit)
	require.NoError(t, err)

	why, err := e.cat.Release(e.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyGrace, tiering.WhyNoBackup}, why, "published now, no run profile")

	e.cat.Grace = 0
	e.cat.Backups = backupAt(time.Now().Add(-time.Hour)) // before the publish
	why, err = e.cat.Release(e.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyNoBackup}, why, "the backup finished before the publish")

	e.cat.Backups = backupAt(time.Now().Add(time.Minute))
	pgtest.Exec(t, e.admin, fmt.Sprintf("INSERT INTO be_tier_lease VALUES ('%s', 0, 'export', now() + interval '15 minutes')", ids.New()))
	why, err = e.cat.Release(e.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyLease}, why, "an export reading under the catalog before the publish (TR06)")
	pgtest.Exec(t, e.admin, "UPDATE be_tier_lease SET expires_at = now() - interval '1 second'")

	_, err = e.admin.Exec(e.ctx, "UPDATE be_tier_unit SET newest_created = now() - interval '30 days' WHERE id = $1", unit)
	require.NoError(t, err)
	why, err = e.cat.Release(e.ctx, unit)
	require.NoError(t, err)
	assert.Equal(t, []string{tiering.WhyRecentRecords}, why, "a record created 30 days ago (TR25)")
	_, err = e.admin.Exec(e.ctx, "UPDATE be_tier_unit SET newest_created = now() - interval '91 days' WHERE id = $1", unit)
	require.NoError(t, err)

	// the SDK's own work names its unit: another unit's name lets nothing through (P15.13)
	other := ids.New().String()
	err = e.d.Tx(e.ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT set_config('be.tier_unit', $1, true)", other); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DELETE FROM order_lines WHERE order_id IN (SELECT root_id::uuid FROM be_tier_member WHERE unit = $1)", unit)
		return err
	})
	assert.True(t, errs.Is(err, "be", errs.ReasonRecordSealed), "another unit's name: %v", err)

	e.cat.Batch = 1 // a line per transaction: the orders go only once no line is left
	before := count(t, e, "SELECT version FROM be_tier_catalog")
	why, err = e.cat.Release(e.ctx, unit)
	require.NoError(t, err)
	assert.Empty(t, why)
	assert.Equal(t, before+1, count(t, e, "SELECT version FROM be_tier_catalog"))
	var state string
	require.NoError(t, e.admin.QueryRow(e.ctx, "SELECT state FROM be_tier_unit WHERE id = $1", unit).Scan(&state))
	assert.Equal(t, tiering.StateReleased, state)
	assert.Zero(t, count(t, e, "SELECT count(*) FROM orders o JOIN be_tier_member m ON m.root_id = o.id::text"),
		"the unit's orders are gone")
	assert.Zero(t, count(t, e, "SELECT count(*) FROM order_lines l JOIN be_tier_member m ON m.root_id = l.order_id::text"))
	assert.Equal(t, 1, count(t, e, "SELECT count(*) FROM orders WHERE id = $1", open), "the open order stays")
	assert.Equal(t, 2, count(t, e, "SELECT count(*) FROM order_lines WHERE order_id = $1", open))
	assert.Equal(t, 2, count(t, e, "SELECT count(*) FROM be_tier_member WHERE unit = $1", unit), "the locate index stays")
}
