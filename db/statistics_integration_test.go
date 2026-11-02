//go:build integration

package db_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// audited writes n rows straight into be_audit_log and flushes the session's table counters, so the next call of the
// statistics upkeep sees them (a backend reports its counters at most once a second otherwise).
func audited(t *testing.T, admin *pgx.Conn, n int) {
	pgtest.Exec(t, admin, `INSERT INTO be_audit_log (id, occurred_at, category, operation, outcome, digest)
		SELECT gen_random_uuid(), now(), 'business_change', 'op', 'succeeded', 'd' FROM generate_series(1, `+strconv.Itoa(n)+`)`)
	pgtest.Exec(t, admin, "SELECT pg_stat_force_next_flush()")
}

// The statistics upkeep of P5.33, as the runtime role: a table is analyzed once at least 1000 rows and at least its
// estimated size changed since its last analyze, and not again within 30 seconds; a temporary table of the same name
// changes nothing.
func TestTheStatisticsUpkeepAnalyzesOnlyTablesThatLag(t *testing.T) { // P5.33, CK-DB-10
	d, admin := dbtest.Schema(t, "stats1", nil)
	pgtest.Exec(t, admin, "ALTER TABLE be_audit_log SET (autovacuum_enabled = false)") // only the upkeep analyzes
	ctx := context.Background()
	analyze := func() []string {
		got, err := db.AnalyzeStale(ctx, d)
		require.NoError(t, err)
		return got
	}

	audited(t, admin, 999)
	require.Empty(t, analyze(), "999 rows changed: too few to matter")
	audited(t, admin, 1)
	require.Equal(t, []string{"be_audit_log"}, analyze(), "1000 rows changed on a table never analyzed")
	audited(t, admin, 1500)
	require.Empty(t, analyze(), "analyzed under 30 seconds ago")

	// analyzed with 2500 rows; resetting the counters forgets when, as if it were long ago: 1500 changes are fewer
	// than the estimated size, 4500 are more
	pgtest.Exec(t, admin, "ANALYZE be_audit_log")
	var estimate float64
	require.NoError(t, admin.QueryRow(ctx, "SELECT reltuples FROM pg_class WHERE oid = 'be_audit_log'::regclass").Scan(&estimate))
	require.Equal(t, 2500.0, estimate)
	pgtest.Exec(t, admin, "SELECT pg_stat_reset_single_table_counters('be_audit_log'::regclass)")
	audited(t, admin, 1500)
	require.Empty(t, analyze(), "1500 changes on a table estimated at 2500 rows")
	audited(t, admin, 3000)
	require.Equal(t, []string{"be_audit_log"}, analyze(), "4500 changes on a table estimated at 2500 rows")

	// a temporary table named like an SDK table is not what the function looks at
	pgtest.Exec(t, admin, "SELECT pg_stat_reset_single_table_counters('be_audit_log'::regclass)")
	pgtest.Exec(t, admin, "CREATE TEMP TABLE be_work_item AS SELECT g FROM generate_series(1, 5000) g")
	pgtest.Exec(t, admin, "SELECT pg_stat_force_next_flush()")
	var temp []string
	rows, err := admin.Query(ctx, "SELECT be_analyze_stale()")
	require.NoError(t, err)
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		temp = append(temp, name)
	}
	require.NoError(t, rows.Err())
	require.Empty(t, temp)
}
