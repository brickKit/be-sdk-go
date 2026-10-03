package lifecycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func ensure(t *testing.T, e *env, now time.Time) lifecycle.Ensured {
	t.Helper()
	var got lifecycle.Ensured
	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		var err error
		got, err = lifecycle.EnsureWindows(ctx, tx, e.decl, now, "be.lifecycle")
		return err
	}))
	return got
}

// The runtime role keeps every window ahead through the SECURITY DEFINER functions (P10.12, P16.2,
// P16.6): followers get the same bounds, idempotently, and the clock moving on extends the window.
func TestEnsureWindowsAsRuntimeRole(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			e := newEnv(t, major, widgetFS(t), false)
			require.Empty(t, e.partitions(t, "widgets"), "migrated without the declaration: outbox only")

			got := ensure(t, e, day0)
			months := []string{"2026m10", "2026m11", "2026m12", "2027m01"}
			for _, tbl := range []string{"widgets", "widget_lines", "widget_ledger", "widget_audit"} {
				var want []string
				for _, m := range months {
					want = append(want, tbl+"_"+m)
				}
				require.Equal(t, want, e.partitions(t, tbl), tbl)
			}
			require.Equal(t, []string{"widget_jobs_2026w40", "widget_jobs_2026w41", "widget_jobs_2026w42"}, e.partitions(t, "widget_jobs"))
			require.Len(t, got.Created, 4*4+3, "the outbox window existed already")

			require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO widget_audit (id, created_at, caller, action) VALUES ($1, $2, 'system', 'created')`,
					uuid.Must(uuid.NewV7()), day0)
				return err
			}), "a row for today goes in")

			require.Empty(t, ensure(t, e, day0).Created, "idempotent")

			var units int
			e.scalar(t, &units, `SELECT count(*) FROM besdk_lifecycle_units WHERE table_name = 'widgets' AND state = 'ACTIVE'`)
			require.Equal(t, 4, units)
			var logged int
			e.scalar(t, &logged, `SELECT count(*) FROM besdk_lifecycle_log WHERE action = 'created' AND actor = 'be.lifecycle'`)
			require.Equal(t, 4*4+3, logged)

			later := ensure(t, e, day0.AddDate(0, 0, 40)) // 2026-11-12
			require.Contains(t, later.Created, "widgets_2027m02")
			require.Contains(t, later.Created, "widget_lines_2027m02")
			require.Contains(t, later.Created, "besdk_outbox_2026w48")
			require.Contains(t, e.partitions(t, "widget_jobs"), "widget_jobs_2026w48")
		})
	}
}

// Partitions are found by their bounds, never by name: an existing one with other naming is adopted,
// a partial overlap is skipped (foundations 09, "Ensure ahead").
func TestEnsureWindowsAdoptsByBounds(t *testing.T) {
	e := newEnv(t, "16", widgetFS(t), false)
	owner := ownerConn(t, e)
	_, err := owner.ExecContext(context.Background(), `CREATE TABLE widget_audit_oct PARTITION OF widget_audit FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
	  CREATE TABLE widget_jobs_odd PARTITION OF widget_jobs FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-10-08 00:00:00+00')`)
	require.NoError(t, err)

	got := ensure(t, e, day0)
	require.Contains(t, got.Adopted, "widget_audit_oct")
	require.NotContains(t, e.partitions(t, "widget_audit"), "widget_audit_2026m10")
	require.ElementsMatch(t, []string{"widget_jobs_2026w40", "widget_jobs_2026w41"}, got.Skipped)
	require.Equal(t, []string{"widget_jobs_2026w42", "widget_jobs_odd"}, e.partitions(t, "widget_jobs"))
	var key string
	e.scalar(t, &key, `SELECT unit_key FROM besdk_lifecycle_units WHERE table_name = 'widget_audit' AND range_from = '2026-10-01Z'`)
	require.Equal(t, "widget_audit_oct", key)
}
