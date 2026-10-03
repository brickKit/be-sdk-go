package lifecycle_test

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func insertOutbox(t *testing.T, e *env, at time.Time, status string, publishedAt any) {
	t.Helper()
	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO besdk_outbox (id, created_at, subject, aggregate_type, aggregate_id,
		  aggregate_version, occurred_at, payload, status, published_at) VALUES ($1, $2, 'c.w.x.v1', 'c.w.w', 'a', 1, $2, '{}', $3, $4)`,
			uuid.Must(uuid.NewV7()), at, status, publishedAt)
		return err
	}))
}

// Platform retention (P12.15): an outbox partition goes once its range ended and every row was
// published more than 14 days ago; a partition with an unpublished row, or a recent publication, stays.
func TestRetentionOutbox(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			e := newEnv(t, major, widgetFS(t), false)
			insertOutbox(t, e, day0, "PUBLISHED", day0)                                     // w40
			insertOutbox(t, e, day0.AddDate(0, 0, 7), "PENDING", nil)                       // w41
			insertOutbox(t, e, day0.AddDate(0, 0, 14), "PUBLISHED", day0.AddDate(0, 0, 20)) // w42, published 2026-10-23
			var pub published
			en := engine(t, e, "on", &pub)

			r := step(t, e, en, time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC))
			require.Equal(t, []string{"besdk_outbox_2026w40"}, r.Dropped, "w40 ended 10-05, published 10-03: 14 days passed")
			require.NotContains(t, e.partitions(t, "besdk_outbox"), "besdk_outbox_2026w40")
			require.Contains(t, pub.of("destroyed"), "conformance.widget.lifecycle.destroyed.v1 besdk_outbox/besdk_outbox_2026w40")

			r = step(t, e, en, time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC))
			require.Empty(t, r.Dropped, "w41 has a PENDING row; w42 was published 10-23, so not before 11-06 00:00 + …")
			r = step(t, e, en, time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC))
			require.Equal(t, []string{"besdk_outbox_2026w42"}, r.Dropped)
			require.Contains(t, e.partitions(t, "besdk_outbox"), "besdk_outbox_2026w41")
			var logged int
			e.scalar(t, &logged, `SELECT count(*) FROM besdk_lifecycle_log WHERE action = 'destroyed' AND table_name = 'besdk_outbox'`)
			require.Equal(t, 2, logged)
		})
	}
}

var queueFS = fstest.MapFS{
	"1_q.up.sql": {Data: []byte(`CREATE TABLE deliveries (id uuid NOT NULL, created_at timestamptz NOT NULL, state text NOT NULL,
	  updated_at timestamptz NOT NULL, PRIMARY KEY (id, created_at)) PARTITION BY RANGE (created_at);
	  CREATE TABLE delivery_attempts (id uuid NOT NULL, created_at timestamptz NOT NULL, PRIMARY KEY (id, created_at)) PARTITION BY RANGE (created_at);
	  CREATE TABLE ledger (id uuid NOT NULL, created_at timestamptz NOT NULL, PRIMARY KEY (id, created_at)) PARTITION BY RANGE (created_at);`)},
	"1_q.down.sql": {Data: []byte(`DROP TABLE deliveries; DROP TABLE delivery_attempts; DROP TABLE ledger;`)},
	"lifecycle.yaml": {Data: []byte(`lifecycle: v1
tables:
  deliveries:
    class: queue
    partition: {by: created_at, grain: week, ahead: 1}
    closed: {column: state, in: [SENT, DEAD], at: updated_at}
    retention: {min: 30d after created, end: destroy}
  delivery_attempts: {follows: deliveries}
  ledger:
    class: ledger
    partition: {by: created_at, grain: week, ahead: 1}
    retention: {min: 1d after created}
`)},
}

// business drops the platform's outbox partitions from a list (empty ones expire alongside).
func business(names []string) []string {
	var out []string
	for _, n := range names {
		if !strings.HasPrefix(n, lifecycle.OutboxTable) {
			out = append(out, n)
		}
	}
	return out
}

func insertDelivery(t *testing.T, e *env, at time.Time, state string) {
	t.Helper()
	require.NoError(t, e.run(t, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO deliveries VALUES ($1, $2, $3, $2)`, uuid.Must(uuid.NewV7()), at, state)
		return err
	}))
}

// Queue retention: a partition with no open row is dropped, follower first, once retention.min has
// passed (G3); a partition with an open row never is; a ledger is never dropped without a cold copy (G4).
func TestRetentionQueue(t *testing.T) {
	e := newEnv(t, "16", queueFS, true) // deliveries_2026w40 (w40) and _10_05 (w41)
	insertDelivery(t, e, day0, "PENDING")
	insertDelivery(t, e, day0.AddDate(0, 0, 3), "SENT")
	var pub published
	en := engine(t, e, "on", &pub)

	r := step(t, e, en, time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC))
	require.Empty(t, business(r.Dropped), "w41 ended 10-12: 30 days pass on 11-11")
	r = step(t, e, en, time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC))
	require.Equal(t, []string{"delivery_attempts_2026w41", "deliveries_2026w41"}, business(r.Dropped))
	require.Contains(t, e.partitions(t, "deliveries"), "deliveries_2026w40", "an open row keeps w40")
	require.Contains(t, e.partitions(t, "ledger"), "ledger_2026w40")
	var state string
	e.scalar(t, &state, `SELECT state FROM besdk_lifecycle_units WHERE unit_key = 'deliveries_2026w41'`)
	require.Equal(t, "DESTROYED", state)
	require.Contains(t, pub.of("destroyed"), "conformance.widget.lifecycle.destroyed.v1 deliveries/deliveries_2026w41")

	r = step(t, e, engine(t, e, "dry-run", &pub), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Empty(t, r.Dropped)
	require.Contains(t, r.Planned, "drop deliveries_2026w46")
	require.Contains(t, r.Planned, "drop delivery_attempts_2026w46", "followers are planned with their parent")
	require.Contains(t, e.partitions(t, "deliveries"), "deliveries_2026w46", "dry-run drops nothing")
}
