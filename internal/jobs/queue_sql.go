package jobs

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// enqueueSQL inserts in the caller's business transaction; with a unique key, a live job of the same
// kind and key makes it a no-op (P14 "queue", the partial index besdk_job_queue_unique).
const enqueueSQL = `INSERT INTO besdk_job_queue (id, kind, args, unique_key, run_at, max_attempts, traceparent,
 causation_id, hop_count)
VALUES ($1, $2, $3::jsonb, $4, COALESCE($5::timestamptz, now()), $6, $7, $8, $9)
ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL AND state <> 'done' DO NOTHING`

// queueClaim takes due ready rows, and with $4 also running rows whose lease expired, with SKIP
// LOCKED; attempts counts starts, so a run that crashed the process still uses an attempt.
const queueClaim = `UPDATE besdk_job_queue SET state = 'running', attempts = attempts + 1,
       lease_until = now() + $3::bigint * interval '1 millisecond'
 WHERE id IN (
       SELECT id FROM besdk_job_queue
        WHERE kind = $1
          AND ((state = 'ready' AND run_at <= now()) OR ($4::boolean AND state = 'running' AND lease_until < now()))
        ORDER BY run_at, id
        LIMIT $2
        FOR UPDATE SKIP LOCKED)
RETURNING id::text, args::text, COALESCE(unique_key, ''), attempts, max_attempts, last_error, traceparent,
          causation_id, hop_count, created_at, run_at`

// The outcomes only touch the row as this claim left it (same attempts, still running): a row whose
// lease expired and that another worker re-claimed is not overwritten.
const (
	queueDone = `UPDATE besdk_job_queue SET state = 'done', finished_at = now(), lease_until = NULL, last_error = ''
 WHERE id = $1 AND state = 'running' AND attempts = $2`
	queueRetry = `UPDATE besdk_job_queue SET state = 'ready', run_at = now() + $3::bigint * interval '1 millisecond',
       lease_until = NULL, last_error = $4
 WHERE id = $1 AND state = 'running' AND attempts = $2`
	queueDead = `UPDATE besdk_job_queue SET state = 'dead', finished_at = now(), lease_until = NULL, last_error = $3
 WHERE id = $1 AND state = 'running' AND attempts = $2`
	// queueRelease gives a row back without using an attempt (the process is stopping).
	queueRelease = `UPDATE besdk_job_queue SET state = 'ready', lease_until = NULL, attempts = attempts - 1
 WHERE id = $1 AND state = 'running' AND attempts = $2`
	queueDepth = `SELECT state, count(*) FROM besdk_job_queue
 WHERE kind = $1 AND state IN ('ready', 'running', 'dead') GROUP BY state`
	queueOldest = `SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(run_at)), 0)::float8 FROM besdk_job_queue
 WHERE kind = $1 AND state = 'ready' AND run_at <= now()`
)

// claimQueue claims up to limit rows of kind; expired lets it take running rows whose lease expired.
func (e *Engine) claimQueue(ctx context.Context, w *Worker, limit int, expired bool) ([]QueuedJob, error) {
	lease := (w.Timeout + leaseGrace).Milliseconds()
	var out []QueuedJob
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		out = out[:0]
		rows, err := tx.QueryContext(ctx, queueClaim, w.Kind, limit, lease, expired)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			j := QueuedJob{Kind: w.Kind}
			var args string
			if err := rows.Scan(&j.ID, &args, &j.UniqueKey, &j.Attempt, &j.MaxAttempts, &j.LastError,
				&j.TraceParent, &j.CausationID, &j.HopCount, &j.EnqueuedAt, &j.RunAt); err != nil {
				return err
			}
			j.Args = []byte(args)
			out = append(out, j)
		}
		return rows.Err()
	})
	return out, err
}

// refreshQueueGauges sets be_queue_depth{kind,state} and be_queue_oldest_age_seconds{kind} (P14.3).
func (e *Engine) refreshQueueGauges(ctx context.Context, kind string) {
	depth := map[string]float64{"ready": 0, "running": 0, "dead": 0}
	var oldest float64
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		rows, err := tx.QueryContext(ctx, queueDepth, kind)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var state string
			var n int64
			if err := rows.Scan(&state, &n); err != nil {
				return err
			}
			depth[state] = float64(n)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, queueOldest, kind).Scan(&oldest)
	})
	if err != nil {
		return
	}
	for state, n := range depth {
		e.m.queueDepth.WithLabelValues(kind, state).Set(n)
	}
	e.m.queueOldest.WithLabelValues(kind).Set(oldest)
}

// millis is d in whole milliseconds for the `$n::bigint * interval '1 millisecond'` parameters.
func millis(d time.Duration) int64 { return d.Milliseconds() }
