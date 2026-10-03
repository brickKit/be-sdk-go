package jobs

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// Retention of the job tables (P14.7; controller ruling: done queue rows 7 days, slot rows 30 days).
type Retention struct {
	QueueDone time.Duration // done queue rows finished longer ago are deleted; 0 = 7 days
	Slots     time.Duration // slot rows older than this are deleted, except each job's newest; 0 = 30 days
	Reconcile time.Duration // besdk_reconcile rows untouched (next_at) for longer and not leased; 0 = 30 days
	Batch     int           // rows per delete statement; 0 = 1000
}

// CleanupResult counts the rows one cleanup deleted.
type CleanupResult struct {
	QueueRows, SlotRows, ReconcileRows int64
}

func (r Retention) withDefaults() Retention {
	if r.QueueDone <= 0 {
		r.QueueDone = 7 * 24 * time.Hour
	}
	if r.Slots <= 0 {
		r.Slots = 30 * 24 * time.Hour
	}
	if r.Reconcile <= 0 {
		r.Reconcile = 30 * 24 * time.Hour
	}
	if r.Batch <= 0 {
		r.Batch = 1000
	}
	return r
}

// The bounded deletes. Dead queue rows are kept for operators. A job's newest slot row is kept, so a
// rarely running cron job still knows it ran before (the catch-up rule of cronLoop).
const (
	cleanQueue = `DELETE FROM besdk_job_queue WHERE id IN (
       SELECT id FROM besdk_job_queue
        WHERE state = 'done' AND finished_at < now() - $1::bigint * interval '1 millisecond'
        LIMIT $2 FOR UPDATE SKIP LOCKED)`
	cleanSlots = `DELETE FROM besdk_job_slot WHERE (name, slot_at) IN (
       SELECT o.name, o.slot_at FROM besdk_job_slot o
        WHERE o.slot_at < now() - $1::bigint * interval '1 millisecond'
          AND o.slot_at < (SELECT max(n.slot_at) FROM besdk_job_slot n WHERE n.name = o.name)
        LIMIT $2 FOR UPDATE SKIP LOCKED)`
	cleanReconcile = `DELETE FROM besdk_reconcile WHERE (name, item_id) IN (
       SELECT name, item_id FROM besdk_reconcile
        WHERE next_at < now() - $1::bigint * interval '1 millisecond'
          AND (lease_until IS NULL OR lease_until < now())
        LIMIT $2 FOR UPDATE SKIP LOCKED)`
)

// Cleanup deletes the job rows past retention in bounded batches, one short transaction per batch,
// until a batch comes back short or ctx ends (P14.7). The runtime's be.cleanup job calls it.
func Cleanup(ctx context.Context, store *pg.Store, r Retention) (CleanupResult, error) {
	r = r.withDefaults()
	var res CleanupResult
	for _, step := range []struct {
		query string
		age   time.Duration
		n     *int64
	}{{cleanQueue, r.QueueDone, &res.QueueRows}, {cleanSlots, r.Slots, &res.SlotRows},
		{cleanReconcile, r.Reconcile, &res.ReconcileRows}} {
		for ctx.Err() == nil {
			var n int64
			err := store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
				out, err := tx.ExecContext(ctx, step.query, millis(step.age), r.Batch)
				if err != nil {
					return err
				}
				n, err = out.RowsAffected()
				return err
			})
			if err != nil {
				return res, err
			}
			*step.n += n
			if n < int64(r.Batch) {
				break
			}
		}
	}
	return res, ctx.Err()
}

// Cleanup runs the retention deletes on the engine's store.
func (e *Engine) Cleanup(ctx context.Context, r Retention) (CleanupResult, error) {
	return Cleanup(ctx, e.cfg.Store, r)
}
