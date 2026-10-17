package idem

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/work"
)

// cleanupBatch bounds one delete so the cleanup never holds many row locks at once.
const cleanupBatch = 1000

// Cleanup deletes expired records in batches and returns how many (P4.25).
func Cleanup(ctx context.Context, d *db.DB) (int, error) {
	total := 0
	for {
		var n int64
		if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			tag, err := tx.Exec(ctx, `DELETE FROM be_idempotency WHERE (principal, key) IN (
				SELECT principal, key FROM be_idempotency WHERE expires_at < now() LIMIT $1 FOR UPDATE SKIP LOCKED)`, cleanupBatch)
			n = tag.RowsAffected()
			return err
		}); err != nil {
			return total, err
		}
		total += int(n)
		if n < cleanupBatch {
			return total, nil
		}
	}
}

// Job is the hourly cleanup every member with a database runs (P4.25).
func Job(d *db.DB) work.Job {
	return work.Job{Name: "be.idempotency.cleanup", Kind: work.Singleton, Effect: work.Local, Interval: time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			if _, err := Cleanup(ctx, d); err != nil {
				return err
			}
			return run.Complete(ctx, nil)
		}}
}
