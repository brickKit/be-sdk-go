package idem

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/work"
)

// Cleanup deletes expired records in batches and returns how many (P4.25).
func Cleanup(ctx context.Context, d *db.DB) (int64, error) {
	return db.DeleteInBatches(ctx, d, `DELETE FROM be_idempotency WHERE (principal, key) IN (
		SELECT principal, key FROM be_idempotency WHERE expires_at < now() LIMIT $1 FOR UPDATE SKIP LOCKED)`)
}

// Job is the hourly cleanup every member with a database runs (P4.25).
func Job(d *db.DB) work.Job {
	return work.Job{Name: "be.idempotency.cleanup", Kind: work.Singleton, Effect: work.Local, Interval: time.Hour,
		MaxAttempts: work.Unlimited, Handle: func(ctx context.Context, run *work.Run) error {
			if _, err := Cleanup(ctx, d); err != nil {
				return err
			}
			return run.Complete(ctx, nil)
		}}
}
