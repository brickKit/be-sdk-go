package work

import (
	"context"
	"errors"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// ErrRefused is returned when an operator action's guard does not hold (protocol 01's transition table).
var ErrRefused = errors.New("work: the item's state does not allow this action")

func operate(ctx context.Context, d *db.DB, sql string, args ...any) error {
	return d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, sql, args...)
		if err == nil && tag.RowsAffected() == 0 {
			return ErrRefused
		}
		return err
	})
}

// Requeue puts a dead or paused item back to pending (P7.14). A paused item with an unknown outcome needs the
// operator's record that the call did not take effect (verifiedNotEffective); a dead item never has one.
func Requeue(ctx context.Context, d *db.DB, id string, verifiedNotEffective bool) error {
	return operate(ctx, d, `UPDATE be_work_item SET state = 'pending', pause_reason = NULL, attempts = 0, run_at = now(),
		outcome_unknown = false, holder = NULL, lease_until = NULL
		WHERE id = $1 AND (
			(state = 'dead' AND NOT outcome_unknown) OR
			(state = 'paused' AND pause_reason IN ('backlog_exceeded', 'business_hold')) OR
			(state = 'paused' AND pause_reason = 'outcome_unknown' AND $2))`, id, verifiedNotEffective)
}

// Resolve marks a dead or paused item resolved with the operator's reason; it never runs again.
func Resolve(ctx context.Context, d *db.DB, id, reason string) error {
	return operate(ctx, d, `UPDATE be_work_item SET state = 'resolved', pause_reason = NULL, result = to_jsonb($2::text),
		finished_at = now() WHERE id = $1 AND state IN ('dead', 'paused')`, id, reason)
}

// Hold pauses a pending or waiting item for a business check (business_hold).
func Hold(ctx context.Context, d *db.DB, id string) error {
	return operate(ctx, d, `UPDATE be_work_item SET state = 'paused', pause_reason = 'business_hold'
		WHERE id = $1 AND state IN ('pending', 'retry_wait')`, id)
}

// Retention of finished items (P7.19).
const (
	QueueRetention = 7 * 24 * time.Hour
	CronRetention  = 30 * 24 * time.Hour
)

// Purge deletes finished items past their retention; dead and paused items are never deleted. Cron items go only
// when their slot is behind the watermark, so the slot is never materialized again.
func Purge(ctx context.Context, d *db.DB) (int64, error) {
	var n int64
	err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM be_work_item w WHERE state IN ('done', 'resolved') AND (
			(kind = 'queue' AND finished_at < now() - make_interval(secs => $1)) OR
			(kind = 'cron' AND finished_at < now() - make_interval(secs => $2)
				AND run_at <= (SELECT watermark_at FROM be_cron_watermark m WHERE m.job = w.job)))`,
			QueueRetention.Seconds(), CronRetention.Seconds())
		n = tag.RowsAffected()
		return err
	})
	return n, err
}
