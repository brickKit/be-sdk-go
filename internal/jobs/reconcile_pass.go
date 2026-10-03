package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// recClaim is the normative claim of P14 "Statements".
const recClaim = `INSERT INTO besdk_reconcile (name, item_id, lease_until)
VALUES ($1, $2, now() + $3::bigint * interval '1 millisecond')
ON CONFLICT (name, item_id) DO UPDATE SET lease_until = now() + $3::bigint * interval '1 millisecond'
 WHERE (besdk_reconcile.lease_until IS NULL OR besdk_reconcile.lease_until < now())
   AND besdk_reconcile.next_at <= now()
RETURNING attempts`

const (
	recDelete = `DELETE FROM besdk_reconcile WHERE name = $1 AND item_id = $2`
	recRetry  = `UPDATE besdk_reconcile SET attempts = attempts + 1, next_at = now() + $3::bigint * interval '1 millisecond',
       lease_until = NULL, last_error = $4 WHERE name = $1 AND item_id = $2`
	recRelease = `UPDATE besdk_reconcile SET lease_until = NULL WHERE name = $1 AND item_id = $2`
	recPending = `SELECT count(*) FROM besdk_reconcile WHERE name = $1`
)

// passResult counts one reconciler pass.
type passResult struct{ handled, failed, gaveUp int }

// reconcilerLoop runs a pass at once and every Every on this replica; items are claimed one by one,
// so replicas share the work (P14 "reconciler"). A database failure of the pass ends the loop with an
// error (P1.7); an item's failure is retried through its besdk_reconcile row.
func (e *Engine) reconcilerLoop(en *entry) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		for {
			start := time.Now()
			if _, err := e.reconcilePass(ctx, en, false); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if !sleep(ctx, en.interval-time.Since(start)) {
				return nil
			}
		}
	}
}

// reconcilePass selects candidates in a short transaction, then claims and drives each one.
func (e *Engine) reconcilePass(ctx context.Context, en *entry, oneShot bool) (passResult, error) {
	r := en.rec
	var items []any
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		var err error
		items, err = r.candidates(ctx, tx, r.batch)
		return err
	})
	if err != nil {
		return passResult{}, fmt.Errorf("reconciler %s: candidates: %w", r.name, err)
	}
	e.refreshReconcileGauges(ctx, r, items)
	var pr passResult
	for _, item := range items {
		if ctx.Err() != nil {
			return pr, nil
		}
		if err := e.reconcileItem(ctx, en, item, oneShot, &pr); err != nil {
			return pr, err
		}
	}
	return pr, nil
}

// reconcileItem drives one candidate (decision tree):
//   - not claimed (leased elsewhere or backing off): skipped;
//   - attempts already at the maximum: GiveUp;
//   - Handle (outside any transaction) succeeded: Apply in a short transaction; Done deletes the row,
//     not done counts an attempt and reschedules (or gives up on the last one);
//   - Handle or Apply failed: attempts + 1, next_at from the backoff, last_error (or GiveUp);
//   - the process is stopping: the lease is released, nothing counted.
func (e *Engine) reconcileItem(ctx context.Context, en *entry, item any, oneShot bool, pr *passResult) error {
	r := en.rec
	id := r.id(item)
	attempts, claimed, err := e.claimItem(ctx, r, id)
	if err != nil {
		return fmt.Errorf("reconciler %s: claim %s: %w", r.name, id, err)
	}
	if !claimed {
		return nil
	}
	dctx, cancel := detached(ctx)
	defer cancel()
	if attempts >= r.max {
		e.giveUp(dctx, r, item, id, pr)
		return nil
	}
	pr.handled++
	var out Outcome
	res, herr := e.exec(ctx, e.info(en, oneShot), r.timeout, func(ctx context.Context) error {
		var err error
		out, err = r.handle(ctx, item)
		return err
	})
	if res == ResultCancelled {
		_ = e.exec1(dctx, recRelease, r.name, id)
		return nil
	}
	if herr == nil {
		herr = e.applyOutcome(dctx, r, item, id, attempts, out, pr)
	}
	if herr != nil {
		pr.failed++
		e.failItem(dctx, r, item, id, attempts, herr, pr)
	}
	return nil
}

// applyOutcome runs Apply and the row's bookkeeping in one transaction.
func (e *Engine) applyOutcome(ctx context.Context, r *Reconciler, item any, id string, attempts int, out Outcome, pr *passResult) error {
	last := !out.Done && attempts+1 >= r.max
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		if err := r.apply(ctx, tx, item, out); err != nil {
			return err
		}
		switch {
		case out.Done:
			_, err := tx.ExecContext(ctx, recDelete, r.name, id)
			return err
		case last:
			if err := r.giveUp(ctx, tx, item); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, recDelete, r.name, id)
			return err
		}
		delay := out.RetryAfter
		if delay <= 0 {
			delay = backoffFor(r.backoff, attempts+1)
		}
		_, err := tx.ExecContext(ctx, recRetry, r.name, id, millis(delay), "not done: "+out.State)
		return err
	})
	if err == nil && last {
		e.gaveUp(r, id, attempts+1)
		pr.gaveUp++
	}
	return err
}

// failItem records a failed attempt, giving up on the last one.
func (e *Engine) failItem(ctx context.Context, r *Reconciler, item any, id string, attempts int, cause error, pr *passResult) {
	if attempts+1 >= r.max {
		e.giveUp(ctx, r, item, id, pr)
		return
	}
	err := e.exec1(ctx, recRetry, r.name, id, millis(backoffFor(r.backoff, attempts+1)), errorText(cause))
	if err != nil {
		e.log.Warn("reconcile attempt not recorded", slog.String("job", r.name), slog.String("item", id),
			slog.String("error", errorText(err)))
	}
}

// giveUp calls GiveUp and deletes the row in one transaction (P14 "reconciler").
func (e *Engine) giveUp(ctx context.Context, r *Reconciler, item any, id string, pr *passResult) {
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		if err := r.giveUp(ctx, tx, item); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, recDelete, r.name, id)
		return err
	})
	if err != nil {
		e.log.Error("reconciler give-up failed; retried on a later pass", slog.String("job", r.name),
			slog.String("item", id), slog.String("error", errorText(err)))
		_ = e.exec1(ctx, recRelease, r.name, id)
		return
	}
	e.gaveUp(r, id, r.max)
	pr.gaveUp++
}

func (e *Engine) gaveUp(r *Reconciler, id string, attempts int) {
	e.m.recGiveups.WithLabelValues(r.name).Inc()
	e.log.Warn("reconciler gave up on an item", slog.String("job", r.name), slog.String("item", id),
		slog.Int("attempts", attempts))
}

func (e *Engine) claimItem(ctx context.Context, r *Reconciler, id string) (attempts int, claimed bool, err error) {
	lease := millis(r.timeout + leaseGrace)
	err = e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		err := tx.QueryRowContext(ctx, recClaim, r.name, id, lease).Scan(&attempts)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			claimed = false
			return nil
		case err != nil:
			return err
		}
		claimed = true
		return nil
	})
	return attempts, claimed, err
}

// refreshReconcileGauges: pending = the reconciler's besdk_reconcile rows; oldest age from Since over
// this pass's candidates (P14.3).
func (e *Engine) refreshReconcileGauges(ctx context.Context, r *Reconciler, items []any) {
	var pending int64
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		return tx.QueryRowContext(ctx, recPending, r.name).Scan(&pending)
	})
	if err == nil {
		e.m.recPending.WithLabelValues(r.name).Set(float64(pending))
	}
	var oldest float64
	now := e.cfg.now()
	for _, it := range items {
		if since, ok := r.since(it); ok && !since.IsZero() {
			oldest = max(oldest, now.Sub(since).Seconds())
		}
	}
	e.m.recOldest.WithLabelValues(r.name).Set(oldest)
}
