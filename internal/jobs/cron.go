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

// slotClaim is the normative claim of P14 "Statements": whoever inserted runs the slot.
const slotClaim = `INSERT INTO besdk_job_slot (name, slot_at, holder) VALUES ($1, $2, $3)
ON CONFLICT (name, slot_at) DO NOTHING RETURNING 1`

const slotDone = `UPDATE besdk_job_slot SET done_at = now(), result = $3 WHERE name = $1 AND slot_at = $2`

// slotUnclaim gives a slot back when its run was cancelled by the process stopping, so the next
// start (or another replica) still runs it.
const slotUnclaim = `DELETE FROM besdk_job_slot WHERE name = $1 AND slot_at = $2 AND holder = $3`

const slotHistory = `SELECT EXISTS (SELECT 1 FROM besdk_job_slot WHERE name = $1)`

// cronLoop runs each slot of en once across replicas (P14 "cron"). Decision tree at start:
//   - the job has slot rows (it ran before): the most recent slot at or before now is eligible, so
//     after downtime only the most recent missed slot runs (P14.6); claiming a slot that already ran
//     is a no-op;
//   - no slot rows (a new job): it starts with the next slot.
//
// Then: sleep until the next slot, claim the latest slot reached (slots passed while asleep or busy
// are skipped), run it, record the result. A failed run ends the loop with an error (P1.7).
func (e *Engine) cronLoop(en *entry) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		holder := e.cfg.holder()
		cursor := en.sched.Prev(e.cfg.now())
		ran, err := e.slotHistory(ctx, en.name)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("job %s: read slots: %w", en.name, err)
		}
		if ran {
			cursor = cursor.Add(-time.Nanosecond)
		}
		for {
			due := en.sched.Next(cursor)
			if !sleep(ctx, due.Sub(e.cfg.now())) {
				return nil
			}
			slot := en.sched.Prev(e.cfg.now())
			if slot.Before(due) {
				slot = due
			}
			cursor = slot
			if _, err := e.runSlot(ctx, en, slot, holder, false); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

// runSlot claims slot for holder and, when the claim succeeds, runs it and records the result.
// claimed is false when the slot was already taken. err is a database failure or the run's failure.
func (e *Engine) runSlot(ctx context.Context, en *entry, slot time.Time, holder string, oneShot bool) (claimed bool, err error) {
	if claimed, err = e.claimSlot(ctx, en.name, slot, holder); err != nil || !claimed {
		return claimed, err
	}
	info := e.info(en, oneShot)
	info.Slot = slot
	res, runErr := e.exec(ctx, info, en.timeout, en.run)
	dctx, cancel := detached(ctx)
	defer cancel()
	if res == ResultCancelled {
		if err := e.exec1(dctx, slotUnclaim, en.name, slot, holder); err != nil {
			e.log.Warn("cron slot not given back", slog.String("job", en.name), slog.String("error", errorText(err)))
		}
		return true, nil
	}
	text := res
	if runErr != nil {
		text = res + ": " + errorText(runErr)
	}
	if err := e.exec1(dctx, slotDone, en.name, slot, text); err != nil {
		e.log.Warn("cron slot result not recorded", slog.String("job", en.name), slog.String("error", errorText(err)))
	}
	if res != ResultOK {
		return true, fmt.Errorf("job %s slot %s: %s: %w", en.name, slot.UTC().Format(time.RFC3339), res, runErr)
	}
	return true, nil
}

func (e *Engine) claimSlot(ctx context.Context, name string, slot time.Time, holder string) (bool, error) {
	var claimed bool
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		var one int
		err := tx.QueryRowContext(ctx, slotClaim, name, slot, holder).Scan(&one)
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
	return claimed, err
}

func (e *Engine) slotHistory(ctx context.Context, name string) (bool, error) {
	var ran bool
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		return tx.QueryRowContext(ctx, slotHistory, name).Scan(&ran)
	})
	return ran, err
}

// exec1 runs one statement in its own short transaction.
func (e *Engine) exec1(ctx context.Context, query string, args ...any) error {
	return e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	})
}
