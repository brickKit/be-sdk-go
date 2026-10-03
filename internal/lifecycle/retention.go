package lifecycle

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// OutboxRetention is how long an outbox partition is kept after its range ended and after its last
// row was published (P12.15, class platform).
const OutboxRetention = 14 * 24 * time.Hour

// retentionStep expires what may leave the database in the 1.0 engine (P16 "Expire", G3):
//   - platform: an outbox partition whose range ended and whose rows were all PUBLISHED more than
//     OutboxRetention ago;
//   - queue: a range partition with no open row (by `closed`) once retention.min has passed after its
//     anchor (`created`: the range end; `closed`: the last close). A queue table without `closed`, or
//     with retention.min forever or another anchor, is never dropped;
//   - snapshot: nothing; document, ledger, audit: nothing, because no cold copy can exist (G4).
//
// Every drop is a plain DETACH + DROP through besdk_drop_partition under the short step lock_timeout.
func (e *Engine) retentionStep(ctx context.Context, run RunFunc, now time.Time, r *Report) error {
	errs := []error{e.expire(ctx, run, now, r, OutboxTable, nil, e.outboxDue)}
	for _, name := range e.decl.Names() {
		t := e.decl.Tables[name]
		if t.Class != Queue || t.Follows != "" || !t.Partition.IsRange() || t.Closed == nil || t.Forever ||
			t.RetentionMin == nil || (t.RetentionMin.Anchor != "created" && t.RetentionMin.Anchor != "closed") {
			continue
		}
		errs = append(errs, e.expire(ctx, run, now, r, name, e.decl.Followers(name), func(ctx context.Context, tx Tx, p partition, now time.Time) (bool, error) {
			return queueDue(ctx, tx, t, p, now)
		}))
	}
	return errors.Join(errs...)
}

type dueFunc func(ctx context.Context, tx Tx, p partition, now time.Time) (bool, error)

// expire drops every partition of table that due says may go, each in its own locked transaction,
// followers' partitions with the same bounds first.
func (e *Engine) expire(ctx context.Context, run RunFunc, now time.Time, r *Report, table string, followers []string, due dueFunc) error {
	var parts []partition
	if err := run(ctx, e.o.StepLockTimeout, func(ctx context.Context, tx Tx) error {
		var err error
		parts, err = listPartitions(ctx, tx, table)
		return err
	}); err != nil {
		return err
	}
	var errs []error
	for _, p := range parts {
		if !p.To.Valid || p.To.Time.After(now) {
			continue
		}
		errs = append(errs, e.locked(ctx, run, r, "retention", func(ctx context.Context, tx Tx) error {
			ok, err := due(ctx, tx, p, now)
			if err != nil || !ok {
				return err
			}
			for _, f := range followers {
				if err := e.dropSame(ctx, tx, f, p, now, r); err != nil {
					return err
				}
			}
			return e.drop(ctx, tx, table, p, now, r)
		}))
	}
	return errors.Join(errs...)
}

func (e *Engine) outboxDue(ctx context.Context, tx Tx, p partition, now time.Time) (bool, error) {
	var pending bool
	var last sql.NullTime
	err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+quoteIdent(p.Name)+` WHERE status <> 'PUBLISHED'),
	  (SELECT max(published_at) FROM `+quoteIdent(p.Name)+`)`).Scan(&pending, &last)
	if err != nil || pending {
		return false, err
	}
	until := p.To.Time
	if last.Valid && last.Time.After(until) {
		until = last.Time
	}
	return !until.Add(OutboxRetention).After(now), nil
}

func queueDue(ctx context.Context, tx Tx, t *Table, p partition, now time.Time) (bool, error) {
	open, _, err := openRows(ctx, tx, t, p.Name)
	if err != nil || open > 0 {
		return false, err
	}
	anchor := p.To.Time
	if t.RetentionMin.Anchor == "closed" {
		last, err := lastClosed(ctx, tx, t, p.Name)
		if err != nil {
			return false, err
		}
		if last.Valid && last.Time.After(anchor) {
			anchor = last.Time
		}
	}
	return !t.RetentionMin.AddTo(anchor).After(now), nil
}

// dropSame drops the follower's partition with p's bounds, if there is one.
func (e *Engine) dropSame(ctx context.Context, tx Tx, follower string, p partition, now time.Time, r *Report) error {
	parts, err := listPartitions(ctx, tx, follower)
	if err != nil {
		return err
	}
	if fp, _ := match(parts, p.From.Time, p.To.Time); fp != nil {
		return e.drop(ctx, tx, follower, *fp, now, r)
	}
	return nil
}

// drop detaches and drops one partition, marks its unit DESTROYED, logs and announces it; in dry-run
// it only plans it.
func (e *Engine) drop(ctx context.Context, tx Tx, table string, p partition, now time.Time, r *Report) error {
	if e.o.Config.Mode == ModeDryRun {
		r.Planned = append(r.Planned, "drop "+p.Name)
		return nil
	}
	if _, err := tx.ExecContext(ctx, `SELECT besdk_drop_partition($1, $2)`, table, p.Name); err != nil {
		return err
	}
	var version int64 = 1
	err := tx.QueryRowContext(ctx, `UPDATE besdk_lifecycle_units SET state = 'DESTROYED', destroyed_at = $3,
	  version = version + 1, updated_at = now() WHERE table_name = $1 AND unit_key = $2 RETURNING version`,
		table, p.Name, now).Scan(&version)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	payload := map[string]any{"table": table, "unit": p.Name, "reason": "retention",
		"destroyed_at": now.UTC().Format(time.RFC3339Nano)}
	addBounds(payload, unitRow{From: p.From, To: p.To})
	if err := logAction(ctx, tx, now, table, p.Name, "destroyed", Actor, payload); err != nil {
		return err
	}
	r.Dropped = append(r.Dropped, p.Name)
	return e.o.Publish(ctx, tx, e.event("destroyed", table, p.Name, version, payload))
}
