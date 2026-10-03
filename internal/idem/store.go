package idem

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Querier is what the table operations need; *pg.Tx satisfies it. Every operation runs inside the
// caller's transaction (P13: one-step commands claim, execute and complete in one transaction).
//
// Clock: every time written or compared comes from the now argument (the runtime clock,
// Runtime.Now), never from SQL now(), so a test drives expiry deterministically and every replica
// agrees with its own clock. now is truncated to microseconds, PostgreSQL's precision.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Prior is what an earlier use of the key left (P13.3): nothing (Found false: the caller may
// execute, and after Claim it holds the claim), a claim still in progress (InProgress), or a
// completed result to replay (Result).
type Prior struct {
	Found      bool
	InProgress bool
	Result     Result
}

const (
	claimSQL = `INSERT INTO besdk_idempotency (caller, idempotency_key, command, target, request_hash, status, created_at, updated_at, expires_at)
VALUES ($1, $2, $3, $4, $5, 'CLAIMED', $6, $6, $7)
ON CONFLICT (caller, idempotency_key) DO NOTHING
RETURNING status`
	selectSQL = `SELECT command, target, request_hash, status, result, created_at, expires_at
FROM besdk_idempotency WHERE caller = $1 AND idempotency_key = $2`
	takeoverSQL = `UPDATE besdk_idempotency SET command = $3, target = $4, request_hash = $5, status = 'CLAIMED',
result = NULL, created_at = $6, updated_at = $6, expires_at = $7
WHERE caller = $1 AND idempotency_key = $2`
	completeSQL = `UPDATE besdk_idempotency SET status = 'DONE', result = $6::jsonb, updated_at = $7
WHERE caller = $1 AND idempotency_key = $2 AND command = $3 AND target = $4 AND request_hash = $5 AND status = 'CLAIMED'`
	releaseSQL = `DELETE FROM besdk_idempotency
WHERE caller = $1 AND idempotency_key = $2 AND command = $3 AND target = $4 AND request_hash = $5 AND status = 'CLAIMED'`
	deleteExpiredSQL = `DELETE FROM besdk_idempotency WHERE (caller, idempotency_key) IN (
SELECT caller, idempotency_key FROM besdk_idempotency WHERE expires_at <= $1
ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED)`
)

// claimAttempts bounds Claim's insert-or-read loop: a row can vanish between the conflicting insert
// and the read only when the retention job deletes it, so a second round always settles.
const claimAttempts = 3

// Claim looks up or claims the command's key atomically (P13.3, P13.6, P10.9), in this order:
//
//	INSERT … ON CONFLICT DO NOTHING returns a row  → claimed: Prior{} (execute, then Complete)
//	otherwise SELECT … FOR UPDATE the existing row:
//	  gone (deleted meanwhile)                     → try the insert again
//	  expired (now >= expires_at)                  → taken over as a fresh claim: Prior{}
//	  command / target / hash differ               → ErrMismatch
//	  CLAIMED                                      → ErrInProgress
//	  DONE                                         → Prior{Found, Result}: replay, do not execute
//
// Only the caller's own namespace is ever read, so nothing of another namespace leaks (P13.4).
func Claim(ctx context.Context, q Querier, c Command, now time.Time) (Prior, error) {
	now = now.Truncate(time.Microsecond)
	args := []any{c.Caller, c.Key, c.Name, c.Target, c.Hash, now, ExpiresAt(now)}
	for i := 0; i < claimAttempts; i++ {
		var st string
		err := q.QueryRowContext(ctx, claimSQL, args...).Scan(&st)
		if err == nil {
			return Prior{}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Prior{}, err
		}
		row, err := get(ctx, q, c.Caller, c.Key, " FOR UPDATE")
		if err != nil || row == nil {
			if err != nil {
				return Prior{}, err
			}
			continue
		}
		if !now.Before(row.ExpiresAt) {
			_, err := q.ExecContext(ctx, takeoverSQL, args...)
			return Prior{}, err
		}
		return priorOf(row, now, c, false)
	}
	return Prior{}, fmt.Errorf("idem: claim %s/%s did not settle in %d attempts", c.Caller, c.Key, claimAttempts)
}

// Lookup is Claim's decision without claiming (P13.5 GetStatus, two-step commands): absent or
// expired → Prior{}; mismatch → ErrMismatch; CLAIMED → Prior{Found, InProgress}; DONE → the result.
func Lookup(ctx context.Context, q Querier, c Command, now time.Time) (Prior, error) {
	row, err := get(ctx, q, c.Caller, c.Key, "")
	if err != nil {
		return Prior{}, err
	}
	return priorOf(row, now, c, true)
}

// priorOf turns Decide's answer into a Prior; a lookup reports a claim in progress as a value.
func priorOf(row *Row, now time.Time, c Command, lookup bool) (Prior, error) {
	d, err := Decide(row, now, c)
	switch {
	case lookup && errors.Is(err, ErrInProgress):
		return Prior{Found: true, InProgress: true}, nil
	case err != nil:
		return Prior{}, err
	case d.Outcome == Replay:
		return Prior{Found: true, Result: d.Result}, nil
	}
	return Prior{}, nil
}

// Get returns the row of (caller, key), or nil when there is none or it has expired at now (P13.7,
// P13.9: a GetStatus by key reads this).
func Get(ctx context.Context, q Querier, caller, key string, now time.Time) (*Row, error) {
	row, err := get(ctx, q, caller, key, "")
	if err != nil || row == nil || !now.Before(row.ExpiresAt) {
		return nil, err
	}
	return row, nil
}

func get(ctx context.Context, q Querier, caller, key, lock string) (*Row, error) {
	r := Row{Caller: caller, Key: key}
	var st string
	var result []byte
	err := q.QueryRowContext(ctx, selectSQL+lock, caller, key).Scan(&r.Command, &r.Target, &r.Hash, &st,
		&result, &r.CreatedAt, &r.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.Status = Status(st)
	if result != nil {
		var res Result
		if err := json.Unmarshal(result, &res); err != nil {
			return nil, fmt.Errorf("idem: stored result of %s/%s: %w", caller, key, err)
		}
		r.Result = &res
	}
	return &r, nil
}

// Complete marks the command's claim DONE with its result (P13.3). The claim must exist, be CLAIMED
// and belong to this command; anything else is a programming error. A body PostgreSQL's jsonb
// cannot hold (a string with U+0000) fails here, and the transaction with it.
func Complete(ctx context.Context, q Querier, c Command, r Result, now time.Time) error {
	if r.Body == nil {
		r.Body = json.RawMessage("null")
	}
	b, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("idem: complete %s/%s: %w", c.Caller, c.Key, err)
	}
	res, err := q.ExecContext(ctx, completeSQL, c.Caller, c.Key, c.Name, c.Target, c.Hash, string(b),
		now.Truncate(time.Microsecond))
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("idem: complete %s/%s: no claim of this command (%d rows, %v)", c.Caller, c.Key, n, err)
	}
	return nil
}

// Release deletes the command's claim so the same key may be retried after a step that failed for
// certain (P13.3). A completed key, another command's claim or no row is left alone (no error).
func Release(ctx context.Context, q Querier, c Command) error {
	_, err := q.ExecContext(ctx, releaseSQL, c.Caller, c.Key, c.Name, c.Target, c.Hash)
	return err
}

// DefaultDeleteBatch is DeleteExpired's batch size when limit <= 0.
const DefaultDeleteBatch = 1000

// DeleteExpired deletes at most limit rows with expires_at <= now (P13.7 retention), skipping rows
// another transaction holds, and returns how many it deleted. The retention job calls it in its own
// transaction per batch until it returns fewer than limit.
func DeleteExpired(ctx context.Context, q Querier, now time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = DefaultDeleteBatch
	}
	res, err := q.ExecContext(ctx, deleteExpiredSQL, now.Truncate(time.Microsecond), limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
