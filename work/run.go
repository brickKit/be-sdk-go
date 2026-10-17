package work

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
)

// Run is one claim of a work item. Every commit through it checks the claim (P7.6).
type Run struct {
	Item
	job       *Job
	db        *db.DB
	epoch     int64
	gen, tl   int64
	completed bool
	cancel    context.CancelFunc
}

// guard is the claim check every commit carries: own epoch and claim epoch, the claim epoch still current.
const guard = `id = $1 AND state = 'running' AND epoch = $2 AND claim_db_generation = $3 AND claim_db_timeline = $4
	AND (claim_db_generation, claim_db_timeline) = (SELECT generation, timeline FROM be_current_db_epoch())`

func (r *Run) args() []any { return []any{r.ID, r.epoch, r.gen, r.tl} }

// exec runs one guarded update with optional business writes in the same transaction; zero rows means ErrLost.
func (r *Run) exec(ctx context.Context, set string, extra []any, fn func(context.Context, db.Tx) error) error {
	err := r.db.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE be_work_item SET "+set+" WHERE "+guard, append(r.args(), extra...)...)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrLost
		}
		if fn != nil {
			return fn(ctx, tx)
		}
		return nil
	})
	if err != nil && isLost(err) {
		if r.cancel != nil {
			r.cancel()
		}
		return ErrLost
	}
	return err
}

func isLost(err error) bool { return errors.Is(err, ErrLost) }

// Complete commits the item's effect and marks it done in one transaction (P7.10); fn may be nil. A non-idempotent
// call's unknown outcome is cleared with it. Singletons go back to pending for their next run.
func (r *Run) Complete(ctx context.Context, fn func(ctx context.Context, tx db.Tx) error) error {
	set := `state = 'done', outcome_unknown = false, waiting_since = NULL, holder = NULL, lease_until = NULL,
		finished_at = now()`
	var extra []any
	if r.Kind == Singleton {
		set = `state = 'pending', outcome_unknown = false, waiting_since = NULL, holder = NULL, lease_until = NULL,
			attempts = 0, progress = NULL, operation_id = $5, run_at = now() + make_interval(secs => $6)`
		extra = []any{ids.New().String(), r.job.Interval.Seconds()}
	}
	if err := r.exec(ctx, set, extra, fn); err != nil {
		return err
	}
	r.completed = true
	return nil
}

// BeforeCall marks a non-idempotent external call as possibly sent (P7.12): call it, and only after it returns
// make the call.
func (r *Run) BeforeCall(ctx context.Context) error {
	return r.exec(ctx, "outcome_unknown = true", nil, nil)
}

// SubKey is the idempotency key of one call within this run: the operation ID and a key chosen by meaning
// (P7.11).
func (r *Run) SubKey(name string) string { return r.OperationID + ":" + name }

// RecordCall stores a sub-call's result (unknown, effective, not_effective) before and after it is made.
func (r *Run) RecordCall(ctx context.Context, key, result string) error {
	b, _ := json.Marshal(map[string]string{key: result})
	if err := r.exec(ctx, "call_results = call_results || $5::jsonb", []any{string(b)}, nil); err != nil {
		return err
	}
	if r.CallResults == nil {
		r.CallResults = map[string]string{}
	}
	r.CallResults[key] = result
	return nil
}

// Checkpoint commits a singleton's progress with fn's writes and starts the next segment's operation ID (P7.17).
func (r *Run) Checkpoint(ctx context.Context, progress any, fn func(ctx context.Context, tx db.Tx) error) error {
	b, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	next := ids.New().String()
	if err := r.exec(ctx, "progress = $5::jsonb, operation_id = $6, call_results = '{}'", []any{string(b), next}, fn); err != nil {
		return err
	}
	r.Progress, r.OperationID, r.CallResults = b, next, map[string]string{}
	return nil
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
