package jobs

import (
	"context"
	"errors"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// Outcome is what a reconciler's Handle found for one item (P14 "reconciler").
type Outcome struct {
	// Done: the item reached a terminal state; its besdk_reconcile row is deleted in Apply's
	// transaction. Not done: the item is looked at again after RetryAfter (or the backoff), and the
	// pass counts as an attempt.
	Done bool
	// State is what Handle observed ("CONFIRMED", "STILL_PENDING", …), for Apply and the logs.
	State string
	// Data is anything else Handle hands to Apply.
	Data any
	// RetryAfter, when not Done and > 0, replaces the backoff for the next look.
	RetryAfter time.Duration
}

// ReconcilerSpec declares a reconciler over items of type T (sdk-redesign-apis §2.9). Candidates
// runs in a short transaction, Handle outside any transaction (it may call other components), Apply
// and GiveUp each in a short transaction that re-checks the item's state machine.
type ReconcilerSpec[T any] struct {
	Name    string
	Every   time.Duration // time between passes, on every replica
	Batch   int           // candidates per pass; 0 = DefaultBatch
	Timeout time.Duration // required: one Handle call (P14.2)
	// Candidates is the component's own SQL: non-terminal items past their deadline. It may leave
	// out items backing off: AND NOT EXISTS (SELECT 1 FROM besdk_reconcile r WHERE r.name = '<Name>'
	// AND r.item_id = <id> AND r.next_at > now()).
	Candidates func(ctx context.Context, tx *pg.Tx, limit int) ([]T, error)
	ID         func(T) string
	// Since, optional, is when the item became stuck: be_reconcile_oldest_age_seconds.
	Since       func(T) time.Time
	Handle      func(ctx context.Context, item T) (Outcome, error)
	Apply       func(ctx context.Context, tx *pg.Tx, item T, out Outcome) error // optional
	MaxAttempts int                                                             // 0 = DefaultMaxAttempts
	Backoff     []time.Duration                                                 // nil = 1 s doubling to 5 min
	// GiveUp is called in a transaction past MaxAttempts: suspend the item and enqueue an exception
	// task. The besdk_reconcile row is deleted in the same transaction.
	GiveUp func(ctx context.Context, tx *pg.Tx, item T) error
}

// Reconciler is a ReconcilerSpec with its item type erased, ready for Declarations.Reconcilers.
type Reconciler struct {
	name           string
	every, timeout time.Duration
	batch, max     int
	backoff        []time.Duration
	candidates     func(ctx context.Context, tx *pg.Tx, limit int) ([]any, error)
	id             func(any) string
	since          func(any) (time.Time, bool)
	handle         func(ctx context.Context, item any) (Outcome, error)
	apply          func(ctx context.Context, tx *pg.Tx, item any, out Outcome) error
	giveUp         func(ctx context.Context, tx *pg.Tx, item any) error
	missing        string // the first required function left nil
}

// NewReconciler erases T (sdk-redesign-apis §2.9). Missing functions are reported when the engine
// is built (exit 78).
func NewReconciler[T any](s ReconcilerSpec[T]) *Reconciler {
	r := &Reconciler{name: s.Name, every: s.Every, timeout: s.Timeout, batch: s.Batch, max: s.MaxAttempts,
		backoff: s.Backoff}
	if r.batch <= 0 {
		r.batch = DefaultBatch
	}
	if r.max <= 0 {
		r.max = DefaultMaxAttempts
	}
	switch {
	case s.Candidates == nil:
		r.missing = "Candidates"
	case s.ID == nil:
		r.missing = "ID"
	case s.Handle == nil:
		r.missing = "Handle"
	case s.GiveUp == nil:
		r.missing = "GiveUp"
	}
	if r.missing != "" {
		return r
	}
	r.candidates = func(ctx context.Context, tx *pg.Tx, limit int) ([]any, error) {
		items, err := s.Candidates(ctx, tx, limit)
		out := make([]any, len(items))
		for i, it := range items {
			out[i] = it
		}
		return out, err
	}
	r.id = func(v any) string { return s.ID(v.(T)) }
	r.since = func(v any) (time.Time, bool) {
		if s.Since == nil {
			return time.Time{}, false
		}
		return s.Since(v.(T)), true
	}
	r.handle = func(ctx context.Context, v any) (Outcome, error) { return s.Handle(ctx, v.(T)) }
	r.apply = func(ctx context.Context, tx *pg.Tx, v any, out Outcome) error {
		if s.Apply == nil {
			return nil
		}
		return s.Apply(ctx, tx, v.(T), out)
	}
	r.giveUp = func(ctx context.Context, tx *pg.Tx, v any) error { return s.GiveUp(ctx, tx, v.(T)) }
	return r
}

// Name is the reconciler's name.
func (r *Reconciler) Name() string { return r.name }

func (r *Reconciler) check() error {
	if r.missing != "" {
		return errors.New(r.missing + " is required")
	}
	return nil
}
