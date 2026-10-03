package besdk

import (
	"context"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/jobs"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"go.opentelemetry.io/otel/propagation"
)

// JobKind is a scheduled job's kind (P14 "The five kinds"); queue workers and reconcilers are
// declared in Module.Workers and Module.Reconcilers.
type JobKind int

// Scheduled kinds.
const (
	Every     JobKind = JobKind(jobs.Every)     // every replica on its own timer; work claimed atomically
	Singleton JobKind = JobKind(jobs.Singleton) // one holder at a time (lease, epoch fencing token)
	Cron      JobKind = JobKind(jobs.Cron)      // each slot once across replicas
)

// Job is one scheduled job (P14.1). Every run has a timeout; at the timeout its context is cancelled.
type Job struct {
	Name     string        // unique in the component; JOBS_OVERRIDES and `job run` use it
	Kind     JobKind       //
	Interval time.Duration // Every and Singleton
	Cron     string        // Cron: five-field expression or "@every <duration>" (P14.6)
	TZ       string        // Cron: IANA zone; "" = BUSINESS_TIMEZONE
	Timeout  time.Duration // required
	Run      func(ctx context.Context) error
}

// QueuedJob is one queued job as a worker sees it.
type QueuedJob = jobs.QueuedJob

// Worker consumes one kind of queued job (P14 "queue"): Run outside any transaction, at least once,
// deduplicating by the unique key or a business key; OnDead in a transaction once the attempts are
// used up.
type Worker struct {
	Kind        string
	Concurrency int             // 0 = 1
	MaxAttempts int             // 0 = 10
	Backoff     []time.Duration // nil = 1 s doubling to 5 min
	Timeout     time.Duration   // required
	Run         func(ctx context.Context, j QueuedJob) error
	OnDead      func(ctx context.Context, tx *Tx, j QueuedJob) error
}

// EnqueueOptions shape one Enqueue.
type EnqueueOptions struct {
	RunAt     time.Time // zero = now
	UniqueKey string    // "" = none; else at most one live job of the kind per key
}

// Enqueue adds a job of a kind one of Module.Workers consumes, in this transaction (P14 "queue"): it
// exists only if the transaction commits; a live job with the same unique key makes it a no-op.
func (tx *Tx) Enqueue(ctx context.Context, kind string, args any, o EnqueueOptions) error {
	e := tx.rt.deps.jobs
	if e == nil {
		return fmt.Errorf("besdk: Enqueue %s: no Module.Workers consume queued jobs", kind)
	}
	c := propagation.MapCarrier{}
	tx.rt.tel.Propagator().Inject(ctx, c)
	_, _, err := e.Enqueue(ctx, tx.Tx, jobs.Enqueue{Kind: kind, Args: args, RunAt: o.RunAt, UniqueKey: o.UniqueKey,
		TraceParent: c.Get(envelope.HeaderTraceParent), Origin: originOf(ctx)})
	return err
}

// Outcome is what a reconciler's Handle found for one item: Done when it reached a terminal state.
type Outcome = jobs.Outcome

// ReconcilerSpec declares a reconciler over items of type T (P14 "reconciler"): Candidates is the
// component's own SQL for non-terminal items past their deadline; Handle runs outside any transaction
// and may call other components; Apply and GiveUp run in short transactions that re-check the state.
type ReconcilerSpec[T any] struct {
	Name        string
	Every       time.Duration
	Batch       int           // 0 = 100
	Timeout     time.Duration // required: one Handle call
	Candidates  func(ctx context.Context, tx *Tx, limit int) ([]T, error)
	ID          func(T) string
	Since       func(T) time.Time // optional: when the item became stuck (be_reconcile_oldest_age_seconds)
	Handle      func(ctx context.Context, item T) (Outcome, error)
	Apply       func(ctx context.Context, tx *Tx, item T, out Outcome) error
	MaxAttempts int // 0 = 10
	Backoff     []time.Duration
	GiveUp      func(ctx context.Context, tx *Tx, item T) error
}

// ReconcilerRunner is a declared reconciler, ready for Module.Reconcilers.
type ReconcilerRunner struct {
	bind func(rt *Runtime) *jobs.Reconciler
}

// NewReconciler turns a spec into a runner for Module.Reconcilers.
func NewReconciler[T any](s ReconcilerSpec[T]) ReconcilerRunner {
	return ReconcilerRunner{bind: func(rt *Runtime) *jobs.Reconciler {
		wrap := func(t *pg.Tx) *Tx { return &Tx{Tx: t, rt: rt} }
		js := jobs.ReconcilerSpec[T]{Name: s.Name, Every: s.Every, Batch: s.Batch, Timeout: s.Timeout,
			ID: s.ID, Since: s.Since, Handle: s.Handle, MaxAttempts: s.MaxAttempts, Backoff: s.Backoff}
		if s.Candidates != nil {
			js.Candidates = func(ctx context.Context, tx *pg.Tx, n int) ([]T, error) { return s.Candidates(ctx, wrap(tx), n) }
		}
		if s.Apply != nil {
			js.Apply = func(ctx context.Context, tx *pg.Tx, it T, o Outcome) error { return s.Apply(ctx, wrap(tx), it, o) }
		}
		if s.GiveUp != nil {
			js.GiveUp = func(ctx context.Context, tx *pg.Tx, it T) error { return s.GiveUp(ctx, wrap(tx), it) }
		}
		return jobs.NewReconciler(js)
	}}
}

// JobEpoch is the singleton lease's epoch of the running job, the fencing token (P14 "singleton").
func JobEpoch(ctx context.Context) (int64, bool) { return jobs.Epoch(ctx) }
