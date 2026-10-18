package work

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
)

// Runner runs a member's jobs: the scheduler (cron slots, singletons), the workers that claim items, and the
// per-replica timers of Every jobs.
type Runner struct {
	DB          *db.DB
	Holder      string // <component ID>/<instance ID>
	Log         *slog.Logger
	Lease       time.Duration
	Poll        time.Duration
	Concurrency int

	jobs    map[string]*Job
	names   []string
	mu      sync.Mutex
	running map[string]*Run
	ready   chan struct{} // one signal per item a commit enqueued, at most one per worker (P7.21)
	wg      sync.WaitGroup
	stopped chan struct{}
}

// NewRunner creates a member's runner; jobs are added before Start.
func NewRunner(d *db.DB, member string, log *slog.Logger) *Runner {
	return &Runner{DB: d, Holder: member + "/" + ids.New().String(), Log: log, Lease: DefaultLease,
		Poll: 500 * time.Millisecond, Concurrency: 4, jobs: map[string]*Job{}, running: map[string]*Run{},
		ready: make(chan struct{}, 4)}
}

// Add declares jobs (the module's Jobs).
func (w *Runner) Add(jobs ...Job) error {
	for _, j := range jobs {
		j := j
		if err := j.prepare(); err != nil {
			return err
		}
		if _, dup := w.jobs[j.Name]; dup {
			return fmt.Errorf("work: job %s declared twice", j.Name)
		}
		w.jobs[j.Name] = &j
		if j.Kind != Every {
			w.names = append(w.names, j.Name)
		}
	}
	return nil
}

// AddAll declares every job a module listed.
func (w *Runner) AddAll(j *Jobs) error { return w.Add(j.list...) }

// Enqueue puts a queue job's item into the business transaction (P7.3) and returns its ID.
func (w *Runner) Enqueue(ctx context.Context, tx db.Tx, job string, args any) (string, error) {
	j, ok := w.jobs[job]
	if !ok || j.Kind != Queue {
		return "", fmt.Errorf("work: %s is not a declared queue job", job)
	}
	b, err := json.Marshal(args)
	if err != nil {
		return "", err
	}
	id := ids.New().String()
	_, err = tx.Exec(ctx, `INSERT INTO be_work_item (id, job, kind, state, max_attempts, effect_class, operation_id, args)
		VALUES ($1, $2, 'queue', 'pending', $3, $4, $5, $6::jsonb)`, id, job, j.MaxAttempts, string(j.Effect), id+":"+job, string(b))
	if err == nil {
		tx.AfterCommit(w.signal)
	}
	return id, err
}

// signal wakes an idle worker once a commit that enqueued an item is visible (P7.21): without it every item waits half
// a poll (measured: event arrival to the job's call p50 250 ms, p99 500 ms at a 500 ms poll). A full channel means every
// worker is already woken; the poll covers other replicas' items, retries and takeovers.
func (w *Runner) signal() {
	select {
	case w.ready <- struct{}{}:
	default:
	}
}

// Start runs everything until ctx ends; Stop then waits for the handlers and releases their leases.
func (w *Runner) Start(ctx context.Context) error {
	w.stopped = make(chan struct{})
	if cap(w.ready) != w.Concurrency {
		w.ready = make(chan struct{}, max(w.Concurrency, 1))
	}
	for _, name := range w.names {
		if j := w.jobs[name]; j.Kind == Singleton {
			if err := w.ensureSingleton(ctx, j); err != nil {
				return err
			}
		}
	}
	w.wg.Add(1)
	go func() { defer w.wg.Done(); w.loop(ctx, time.Second, nil, w.schedule) }()
	for i := 0; i < w.Concurrency; i++ {
		w.wg.Add(1)
		go func() { defer w.wg.Done(); w.loop(ctx, w.Poll, w.ready, w.claimAndRun) }()
	}
	for _, j := range w.jobs {
		if j.Kind == Every {
			j := j
			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				w.loop(ctx, j.Interval, nil, func(ctx context.Context) bool {
					ctx, cancel := context.WithTimeout(ctx, j.Timeout)
					defer cancel()
					if err := safely(func() error { return j.Handle(ctx, nil) }); err != nil && ctx.Err() == nil {
						w.Log.Warn("an every job failed", "job", j.Name, "error", err.Error())
					}
					return false
				})
			}()
		}
	}
	return nil
}

// Stop waits for the loops to end (their context is already cancelled).
func (w *Runner) Stop() { w.wg.Wait() }

// loop runs fn at once, then every interval, at once again while it finds more, and whenever wake signals (nil: never).
func (w *Runner) loop(ctx context.Context, every time.Duration, wake <-chan struct{}, fn func(context.Context) bool) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-wake:
		}
		if fn(ctx) {
			t.Reset(0) // there may be more
		} else {
			t.Reset(every)
		}
	}
}

func (w *Runner) ensureSingleton(ctx context.Context, j *Job) error {
	return w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO be_work_item (id, job, kind, state, max_attempts, effect_class, operation_id)
			VALUES ($1, $1, 'singleton', 'pending', $2, $3, $4) ON CONFLICT DO NOTHING`,
			j.Name, j.MaxAttempts, string(j.Effect), ids.New().String())
		return err
	})
}

// schedule materializes due cron slots under each job's watermark lock (P7.16).
func (w *Runner) schedule(ctx context.Context) bool {
	for _, name := range w.names {
		if j := w.jobs[name]; j.Kind == Cron {
			if err := w.Materialize(ctx, j.Name, time.Time{}); err != nil && ctx.Err() == nil {
				w.Log.Warn("materializing cron slots failed", "job", j.Name, "error", err.Error())
			}
		}
	}
	return false
}

// Materialize turns the due slots of a cron job into work items; now zero means the database's clock.
func (w *Runner) Materialize(ctx context.Context, job string, now time.Time) error {
	j := w.jobs[job]
	return w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if now.IsZero() {
			if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO be_cron_watermark (job, watermark_at) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			j.Name, now); err != nil {
			return err
		}
		var wm time.Time
		if err := tx.QueryRow(ctx, "SELECT watermark_at FROM be_cron_watermark WHERE job = $1 FOR UPDATE", j.Name).Scan(&wm); err != nil {
			return err
		}
		pending, paused, next := Plan(j.schedule, j.loc, j.Backfill, j.MaxBacklog, wm, now)
		for _, group := range []struct {
			slots []Slot
			state string
		}{{pending, "pending"}, {paused, "paused"}} {
			for _, s := range group.slots {
				var reason any
				if group.state == "paused" {
					reason = "backlog_exceeded"
				}
				if _, err := tx.Exec(ctx, `INSERT INTO be_work_item (id, job, kind, state, pause_reason, max_attempts,
					effect_class, operation_id, slot, run_at) VALUES ($1, $2, 'cron', $3, $4, $5, $6, $7, $8, $9)
					ON CONFLICT DO NOTHING`, j.Name+"@"+s.ID, j.Name, group.state, reason, j.MaxAttempts, string(j.Effect),
					ids.New().String(), s.ID, s.At); err != nil {
					return err
				}
			}
		}
		if next.Equal(wm) {
			return nil
		}
		last := any(nil)
		if n := len(pending) + len(paused); n > 0 {
			all := append(append([]Slot{}, pending...), paused...)
			last = all[len(all)-1].ID
		}
		_, err := tx.Exec(ctx, "UPDATE be_cron_watermark SET watermark_at = $2, last_slot = coalesce($3, last_slot) WHERE job = $1",
			j.Name, next, last)
		return err
	})
}

// claimAndRun claims one due item (P7.5) and runs it; it reports whether one was found.
func (w *Runner) claimAndRun(ctx context.Context) bool {
	r, err := w.claim(ctx)
	if err != nil {
		if ctx.Err() == nil {
			w.Log.Warn("claiming work failed", "error", err.Error())
		}
		return false
	}
	if r == nil {
		return false
	}
	w.run(ctx, r)
	return true
}

func (w *Runner) claim(ctx context.Context) (*Run, error) {
	if len(w.names) == 0 {
		return nil, nil
	}
	var r *Run
	err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		r = nil
		var kind, effect string
		var unknown bool
		var calls []byte
		row := &Run{}
		err := tx.QueryRow(ctx, `WITH c AS (
				SELECT id FROM be_work_item WHERE job = ANY($1) AND (
					(state IN ('pending', 'retry_wait') AND run_at <= now()) OR (state = 'running' AND lease_until <= now()))
				ORDER BY run_at LIMIT 1 FOR UPDATE SKIP LOCKED)
			UPDATE be_work_item w SET state = 'running', epoch = w.epoch + 1, attempts = w.attempts + 1, holder = $2,
				lease_until = now() + make_interval(secs => $3), claim_db_generation = e.generation,
				claim_db_timeline = e.timeline
			FROM c, be_current_db_epoch() e WHERE w.id = c.id
			RETURNING w.id, w.job, w.kind, w.epoch, w.claim_db_generation, w.claim_db_timeline, w.attempts,
				w.max_attempts, w.effect_class, w.operation_id, w.outcome_unknown, w.args, w.progress,
				coalesce(w.slot, ''), w.call_results`, w.names, w.Holder, w.Lease.Seconds()).
			Scan(&row.ID, &row.Item.Job, &kind, &row.epoch, &row.gen, &row.tl, &row.Attempts, &row.MaxAttempts, &effect,
				&row.OperationID, &unknown, &row.Args, &row.Progress, &row.Slot, &calls)
		if err != nil {
			if isNoRows(err) {
				return nil
			}
			return err
		}
		row.Kind = Kind(kind)
		_ = json.Unmarshal(calls, &row.CallResults)
		row.job, row.db = w.jobs[row.Item.Job], w.DB
		if Effect(effect) == NonIdempotentExternal && unknown {
			// a call may have been sent by an earlier holder: never send again (P7.12)
			_, err := tx.Exec(ctx, `UPDATE be_work_item SET state = 'paused', pause_reason = 'outcome_unknown',
				holder = NULL, lease_until = NULL WHERE id = $1`, row.ID)
			w.Log.Error("a non-idempotent call's outcome is unknown; the item is paused for an operator", "job",
				row.Item.Job, "item", row.ID)
			return err
		}
		r = row
		return nil
	})
	return r, err
}

func (w *Runner) run(parent context.Context, r *Run) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), r.job.Timeout)
	r.cancel = cancel
	defer cancel()
	stopping := make(chan struct{})
	go func() { // stop: cancel the handler; lease renewal: every third of the lease
		t := time.NewTicker(w.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-parent.Done():
				close(stopping)
				cancel()
				return
			case <-t.C:
				if err := r.exec(ctx, "lease_until = now() + make_interval(secs => $5)", []any{w.Lease.Seconds()}, nil); err != nil {
					cancel() // taken over or the epoch changed: stop at once (P7.6)
					return
				}
			}
		}
	}()
	err := safely(func() error { return r.job.Handle(ctx, r) })
	select {
	case <-stopping: // P7.8: release the lease and give the attempt back
		_ = r.exec(context.Background(), "lease_until = now(), attempts = attempts - 1", nil, nil)
		return
	default:
	}
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	if errors.Is(err, ErrLost) || (ctx.Err() != nil && !timedOut && !r.completed) {
		return // taken over or the epoch changed: nothing more to commit
	}
	if timedOut && !r.completed {
		err = fmt.Errorf("the run passed its %s deadline: %w", r.job.Timeout, context.DeadlineExceeded)
	}
	bg := context.Background()
	switch {
	case err == nil && !r.completed:
		if cerr := r.Complete(bg, nil); cerr != nil && !errors.Is(cerr, ErrLost) {
			w.Log.Warn("completing a work item failed", "job", r.Item.Job, "item", r.ID, "error", cerr.Error())
		}
	case err == nil:
	default:
		w.fail(bg, r, err)
	}
}

func (w *Runner) fail(ctx context.Context, r *Run, cause error) {
	msg := cause.Error()
	switch k := kindOf(cause); {
	case k == "wait":
		set := `state = 'retry_wait', attempts = attempts - 1, holder = NULL, lease_until = NULL, last_error = $5,
			waiting_since = coalesce(waiting_since, now()), run_at = now() + make_interval(secs => $6)`
		if r.job.MaxWait > 0 {
			var waiting bool
			_ = w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
				return tx.QueryRow(ctx, "SELECT coalesce(waiting_since < now() - make_interval(secs => $2), false) FROM be_work_item WHERE id = $1",
					r.ID, r.job.MaxWait.Seconds()).Scan(&waiting)
			})
			if waiting {
				w.dead(ctx, r, fmt.Errorf("waited longer than %s: %w", r.job.MaxWait, cause))
				return
			}
		}
		_ = r.exec(ctx, set, []any{msg, backoff(r.Attempts).Seconds()}, nil)
	case k == "permanent":
		w.dead(ctx, r, cause)
	case r.job.Effect == NonIdempotentExternal && k != "not_effective" && w.unknown(ctx, r):
		_ = r.exec(ctx, `state = 'paused', pause_reason = 'outcome_unknown', holder = NULL, lease_until = NULL, last_error = $5`,
			[]any{msg}, nil)
		w.Log.Error("a non-idempotent call failed without proof it did not take effect; paused", "job", r.Item.Job, "item", r.ID)
	case r.Attempts >= r.MaxAttempts:
		w.dead(ctx, r, cause)
	default:
		w.Log.Warn("a work item failed; it is retried after a backoff", "job", r.Item.Job, "item", r.ID,
			"attempt", r.Attempts, "error", msg)
		_ = r.exec(ctx, `state = 'retry_wait', outcome_unknown = false, holder = NULL, lease_until = NULL, last_error = $5,
			run_at = now() + make_interval(secs => $6)`, []any{msg, backoff(r.Attempts).Seconds()}, nil)
	}
}

func (w *Runner) unknown(ctx context.Context, r *Run) bool {
	var u bool
	_ = w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT outcome_unknown FROM be_work_item WHERE id = $1", r.ID).Scan(&u)
	})
	return u
}

func (w *Runner) dead(ctx context.Context, r *Run, cause error) {
	err := r.exec(ctx, `state = 'dead', outcome_unknown = false, holder = NULL, lease_until = NULL, last_error = $5,
		finished_at = now()`, []any{cause.Error()}, func(ctx context.Context, tx db.Tx) error {
		if r.job.DeadLetter != nil {
			return r.job.DeadLetter(ctx, tx, r.Item, cause)
		}
		return nil
	})
	if err != nil && !errors.Is(err, ErrLost) {
		w.Log.Error("marking a work item dead failed", "job", r.Item.Job, "item", r.ID, "error", err.Error())
		return
	}
	w.Log.Error("a work item is dead; an operator decides", "job", r.Item.Job, "item", r.ID, "error", cause.Error())
}

// safely runs a handler, turning a panic into an error: a job never takes the process down.
func safely(fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v\n%s", p, debug.Stack())
		}
	}()
	return fn()
}
