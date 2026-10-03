package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
)

// statsEvery is how often a worker refreshes its queue gauges.
const statsEvery = 5 * time.Second

// Enqueue is one job to enqueue (sdk-redesign-apis §2.9 tx.Enqueue).
type Enqueue struct {
	Kind        string          // a kind one of the member's Workers consumes
	Args        any             // marshalled to JSON; json.RawMessage and []byte are taken as they are
	RunAt       time.Time       // zero = now
	UniqueKey   string          // "" = none; else one live job per kind and key
	TraceParent string          // the current span's traceparent, "" = none (P18.1)
	Origin      envelope.Origin // what the job is enqueued from: its causation and hop (P12.8)
}

// Enqueue inserts a job in the caller's business transaction (P14 "queue"): it exists if and only if
// the transaction commits. enqueued is false when a live job of the same kind and unique key already
// exists. An unknown kind or unmarshalable arguments are a programming error (INTERNAL); a database
// error is returned wrapped so Store.Run classifies it by SQLSTATE.
func (e *Engine) Enqueue(ctx context.Context, tx *pg.Tx, q Enqueue) (id string, enqueued bool, err error) {
	en, ok := e.plan.byName[q.Kind]
	if !ok || en.kind != kindQueue {
		return "", false, programming("enqueue: no worker consumes kind %q", q.Kind)
	}
	if tx == nil {
		return "", false, programming("enqueue %s: needs the business transaction", q.Kind)
	}
	args, err := marshalArgs(q.Args)
	if err != nil {
		return "", false, programming("enqueue %s: marshal args: %v", q.Kind, err)
	}
	var uniqueKey, runAt any
	if q.UniqueKey != "" {
		uniqueKey = q.UniqueKey
	}
	if !q.RunAt.IsZero() {
		runAt = q.RunAt
	}
	causation, hop := envelope.EnqueueContext(q.Origin)
	newID := envelope.NewID().String()
	res, err := tx.ExecContext(ctx, enqueueSQL, newID, q.Kind, string(args), uniqueKey, runAt,
		en.worker.MaxAttempts, q.TraceParent, causation, hop)
	if err != nil {
		return "", false, fmt.Errorf("jobs: enqueue %s: %w", q.Kind, err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return "", false, nil
	}
	return newID, true, nil
}

func marshalArgs(v any) ([]byte, error) {
	switch a := v.(type) {
	case nil:
		return []byte("{}"), nil
	case json.RawMessage:
		return a, nil
	case []byte:
		return a, nil
	}
	return json.Marshal(v)
}

// programming is a programming error of the component: INTERNAL with a clear cause (P4.3).
func programming(format string, args ...any) error {
	return problem.Wrap(fmt.Errorf("jobs: "+format, args...), "INTERNAL", nil)
}

// workerLoop consumes one kind (P14 "queue"): it claims up to the free concurrency, runs each job in
// its own goroutine (scoped to this call), polls every Poll while idle and refreshes the gauges. A
// failing job is retried through its row, never by ending the loop; a database failure ends the loop
// with an error (P1.7). On return every handler has finished.
func (e *Engine) workerLoop(en *entry) func(ctx context.Context) error {
	w := en.worker
	return func(ctx context.Context) error {
		slots := make(chan struct{}, w.Concurrency)
		freed := make(chan struct{}, 1)
		var wg sync.WaitGroup
		defer wg.Wait()
		var lastStats time.Time
		for {
			if time.Since(lastStats) >= statsEvery {
				e.refreshQueueGauges(ctx, w.Kind)
				lastStats = time.Now()
			}
			free := w.Concurrency - len(slots)
			var jobs []QueuedJob
			if free > 0 {
				var err error
				if jobs, err = e.claimQueue(ctx, w, free, true); err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("worker %s: claim: %w", w.Kind, err)
				}
			}
			for _, j := range jobs {
				slots <- struct{}{}
				wg.Add(1)
				go func(j QueuedJob) {
					defer wg.Done()
					e.process(ctx, en, j, false)
					<-slots
					select {
					case freed <- struct{}{}:
					default:
					}
				}(j)
			}
			if len(jobs) > 0 && len(jobs) == free {
				continue // a full claim: there may be more right away once a slot frees
			}
			if !e.idle(ctx, freed, free > 0 && len(jobs) == 0) {
				return nil
			}
		}
	}
}

// idle waits for the next claim: Poll when the queue had nothing due (empty), else until a handler
// finishes or Poll passes. false when ctx ended.
func (e *Engine) idle(ctx context.Context, freed <-chan struct{}, empty bool) bool {
	t := time.NewTimer(e.cfg.poll())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		case <-freed:
			if !empty {
				return true
			}
		}
	}
}

// process runs one claimed job and records its outcome (decision tree):
//   - attempts already past the maximum (a crashed run used the last one): dead, without running;
//   - success: done;
//   - the process is stopping: the row goes back to ready without using the attempt;
//   - failure with attempts left: ready again at now + backoff;
//   - failure on the last attempt: dead, and OnDead in the same transaction.
//
// It returns the run's result and error ("" when it did not run).
func (e *Engine) process(ctx context.Context, en *entry, j QueuedJob, oneShot bool) (string, error) {
	w := en.worker
	if j.Attempt > j.MaxAttempts {
		dctx, cancel := detached(ctx)
		defer cancel()
		e.bury(dctx, w, j, "attempts used up: the last run's lease expired")
		return ResultError, fmt.Errorf("job %s: attempts used up", j.ID)
	}
	info := e.info(en, oneShot)
	info.Queued = &j
	res, err := e.exec(ctx, info, w.Timeout, func(ctx context.Context) error { return w.Run(ctx, j) })
	dctx, cancel := detached(ctx)
	defer cancel()
	var markErr error
	switch {
	case res == ResultOK:
		markErr = e.exec1(dctx, queueDone, j.ID, j.Attempt)
	case res == ResultCancelled:
		markErr = e.exec1(dctx, queueRelease, j.ID, j.Attempt)
	case j.Attempt >= j.MaxAttempts:
		e.bury(dctx, w, j, errorText(err))
	default:
		markErr = e.exec1(dctx, queueRetry, j.ID, j.Attempt, millis(backoffFor(w.Backoff, j.Attempt)), errorText(err))
	}
	if markErr != nil {
		e.log.Warn("queued job outcome not recorded; the row is re-claimed after its lease", slog.String("job", w.Kind),
			slog.String("id", j.ID), slog.String("error", errorText(markErr)))
	}
	return res, err
}

// bury marks the job dead and calls OnDead in the same transaction; when OnDead fails the
// transaction rolls back and the row is buried again once its lease expires.
func (e *Engine) bury(ctx context.Context, w *Worker, j QueuedJob, lastError string) {
	err := e.tx(ctx, func(ctx context.Context, tx *pg.Tx) error {
		res, err := tx.ExecContext(ctx, queueDead, j.ID, j.Attempt, lastError)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n == 0 || w.OnDead == nil {
			return err
		}
		return w.OnDead(ctx, tx, j)
	})
	if err != nil {
		e.log.Error("dead queued job not recorded; retried after its lease", slog.String("job", w.Kind),
			slog.String("id", j.ID), slog.String("error", errorText(err)))
		return
	}
	e.log.Warn("queued job is dead: attempts used up", slog.String("job", w.Kind), slog.String("id", j.ID),
		slog.Int("attempts", j.Attempt), slog.String("last_error", lastError))
}
