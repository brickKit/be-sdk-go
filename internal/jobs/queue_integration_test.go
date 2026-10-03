package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/stretchr/testify/require"
)

// enqueue adds jobs in one business transaction; rollback fails it afterwards.
func enqueue(t *testing.T, eng *Engine, rollback bool, qs ...Enqueue) []string {
	t.Helper()
	var ids []string
	err := eng.cfg.Store.Run(within(t, 10*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		ids = ids[:0]
		for _, q := range qs {
			id, _, err := eng.Enqueue(ctx, tx, q)
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if rollback {
			return errors.New("business rule failed")
		}
		return nil
	})
	if rollback {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
	return ids
}

// CP-JOBS-03: a queued job runs once across two replicas.
func TestQueuedJobRunsOnceAcrossReplicas(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	seen := map[string]int{}
	w := Worker{Kind: "widget.notify", Concurrency: 2, Timeout: 5 * time.Second,
		Run: func(ctx context.Context, j QueuedJob) error {
			mu.Lock()
			seen[j.ID]++
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			return nil
		}}
	a := e.engine(t, "i1", Declarations{Workers: []Worker{w}})
	b := e.engine(t, "i2", Declarations{Workers: []Worker{w}})
	var qs []Enqueue
	for i := range 20 {
		qs = append(qs, Enqueue{Kind: "widget.notify", Args: map[string]int{"n": i}})
	}
	ids := enqueue(t, a, false, qs...)
	runLoops(t, a)
	runLoops(t, b)
	eventually(t, 10*time.Second, func() bool {
		return e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_queue WHERE state = 'done'`) == 20
	}, "every job done")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 20)
	for _, id := range ids {
		require.Equal(t, 1, seen[id], "job %s ran once", id)
		_, err := envelope.ParseID(id)
		require.NoError(t, err, "ids are UUIDv7")
	}
}

// CP-JOBS-04: a job enqueued in a rolled-back transaction does not exist; a unique key keeps one
// live job.
func TestEnqueueFollowsTheTransaction(t *testing.T) {
	e := newEnv(t)
	eng := e.engine(t, "i1", Declarations{Workers: []Worker{worker("widget.notify")}})
	enqueue(t, eng, true, Enqueue{Kind: "widget.notify", Args: map[string]string{"a": "b"}})
	require.Equal(t, 0, e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_queue`))

	enqueue(t, eng, false, Enqueue{Kind: "widget.notify", UniqueKey: "order-1"})
	ids := enqueue(t, eng, false, Enqueue{Kind: "widget.notify", UniqueKey: "order-1"})
	require.Equal(t, []string{""}, ids, "the second enqueue with a live key is a no-op")
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_queue WHERE unique_key = 'order-1'`))

	err := eng.cfg.Store.Run(within(t, 5*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		_, _, err := eng.Enqueue(ctx, tx, Enqueue{Kind: "widget.unknown"})
		return err
	})
	var pe *problem.Error
	require.True(t, errors.As(err, &pe), "a programming error: %v", err)
	require.Equal(t, "INTERNAL", pe.Reason)
	require.ErrorContains(t, pe.Cause, "no worker consumes")
}

// A failing job is retried through its row with the backoff and carries what it captured at enqueue.
func TestQueuedJobRetriedThenDone(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int32
	var mu sync.Mutex
	var got QueuedJob
	w := Worker{Kind: "widget.notify", MaxAttempts: 5, Backoff: []time.Duration{20 * time.Millisecond},
		Timeout: 5 * time.Second, Run: func(ctx context.Context, j QueuedJob) error {
			if calls.Add(1) < 3 {
				return fmt.Errorf("attempt %d failed", j.Attempt)
			}
			mu.Lock()
			got = j
			mu.Unlock()
			return nil
		}}
	eng := e.engine(t, "i1", Declarations{Workers: []Worker{w}})
	enqueue(t, eng, false, Enqueue{Kind: "widget.notify", Args: map[string]string{"order": "o-1"}, TraceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Origin: envelope.Origin{Kind: envelope.OriginEvent, CausationID: "0190a3c2-0000-7000-8000-000000000001", HopCount: 2}})
	runLoops(t, eng)
	eventually(t, 10*time.Second, func() bool {
		return e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_queue WHERE state = 'done'`) == 1
	}, "done after two failures")
	require.Equal(t, int32(3), calls.Load())
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 3, got.Attempt)
	require.Equal(t, "attempt 2 failed", got.LastError)
	require.Equal(t, "0190a3c2-0000-7000-8000-000000000001", got.CausationID)
	require.Equal(t, 3, got.HopCount, "enqueued from an event handler: the handled hop + 1 (P12.8)")
	require.Equal(t, "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", got.TraceParent)
	var args map[string]string
	require.NoError(t, got.Decode(&args))
	require.Equal(t, "o-1", args["order"])
}

// Exhausted attempts set dead and call OnDead in a transaction.
func TestDeadQueuedJobCallsOnDead(t *testing.T) {
	e := newEnv(t)
	w := Worker{Kind: "widget.notify", MaxAttempts: 2, Backoff: []time.Duration{20 * time.Millisecond},
		Timeout: 5 * time.Second,
		Run:     func(context.Context, QueuedJob) error { return errors.New("always fails") },
		OnDead: func(ctx context.Context, tx *pg.Tx, j QueuedJob) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO dead_log (job_id, attempts) VALUES ($1, $2)`, j.ID, j.Attempt)
			return err
		}}
	eng := e.engine(t, "i1", Declarations{Workers: []Worker{w}})
	ids := enqueue(t, eng, false, Enqueue{Kind: "widget.notify"})
	runLoops(t, eng)
	eventually(t, 10*time.Second, func() bool {
		return e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_queue WHERE state = 'dead'`) == 1
	}, "dead after two attempts")
	var attempts int
	var lastError string
	e.scan(t, `SELECT attempts, last_error FROM SCHEMA.besdk_job_queue`, &attempts, &lastError)
	require.Equal(t, 2, attempts)
	require.Equal(t, "always fails", lastError)
	e.scan(t, `SELECT attempts FROM SCHEMA.dead_log WHERE job_id = '`+ids[0]+`'`, &attempts)
	require.Equal(t, 2, attempts)
}

// P14.8: job run of a queue kind drains the ready rows once.
func TestRunOnceDrainsQueue(t *testing.T) {
	e := newEnv(t)
	var runs atomic.Int32
	w := Worker{Kind: "widget.notify", Concurrency: 2, Timeout: 5 * time.Second,
		Run: func(context.Context, QueuedJob) error { runs.Add(1); return nil }}
	eng := e.engine(t, "i1", Declarations{Workers: []Worker{w}})
	require.Equal(t, RunNoop, eng.RunOnce(within(t, 5*time.Second), "widget.notify").Result)
	enqueue(t, eng, false, Enqueue{Kind: "widget.notify"}, Enqueue{Kind: "widget.notify"}, Enqueue{Kind: "widget.notify"},
		Enqueue{Kind: "widget.notify", RunAt: time.Now().Add(time.Hour)})
	require.Equal(t, RunOK, eng.RunOnce(within(t, 5*time.Second), "widget.notify").Result)
	require.Equal(t, int32(3), runs.Load(), "the job due in an hour is not ready")
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_queue WHERE state = 'ready'`))
}

// Retention: done rows after 7 days, slot rows after 30 days, each job's newest slot kept.
func TestCleanup(t *testing.T) {
	e := newEnv(t)
	e.exec(t, `INSERT INTO SCHEMA.besdk_job_queue (id, kind, args, max_attempts, state, finished_at) VALUES
		('0190a3c2-0000-7000-8000-000000000001', 'k', '{}', 1, 'done', now() - interval '8 days'),
		('0190a3c2-0000-7000-8000-000000000002', 'k', '{}', 1, 'done', now() - interval '1 day'),
		('0190a3c2-0000-7000-8000-000000000003', 'k', '{}', 1, 'dead', now() - interval '8 days')`)
	e.exec(t, `INSERT INTO SCHEMA.besdk_job_slot (name, slot_at, holder) VALUES
		('x', now() - interval '40 days', 'h'), ('x', now() - interval '35 days', 'h'), ('y', now() - interval '40 days', 'h'),
		('x', now() - interval '1 day', 'h')`)
	res, err := Cleanup(within(t, 10*time.Second), e.store(t), Retention{Batch: 1})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.QueueRows)
	require.Equal(t, int64(2), res.SlotRows)
	require.Equal(t, 2, e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_queue`))
	require.Equal(t, 2, e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_slot`), "x's newest and y's only row stay")
}
