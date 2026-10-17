//go:build integration

package work

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func replica(d *db.DB, lease time.Duration, jobs ...Job) *Runner {
	r := NewRunner(d, "demo/work", quiet())
	r.Lease, r.Poll, r.Concurrency = lease, 30*time.Millisecond, 2
	if err := r.Add(jobs...); err != nil {
		panic(err)
	}
	return r
}

func state(t *testing.T, admin *pgx.Conn, id string) (st string, attempts int, epoch int64, reason string) {
	var r *string
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT state, attempts, epoch, pause_reason FROM be_work_item WHERE id = $1", id).
		Scan(&st, &attempts, &epoch, &r))
	if r != nil {
		reason = *r
	}
	return
}

func enqueue(t *testing.T, d *db.DB, r *Runner, job string) string {
	var id string
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		var err error
		id, err = r.Enqueue(ctx, tx, job, map[string]int{"n": 1})
		return err
	}))
	return id
}

func TestTwoReplicasRunACronSlotOnce(t *testing.T) { // P7.9, RT08
	d, admin := dbtest.Schema(t, "work1", nil)
	var runs atomic.Int32
	job := Job{Name: "daily", Kind: Cron, Effect: Local, Cron: "0 2 * * *", TimeZone: "Europe/Berlin",
		Handle: func(ctx context.Context, r *Run) error {
			runs.Add(1)
			time.Sleep(50 * time.Millisecond)
			return r.Complete(ctx, func(ctx context.Context, tx db.Tx) error {
				_, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1)", ids.New())
				return err
			})
		}}
	a, b := replica(d, 5*time.Second, job), replica(d, 5*time.Second, job)
	pgtest.Exec(t, admin, "INSERT INTO be_cron_watermark (job, watermark_at) VALUES ('daily', '2026-10-16T23:59:00Z')")
	now := time.Date(2026, 10, 17, 0, 0, 30, 0, time.UTC) // 02:00 Berlin was 30 s ago
	var wg sync.WaitGroup
	for _, r := range []*Runner{a, b} {
		wg.Add(1)
		go func(r *Runner) { defer wg.Done(); assert.NoError(t, r.Materialize(context.Background(), "daily", now)) }(r)
	}
	wg.Wait()
	var items int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_work_item").Scan(&items))
	require.Equal(t, 1, items)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, a.Start(ctx))
	require.NoError(t, b.Start(ctx))
	require.Eventually(t, func() bool { s, _, _, _ := state(t, admin, "daily@2026-10-17T02:00"); return s == "done" }, 10*time.Second, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	cancel()
	a.Stop()
	b.Stop()
	assert.Equal(t, int32(1), runs.Load())
	var effects int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM items").Scan(&effects))
	assert.Equal(t, 1, effects)
}

func TestStopReleasesTheLeaseForAnotherReplica(t *testing.T) { // P7.8, RT07
	d, admin := dbtest.Schema(t, "work2", nil)
	started := make(chan struct{}, 2)
	var completedBy atomic.Value
	mk := func(name string) Job {
		return Job{Name: "export", Kind: Queue, Effect: Local, Handle: func(ctx context.Context, r *Run) error {
			started <- struct{}{}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(300 * time.Millisecond):
			}
			completedBy.Store(name)
			return r.Complete(ctx, nil)
		}}
	}
	a := replica(d, 30*time.Second, mk("a"))
	id := enqueue(t, d, a, "export")
	actx, stopA := context.WithCancel(context.Background())
	require.NoError(t, a.Start(actx))
	<-started
	stopA()
	a.Stop()
	st, attempts, _, _ := state(t, admin, id)
	assert.Equal(t, "running", st)
	assert.Equal(t, 0, attempts, "a stop does not spend an attempt")

	b := replica(d, 30*time.Second, mk("b"))
	bctx, stopB := context.WithCancel(context.Background())
	defer func() { stopB(); b.Stop() }()
	begin := time.Now()
	require.NoError(t, b.Start(bctx))
	require.Eventually(t, func() bool { s, _, _, _ := state(t, admin, id); return s == "done" }, 5*time.Second, 20*time.Millisecond)
	assert.Less(t, time.Since(begin), 5*time.Second, "taken over without waiting for the 30 s lease")
	assert.Equal(t, "b", completedBy.Load())
}

func TestATakenOverHolderCanCommitNothing(t *testing.T) { // P7.6, CK-WK-02, CK-WK-03
	d, admin := dbtest.Schema(t, "work3", nil)
	job := Job{Name: "q", Kind: Queue, Effect: Local, Handle: func(context.Context, *Run) error { return nil }}
	a, b := replica(d, time.Second, job), replica(d, time.Second, job)
	id := enqueue(t, d, a, "q")
	ra, err := a.claim(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ra)
	time.Sleep(1100 * time.Millisecond) // A stalls past its lease
	rb, err := b.claim(context.Background())
	require.NoError(t, err)
	require.NotNil(t, rb, "taken over")
	assert.Equal(t, ra.ID, rb.ID)
	assert.Equal(t, ra.epoch+1, rb.epoch)
	assert.Equal(t, ra.OperationID, rb.OperationID, "the operation ID is kept")
	assert.ErrorIs(t, ra.Complete(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1)", ids.New())
		return err
	}), ErrLost)
	var effects int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM items").Scan(&effects))
	assert.Zero(t, effects, "the stale holder's effect rolled back with its refused commit")

	pgtest.Exec(t, admin, "UPDATE be_recovery_generation SET generation = generation + 1")
	assert.ErrorIs(t, rb.Complete(context.Background(), nil), ErrLost, "a new database epoch invalidates older claims")
	st, _, _, _ := state(t, admin, id)
	assert.Equal(t, "running", st)
}

func TestNonIdempotentCallsAreNeverSentTwice(t *testing.T) { // P7.12, CK-WK-07
	d, admin := dbtest.Schema(t, "work4", nil)
	var sent atomic.Int32
	job := Job{Name: "pay", Kind: Queue, Effect: NonIdempotentExternal, Handle: func(ctx context.Context, r *Run) error {
		if err := r.BeforeCall(ctx); err != nil {
			return err
		}
		sent.Add(1)
		return errors.New("timeout: no answer") // cannot prove it did not take effect
	}}
	r := replica(d, 5*time.Second, job)
	id := enqueue(t, d, r, "pay")
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(ctx))
	require.Eventually(t, func() bool { s, _, _, _ := state(t, admin, id); return s == "paused" }, 5*time.Second, 20*time.Millisecond)
	cancel()
	r.Stop()
	_, _, _, reason := state(t, admin, id)
	assert.Equal(t, "outcome_unknown", reason)
	assert.Equal(t, int32(1), sent.Load())
	assert.ErrorIs(t, Requeue(context.Background(), d, id, false), ErrRefused, "not without the operator's verification")
	require.NoError(t, Requeue(context.Background(), d, id, true))
	st, attempts, _, _ := state(t, admin, id)
	assert.Equal(t, "pending", st)
	assert.Zero(t, attempts)
	require.NoError(t, Hold(context.Background(), d, id)) // a business check keeps it from running below
	assert.ErrorIs(t, Hold(context.Background(), d, id), ErrRefused)

	// a holder that crashed after marking: the next claim pauses without calling
	id2 := enqueue(t, d, r, "pay")
	run, err := r.claim(context.Background())
	require.NoError(t, err)
	require.Equal(t, id2, run.ID)
	require.NoError(t, run.BeforeCall(context.Background()))
	pgtest.Exec(t, admin, "UPDATE be_work_item SET lease_until = now() - interval '1 second' WHERE id = '"+id2+"'")
	again, err := r.claim(context.Background())
	require.NoError(t, err)
	if again != nil {
		assert.NotEqual(t, id2, again.ID)
	}
	st, _, _, reason = state(t, admin, id2)
	assert.Equal(t, "paused", st)
	assert.Equal(t, "outcome_unknown", reason)
}

func TestRetriesThenDeadWithItsDeadLetter(t *testing.T) { // P7.13, CK-WK-08
	d, admin := dbtest.Schema(t, "work5", nil)
	var calls atomic.Int32
	job := Job{Name: "flaky", Kind: Queue, Effect: Local, MaxAttempts: 2,
		Handle: func(context.Context, *Run) error { calls.Add(1); return errors.New("still broken") },
		DeadLetter: func(ctx context.Context, tx db.Tx, item Item, cause error) error {
			_, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1)", ids.New())
			return err
		}}
	r := replica(d, 5*time.Second, job)
	id := enqueue(t, d, r, "flaky")
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(ctx))
	require.Eventually(t, func() bool { s, _, _, _ := state(t, admin, id); return s == "dead" }, 10*time.Second, 20*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	cancel()
	r.Stop()
	assert.Equal(t, int32(2), calls.Load(), "two attempts, the first retry after a 1 s backoff, then no more")
	var letters int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM items").Scan(&letters))
	assert.Equal(t, 1, letters, "the dead letter handler ran in the transaction that marked it dead")

	// rollback of the business transaction: no item
	err := d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := r.Enqueue(ctx, tx, "flaky", nil)
		require.NoError(t, err)
		return errors.New("business rollback")
	})
	require.Error(t, err)
	var items int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_work_item").Scan(&items))
	assert.Equal(t, 1, items)
}

func TestASingletonContinuesFromItsCommittedProgress(t *testing.T) { // P7.17, CK-WK-09
	d, admin := dbtest.Schema(t, "work6", nil)
	var seen []int
	var ops []string
	var mu sync.Mutex
	job := Job{Name: "partitions", Kind: Singleton, Effect: Local, Interval: time.Hour,
		Handle: func(ctx context.Context, r *Run) error {
			var done int
			if r.Progress != nil {
				_ = jsonUnmarshal(r.Progress, &done)
			}
			for step := done + 1; step <= 3; step++ {
				mu.Lock()
				seen, ops = append(seen, step), append(ops, r.OperationID)
				mu.Unlock()
				if err := r.Checkpoint(ctx, step, nil); err != nil {
					return err
				}
			}
			return nil
		}}
	a := replica(d, time.Second, job)
	require.NoError(t, a.ensureSingleton(context.Background(), a.jobs["partitions"]))
	run, err := a.claim(context.Background())
	require.NoError(t, err)
	// replica A commits step 1 by hand, then stalls past its lease before step 2
	require.NoError(t, run.Checkpoint(context.Background(), 1, nil))
	segment2 := run.OperationID
	pgtest.Exec(t, admin, "UPDATE be_work_item SET lease_until = now() - interval '1 second' WHERE id = 'partitions'")
	b := replica(d, time.Second, job)
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, b.Start(ctx))
	require.Eventually(t, func() bool { s, _, _, _ := state(t, admin, "partitions"); return s == "pending" }, 5*time.Second, 20*time.Millisecond)
	cancel()
	b.Stop()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int{2, 3}, seen, "continued after the committed step 1")
	assert.Equal(t, segment2, ops[0], "the segment's operation ID survives the takeover")
	var runAt time.Time
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT run_at FROM be_work_item WHERE id = 'partitions'").Scan(&runAt))
	assert.WithinDuration(t, time.Now().Add(time.Hour), runAt, time.Minute, "next run after the interval")
}
