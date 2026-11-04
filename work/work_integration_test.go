//go:build integration

package work

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/google/uuid"
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

func TestTwoReplicasRunACronSlotOnce(t *testing.T) { // P7.9, RT08, P7.1
	d, admin := dbtest.Schema(t, "work1", nil)
	// runs per slot: once started, the replicas also materialize the slots due by the database's clock (any day's
	// 02:00 Berlin after the one this test plants), so the property is "every slot runs once", not "one run"
	var mu sync.Mutex
	runs := map[string]int{}
	job := Job{Name: "daily", Kind: Cron, Effect: Local, Cron: "0 2 * * *", TimeZone: "Europe/Berlin",
		Handle: func(ctx context.Context, r *Run) error {
			mu.Lock()
			runs[r.Slot]++
			mu.Unlock()
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
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, runs["2026-10-17T02:00"], "the planted slot ran once: %v", runs)
	for slot, n := range runs {
		assert.Equal(t, 1, n, "slot %s ran %d times", slot, n)
	}
	var effects int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM items").Scan(&effects))
	assert.Equal(t, len(runs), effects, "one effect per slot")
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

func TestATakenOverHolderCanCommitNothing(t *testing.T) { // P7.6, CK-WK-02, CK-WK-03, P7.10, P7.2 (the operation ID kept on takeover; the identities' formats are not checked here)
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

func TestRetriesThenDeadWithItsDeadLetter(t *testing.T) { // P7.13, CK-WK-08, P7.3 (the rollback at the end), CK-WK-01
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

func TestARunPastItsDeadlineIsRetried(t *testing.T) {
	d, admin := dbtest.Schema(t, "work7", nil)
	var runs atomic.Int32
	job := Job{Name: "slow", Kind: Queue, Effect: Local, Timeout: 100 * time.Millisecond, MaxAttempts: 2,
		Handle: func(ctx context.Context, r *Run) error {
			runs.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}}
	r := replica(d, 30*time.Second, job)
	id := enqueue(t, d, r, "slow")
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(ctx))
	require.Eventually(t, func() bool { s, _, _, _ := state(t, admin, id); return s == "dead" }, 10*time.Second, 20*time.Millisecond)
	cancel()
	r.Stop()
	assert.Equal(t, int32(2), runs.Load(), "retried after the timeout without waiting for the 30 s lease")
}

func TestACommitWakesAWorkerWithoutWaitingForThePoll(t *testing.T) { // P7.21
	d, _ := dbtest.Schema(t, "work8", nil)
	done := make(chan time.Time, 8)
	job := Job{Name: "quick", Kind: Queue, Effect: Local,
		Handle: func(context.Context, *Run) error { done <- time.Now(); return nil }}
	r := replica(d, 30*time.Second, job)
	r.Poll = time.Hour // only the wake can pick the items up
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.Stop() }()
	require.NoError(t, r.Start(ctx))
	time.Sleep(100 * time.Millisecond) // the first poll at start has found nothing

	committed := time.Now()
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		for range 3 {
			if _, err := r.Enqueue(ctx, tx, "quick", nil); err != nil {
				return err
			}
		}
		return nil
	}))
	for range 3 {
		select {
		case at := <-done:
			assert.Less(t, at.Sub(committed), 2*time.Second, "run on the commit's wake, not on the hourly poll")
		case <-time.After(5 * time.Second):
			t.Fatal("an item enqueued by a committed transaction did not run without a poll")
		}
	}

	// a rolled-back transaction wakes nothing and leaves nothing to run
	require.Error(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := r.Enqueue(ctx, tx, "quick", nil)
		require.NoError(t, err)
		return errors.New("business rollback")
	}))
	select {
	case <-done:
		t.Fatal("a rolled-back item ran")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestAnItemRunsNoEarlierThanItsRunAt(t *testing.T) { // P7.24, P7.5
	d, admin := dbtest.Schema(t, "work11", nil)
	ran := make(chan string, 4)
	job := Job{Name: "due", Kind: Queue, Effect: Local, Handle: func(_ context.Context, r *Run) error {
		ran <- string(r.Args)
		return r.Complete(context.Background(), nil)
	}}
	r := replica(d, 30*time.Second, job)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.Stop() }()
	require.NoError(t, r.Start(ctx))

	var dbNow time.Time
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT now()").Scan(&dbNow))
	later := dbNow.Add(1500 * time.Millisecond)
	var future, past string
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		var err error
		if future, err = r.EnqueueAt(ctx, tx, "due", "future", later); err != nil {
			return err
		}
		past, err = r.EnqueueAt(ctx, tx, "due", "past", dbNow.Add(-time.Minute))
		return err
	}))
	select { // an item already due runs at once
	case a := <-ran:
		require.Equal(t, `"past"`, a, "the future item ran before its run_at")
	case <-time.After(3 * time.Second):
		t.Fatal("an item due in the past did not run")
	}
	st, _, _, _ := state(t, admin, past)
	require.Equal(t, "done", st)

	select { // the future one waits: 30 ms polls find nothing to claim until its run_at
	case a := <-ran:
		t.Fatalf("%s ran before its run_at", a)
	case <-time.After(time.Second):
	}
	st, attempts, _, _ := state(t, admin, future)
	require.Equal(t, "pending", st)
	require.Zero(t, attempts, "no claim before its run_at")
	select {
	case a := <-ran:
		require.Equal(t, `"future"`, a)
		var at time.Time
		require.NoError(t, admin.QueryRow(context.Background(), "SELECT now()").Scan(&at))
		require.False(t, at.Before(later), "claimed by the database clock at or after run_at")
	case <-time.After(3 * time.Second):
		t.Fatal("an item past its run_at did not run")
	}
}

func TestABusinessWaitBacksOffWithItsWaitAndEndsPastItsLimit(t *testing.T) { // P7.13
	d, admin := dbtest.Schema(t, "work12", nil)
	job := Job{Name: "waits", Kind: Queue, Effect: Local, MaxWait: time.Hour,
		Handle: func(context.Context, *Run) error { return Wait(errors.New("the source cannot be proven yet")) }}
	r := replica(d, 30*time.Second, job)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.Stop() }()
	require.NoError(t, r.Start(ctx))
	id := enqueue(t, d, r, "waits")
	gap := func() (st string, attempts int, secs float64) {
		require.NoError(t, admin.QueryRow(context.Background(), `SELECT state, attempts,
			extract(epoch FROM run_at - now()) FROM be_work_item WHERE id = $1`, id).Scan(&st, &attempts, &secs))
		return
	}
	require.Eventually(t, func() bool { st, _, _ := gap(); return st == "retry_wait" }, 5*time.Second, 20*time.Millisecond)
	st, attempts, secs := gap()
	require.Equal(t, "retry_wait", st)
	assert.Zero(t, attempts, "a wait spends no attempt")
	assert.InDelta(t, 1, secs, 0.5, "the first wait: the backoff's start")

	// it has waited 20 s: the next wait is as long again (the backoff doubles with the wait, not with attempts, which a
	// wait never spends); then capped at 5 minutes
	for _, c := range []struct{ waited, want float64 }{{20, 20}, {1200, 300}} {
		_, err := admin.Exec(context.Background(), `UPDATE be_work_item SET waiting_since = now() - make_interval(secs => $2),
			run_at = now() WHERE id = $1`, id, c.waited)
		require.NoError(t, err)
		require.Eventually(t, func() bool { _, _, s := gap(); return s > 2 }, 5*time.Second, 20*time.Millisecond)
		st, attempts, secs = gap()
		require.Equal(t, "retry_wait", st)
		assert.Zero(t, attempts)
		assert.InDelta(t, c.want, secs, 1.5, "waited %v s", c.waited)
	}

	// past MaxWait the wait is a permanent failure: dead
	_, err := admin.Exec(context.Background(), `UPDATE be_work_item SET waiting_since = now() - interval '2 hours',
		run_at = now() WHERE id = $1`, id)
	require.NoError(t, err)
	require.Eventually(t, func() bool { st, _, _ := gap(); return st == "dead" }, 5*time.Second, 20*time.Millisecond,
		"waited longer than its limit")
}

func TestABacklogDoesNotSlowEachClaim(t *testing.T) { // P7.22, P5.33, CK-DB-10
	// the claim's cached plan settles while the table is small; 20 000 items then arrive. Autovacuum is off on this
	// table, so only the SDK's statistics upkeep analyzes it: until then the plan joined the claimed item back by
	// scanning the whole table (about 3 ms a claim, 2026-11-02); the old test analyzed the table itself and passed
	d, admin := dbtest.Schema(t, "work9", nil)
	pgtest.Exec(t, admin, "ALTER TABLE be_work_item SET (autovacuum_enabled = false)")
	r := replica(d, 30*time.Second, Job{Name: "bulk", Kind: Queue, Effect: Local,
		Handle: func(context.Context, *Run) error { return nil }},
		Job{Name: "other", Kind: Queue, Effect: Local, Handle: func(context.Context, *Run) error { return nil }})
	median := func() time.Duration {
		var took []time.Duration
		for range 15 {
			started := time.Now()
			run, err := r.claim(context.Background())
			took = append(took, time.Since(started))
			require.NoError(t, err)
			require.NotNil(t, run)
		}
		slices.Sort(took)
		return took[len(took)/2]
	}
	insert := func(n int) {
		pgtest.Exec(t, admin, `INSERT INTO be_work_item (id, job, kind, state, max_attempts, effect_class, operation_id, args,
			run_at) SELECT gen_random_uuid(), CASE WHEN g % 2 = 0 THEN 'bulk' ELSE 'other' END, 'queue', 'pending', 3,
			'local', gen_random_uuid()::text, '{}'::jsonb, now() - g * interval '1 ms' FROM generate_series(1, `+
			strconv.Itoa(n)+`) g`)
		pgtest.Exec(t, admin, "SELECT pg_stat_force_next_flush()")
	}
	insert(40)
	small := median()
	insert(20000)
	analyzed, err := db.AnalyzeStale(context.Background(), d)
	require.NoError(t, err)
	require.Contains(t, analyzed, "be_work_item")
	large := median()
	t.Logf("claim median: %v with 40 items due, %v with 20 000", small, large)
	assert.Less(t, large, small+time.Millisecond, "a claim must not grow with the backlog")
}

// The identities and operation IDs of P7.2: a queue item is a UUIDv7 with the operation ID <id>:<job>, written when it is
// enqueued; a cron item is <job>@<slot> with an operation ID that no other item reuses; a retry keeps the operation ID.
func TestTheIdentitiesAndOperationIDs(t *testing.T) { // P7.2, CK-WK-01
	d, admin := dbtest.Schema(t, "work_ids", nil)
	var mu sync.Mutex
	var seen []string
	queue := Job{Name: "q", Kind: Queue, Effect: Local, MaxAttempts: 3, Handle: func(ctx context.Context, r *Run) error {
		mu.Lock()
		seen = append(seen, r.OperationID)
		first := len(seen) == 1
		mu.Unlock()
		if first {
			return errors.New("the first attempt fails")
		}
		return r.Complete(ctx, nil)
	}}
	cron := Job{Name: "daily", Kind: Cron, Effect: Local, Cron: "0 2 * * *", TimeZone: "Europe/Berlin", Backfill: All,
		Handle: func(context.Context, *Run) error { return nil }}
	r := replica(d, 5*time.Second, queue)

	id := enqueue(t, d, r, "q")
	u, err := uuid.Parse(id)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), u.Version(), "a queue item is a UUIDv7")
	var op string
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT operation_id FROM be_work_item WHERE id = $1", id).Scan(&op))
	assert.Equal(t, id+":q", op, "written when enqueued")
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(ctx))
	require.Eventually(t, func() bool { st, _, _, _ := state(t, admin, id); return st == "done" }, 10*time.Second, 20*time.Millisecond)
	cancel()
	r.Stop()
	mu.Lock()
	require.Len(t, seen, 2, "one failed attempt, one retry")
	assert.Equal(t, []string{op, op}, seen, "the retry keeps the operation ID")
	mu.Unlock()

	// cron on a runner that is never started: a started one materializes the slots due by the database's clock too
	dc, adminc := dbtest.Schema(t, "work_ids_cron", nil)
	rc := replica(dc, 5*time.Second, cron)
	pgtest.Exec(t, adminc, "INSERT INTO be_cron_watermark (job, watermark_at) VALUES ('daily', '2026-10-14T23:59:00Z')")
	require.NoError(t, rc.Materialize(context.Background(), "daily", time.Date(2026, 10, 17, 0, 0, 30, 0, time.UTC)))
	rows, err := adminc.Query(context.Background(), "SELECT id, slot, operation_id FROM be_work_item WHERE kind = 'cron' ORDER BY slot")
	require.NoError(t, err)
	ops := map[string]bool{op: true}
	n := 0
	for rows.Next() {
		var cid, slot, cop string
		require.NoError(t, rows.Scan(&cid, &slot, &cop))
		assert.Equal(t, "daily@"+slot, cid, "a cron item is <job>@<slot>")
		assert.False(t, ops[cop], "operation ID %s reused", cop)
		ops[cop] = true
		n++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 3, n, "the slots of 15, 16 and 17 October")
}

// idempotent_external (P7.11): every attempt carries the same operation ID, a call within the run its sub-key, and a
// holder that takes over sees what the stalled one recorded about the call, so it asks the other side by that key
// before sending again.
func TestAnIdempotentExternalTakeoverKeepsTheKeysAndTheRecordedCall(t *testing.T) { // P7.11, CK-WK-06
	d, _ := dbtest.Schema(t, "work_ext", nil)
	job := Job{Name: "pay", Kind: Queue, Effect: IdempotentExternal, Handle: func(context.Context, *Run) error { return nil }}
	a, b := replica(d, time.Second, job), replica(d, time.Second, job)
	enqueue(t, d, a, "pay")
	ra, err := a.claim(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ra)
	key := ra.SubKey("charge")
	assert.Equal(t, ra.OperationID+":charge", key)
	require.NoError(t, ra.RecordCall(context.Background(), key, "unknown"), "recorded before the call is made")
	time.Sleep(1100 * time.Millisecond) // A stalls past its lease, its call's outcome unknown
	rb, err := b.claim(context.Background())
	require.NoError(t, err)
	require.NotNil(t, rb, "taken over")
	assert.Equal(t, ra.OperationID, rb.OperationID, "the same operation ID on every attempt")
	assert.Equal(t, key, rb.SubKey("charge"), "the same sub-key")
	assert.Equal(t, "unknown", rb.CallResults[key], "the new holder knows the call may have been made, so it queries first")
	require.NoError(t, rb.RecordCall(context.Background(), key, "effective"))
	require.NoError(t, rb.Complete(context.Background(), nil))
}

// Model 37's ONE_HOLDER: a lossy restore loses A's claim with the last commits and advances the recovery generation;
// B claims the item and gets the same claim counter again. A, still running, must not commit: holders compare the
// database epoch they obtained with their own claim, not the row's claim epoch with the current one.
func TestALostClaimCannotCommitUnderAReusedCounter(t *testing.T) { // CK-WK-03, P7.6
	d, admin := dbtest.Schema(t, "work_reuse", nil)
	job := Job{Name: "q", Kind: Queue, Effect: Local, Handle: func(context.Context, *Run) error { return nil }}
	a, b := replica(d, 30*time.Second, job), replica(d, 30*time.Second, job)
	id := enqueue(t, d, a, "q")
	ra, err := a.claim(context.Background())
	require.NoError(t, err)
	require.NotNil(t, ra)
	pgtest.Exec(t, admin, `UPDATE be_work_item SET state = 'pending', epoch = 0, attempts = 0, holder = NULL,
		lease_until = NULL, claim_db_generation = NULL, claim_db_timeline = NULL WHERE id = '`+id+`'`,
		"UPDATE be_recovery_generation SET generation = generation + 1")
	rb, err := b.claim(context.Background())
	require.NoError(t, err)
	require.NotNil(t, rb)
	require.Equal(t, ra.epoch, rb.epoch, "precondition: the claim counter was handed out again")
	require.NotEqual(t, ra.gen, rb.gen, "precondition: in a larger recovery generation")
	effect := func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1)", ids.New())
		return err
	}
	assert.ErrorIs(t, ra.Complete(context.Background(), effect), ErrLost, "the lost claim commits nothing")
	var n int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM items").Scan(&n))
	assert.Zero(t, n)
	require.NoError(t, rb.Complete(context.Background(), effect))
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM items").Scan(&n))
	assert.Equal(t, 1, n, "the holder of the new epoch does")
}
