//go:build integration

package work

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An idle member: one claim per poll whatever the number of workers, and no transaction for a cron job before its
// next slot (each worker polling and the scheduler locking the watermark every second: about 10 transactions a second
// for 4 workers and one cron job, 2026-11-03).
func TestAnIdleMemberClaimsOncePerPoll(t *testing.T) { // P7.23
	d, _ := dbtest.Schema(t, "work_idle", nil)
	noop := func(context.Context, *Run) error { return nil }
	r := replica(d, 30*time.Second, Job{Name: "q", Kind: Queue, Effect: Local, Handle: noop},
		Job{Name: "yearly", Kind: Cron, Effect: Local, Cron: "0 0 1 1 *", TimeZone: "UTC", Handle: noop})
	r.Poll, r.Concurrency = 100*time.Millisecond, 4
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(ctx))
	time.Sleep(2 * time.Second)
	cancel()
	r.Stop()
	claims, materialized := r.claims.Load(), r.materialized.Load()
	t.Logf("in 2 s: %d claims, %d materializing transactions", claims, materialized)
	assert.LessOrEqual(t, claims, int64(2*time.Second/r.Poll)+3, "one claim per poll, not one per worker")
	assert.Equal(t, int64(1), materialized, "the first look sets the watermark; the next slot is months away")
}

// One poll still spreads a backlog the member did not enqueue itself over all its workers.
func TestAPollSpreadsABacklogOverTheWorkers(t *testing.T) { // P7.23
	d, admin := dbtest.Schema(t, "work_spread", nil)
	var now, most atomic.Int32
	r := replica(d, 30*time.Second, Job{Name: "slow", Kind: Queue, Effect: Local, Handle: func(ctx context.Context, run *Run) error {
		n := now.Add(1)
		for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
		}
		time.Sleep(300 * time.Millisecond)
		now.Add(-1)
		return run.Complete(ctx, nil)
	}})
	r.Poll, r.Concurrency = 50*time.Millisecond, 4
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(ctx))
	time.Sleep(200 * time.Millisecond) // idle: the polls find nothing
	pgtest.Exec(t, admin, `INSERT INTO be_work_item (id, job, kind, state, max_attempts, effect_class, operation_id, args)
		SELECT gen_random_uuid(), 'slow', 'queue', 'pending', 3, 'local', gen_random_uuid()::text, '{}'::jsonb
		FROM generate_series(1, 8)`) // as another replica would: no wake in this process
	require.Eventually(t, func() bool {
		var left int
		require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_work_item WHERE job = 'slow' AND state <> 'done'").Scan(&left))
		return left == 0
	}, 5*time.Second, 20*time.Millisecond)
	cancel()
	r.Stop()
	assert.Equal(t, int32(4), most.Load(), "every worker took part")
}

// The scheduler skips the transaction before the next slot, and materializes the slot when it comes.
func TestTheSchedulerMaterializesASlotWhenItComes(t *testing.T) { // P7.23, P7.16
	d, admin := dbtest.Schema(t, "work_slot", nil)
	r := replica(d, 30*time.Second, Job{Name: "minutely", Kind: Cron, Effect: Local, Cron: "* * * * *", TimeZone: "UTC",
		Handle: func(ctx context.Context, run *Run) error { return run.Complete(ctx, nil) }})
	var dbNow time.Time
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT now()").Scan(&dbNow))
	next := dbNow.Truncate(time.Minute).Add(time.Minute)
	if time.Until(next) < 2*time.Second { // too close to tell the first look from the slot
		next = next.Add(time.Minute)
		time.Sleep(time.Until(next.Add(-time.Minute)) + 100*time.Millisecond)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); r.Stop() }()
	require.NoError(t, r.Start(ctx))
	slot := "minutely@" + next.UTC().Format("2006-01-02T15:04")
	require.Eventually(t, func() bool {
		var st string
		err := admin.QueryRow(context.Background(), "SELECT state FROM be_work_item WHERE id = $1", slot).Scan(&st)
		return err == nil && st == "done"
	}, time.Until(next)+3*time.Second, 50*time.Millisecond, "the slot %s ran within 3 s of its time", slot)
	var items int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_work_item WHERE job = 'minutely'").Scan(&items))
	assert.Equal(t, 1, items, "only that slot: the first look set the watermark without backfilling ("+strconv.Itoa(items)+")")
	assert.LessOrEqual(t, r.materialized.Load(), int64(3), "no transaction every second while waiting for the slot")
}
