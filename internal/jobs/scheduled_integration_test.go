package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/stretchr/testify/require"
)

// CP-JOBS-01: two replicas run the platform cron be.cleanup, set to @every 2s through
// JOBS_OVERRIDES, once per slot.
func TestCronSlotRunsOnceAcrossReplicas(t *testing.T) {
	e := newEnv(t)
	var runs atomic.Int32
	decl := Declarations{Runtime: []Job{{Name: "be.cleanup", Kind: Cron, Cron: "@every 1h", Timeout: 5 * time.Second,
		Run: func(context.Context) error { runs.Add(1); return nil }}}}
	over := func(c *Config) { c.Overrides = `{"be.cleanup":{"cron":"@every 2s"}}` }
	stopA := runLoops(t, e.engine(t, "i1", decl, over))
	stopB := runLoops(t, e.engine(t, "i2", decl, over))
	time.Sleep(5500 * time.Millisecond)
	stopA()
	stopB()
	slots := e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_slot WHERE name = 'be.cleanup'`)
	done := e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_slot WHERE name = 'be.cleanup' AND done_at IS NOT NULL AND result = 'ok'`)
	require.GreaterOrEqual(t, slots, 2, "a slot every 2 s over 5.5 s")
	require.Equal(t, slots, int(runs.Load()), "each slot ran exactly once")
	require.Equal(t, slots, done)
	require.Equal(t, 0, e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_slot
		WHERE name = 'be.cleanup' AND extract(epoch FROM slot_at)::bigint % 2 <> 0`), "slots are multiples of 2 s since the epoch")
}

// singletonProbe records who ran a singleton and how many ran at once.
type singletonProbe struct {
	active, maxActive atomic.Int32
	mu                sync.Mutex
	byHolder          map[string]int
	epochs            map[string]int64
}

func (p *singletonProbe) run(ctx context.Context) error {
	n := p.active.Add(1)
	defer p.active.Add(-1)
	for {
		m := p.maxActive.Load()
		if n <= m || p.maxActive.CompareAndSwap(m, n) {
			break
		}
	}
	info, _ := RunOf(ctx)
	epoch, _ := Epoch(ctx)
	p.mu.Lock()
	p.byHolder[info.Holder]++
	p.epochs[info.Holder] = epoch
	p.mu.Unlock()
	sleep(ctx, 30*time.Millisecond)
	return nil
}

func (p *singletonProbe) runs(holder string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.byHolder[holder]
}

func (p *singletonProbe) holders() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for h := range p.byHolder {
		out = append(out, h)
	}
	return out
}

// CP-JOBS-02: a singleton has one holder at a time; a dead holder's lease is taken over within the
// TTL (epoch + 1), and a stopped holder's lease at once.
func TestSingletonOneHolderAndTakeover(t *testing.T) {
	e := newEnv(t)
	p := &singletonProbe{byHolder: map[string]int{}, epochs: map[string]int64{}}
	const ttl = 1500 * time.Millisecond
	decl := Declarations{Jobs: []Job{{Name: "widget.partitions", Kind: Singleton, Interval: 100 * time.Millisecond,
		Timeout: 5 * time.Second, Run: p.run}}}
	shortTTL := func(c *Config) { c.LeaseTTL = ttl }
	// A holder that died: its lease runs out 1.5 s from now.
	e.exec(t, `INSERT INTO SCHEMA.besdk_job_lease (name, holder, epoch, expires_at)
		VALUES ('widget.partitions', 'conformance/widget/dead', 5, now() + interval '1500 milliseconds')`)
	start := time.Now()
	stops := map[string]func(){
		componentID + "/i1": runLoops(t, e.engine(t, "i1", decl, shortTTL)),
		componentID + "/i2": runLoops(t, e.engine(t, "i2", decl, shortTTL)),
	}
	eventually(t, 5*time.Second, func() bool { return len(p.holders()) > 0 }, "a replica takes over the dead lease")
	took := time.Since(start)
	require.Greater(t, took, time.Second, "nobody runs while the dead holder's lease lasts")
	require.Less(t, took, ttl+ttl/3+time.Second, "taken over within the TTL")
	first := p.holders()[0]
	time.Sleep(600 * time.Millisecond)
	require.Equal(t, []string{first}, p.holders(), "one holder at a time")
	p.mu.Lock()
	require.Equal(t, int64(6), p.epochs[first], "epoch + 1 on takeover")
	p.mu.Unlock()

	stops[first]()
	var second string
	for h := range stops {
		if h != first {
			second = h
		}
	}
	eventually(t, ttl+time.Second, func() bool { return p.runs(second) > 0 }, "the other replica takes over")
	p.mu.Lock()
	require.Equal(t, int64(7), p.epochs[second])
	p.mu.Unlock()
	require.Equal(t, int32(1), p.maxActive.Load(), "never two runs at once")
}

// CP-JOBS-05: a run that fails (its connection terminated, then a panic) ends the loop with an error
// instead of the process; the restarted loop runs again.
func TestFailedRunRestartsWithoutEndingTheProcess(t *testing.T) {
	e := newEnv(t)
	st := e.store(t)
	var calls atomic.Int32
	job := Job{Name: "widget.flaky", Kind: Every, Interval: 50 * time.Millisecond, Timeout: 10 * time.Second,
		Run: func(ctx context.Context) error {
			switch calls.Add(1) {
			case 1:
				return st.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
					_, err := tx.ExecContext(ctx, "SELECT pg_sleep(4)")
					return err
				})
			case 2:
				panic("second run panics")
			}
			return nil
		}}
	loop := e.engine(t, "i1", Declarations{Jobs: []Job{job}}).Loops()[0]
	ctx := within(t, 20*time.Second)
	errs := make(chan error, 1)
	go func() { errs <- loop.Run(ctx) }()
	eventually(t, 5*time.Second, func() bool {
		return e.count(t, `SELECT count(*) FROM pg_stat_activity WHERE usename = '`+e.id.User+`' AND query LIKE '%pg_sleep(4)%'`) == 1
	}, "the first run is in its statement")
	e.exec(t, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '`+e.id.User+`' AND query LIKE '%pg_sleep(4)%'`)
	require.Error(t, <-errs, "the terminated connection fails the run and ends the loop")
	require.ErrorContains(t, loop.Run(ctx), "second run panics", "a panic becomes the loop's error")
	ctx3, stop := context.WithCancel(ctx)
	go func() { errs <- loop.Run(ctx3) }()
	eventually(t, 5*time.Second, func() bool { return calls.Load() >= 4 }, "the restarted loop keeps running")
	stop()
	require.NoError(t, <-errs)
}

// CP-JOBS-06: job run of a cron job claims the most recent slot; concurrent runs run it once, with the
// in-process copy disabled; a slot already claimed is a no-op.
func TestRunOnceCronSlotOnce(t *testing.T) {
	e := newEnv(t)
	var runs atomic.Int32
	decl := Declarations{Jobs: []Job{{Name: "widget.daily", Kind: Cron, Cron: "0 3 * * *", Timeout: 5 * time.Second,
		Run: func(context.Context) error { runs.Add(1); time.Sleep(100 * time.Millisecond); return nil }}}}
	off := func(c *Config) { c.Overrides = `{"widget.daily":{"enabled":false}}` }
	a, b := e.engine(t, "i1", decl, off), e.engine(t, "i2", decl, off)
	require.Empty(t, a.Loops(), "enabled: false stops the in-process scheduling")
	outs := make([]RunOutcome, 2)
	var wg sync.WaitGroup
	for i, eng := range []*Engine{a, b} {
		wg.Add(1)
		go func() { defer wg.Done(); outs[i] = eng.RunOnce(within(t, 10*time.Second), "widget.daily") }()
	}
	wg.Wait()
	require.Equal(t, int32(1), runs.Load(), "one slot, one run")
	require.ElementsMatch(t, []RunResult{RunOK, RunNoop}, []RunResult{outs[0].Result, outs[1].Result})
	require.Equal(t, 0, outs[0].ExitCode())
	require.Equal(t, 0, outs[1].ExitCode())
	require.Equal(t, RunNoop, a.RunOnce(within(t, 5*time.Second), "widget.daily").Result)
	require.Equal(t, 1, e.count(t, `SELECT count(*) FROM SCHEMA.besdk_job_slot
		WHERE name = 'widget.daily' AND holder LIKE 'conformance/widget/job-run:i_' AND done_at IS NOT NULL`))
}

// P14.8: job run of a singleton is a no-op while another holder has the lease, and gives the lease
// back after its run.
func TestRunOnceSingleton(t *testing.T) {
	e := newEnv(t)
	var runs atomic.Int32
	decl := Declarations{Jobs: []Job{{Name: "widget.partitions", Kind: Singleton, Interval: time.Hour,
		Timeout: 5 * time.Second, Run: func(ctx context.Context) error {
			if epoch, ok := Epoch(ctx); !ok || epoch < 1 {
				t.Errorf("no fencing token in a singleton run")
			}
			runs.Add(1)
			return nil
		}}}}
	eng := e.engine(t, "i1", decl)
	e.exec(t, `INSERT INTO SCHEMA.besdk_job_lease (name, holder, epoch, expires_at)
		VALUES ('widget.partitions', 'conformance/widget/other', 3, now() + interval '1 hour')`)
	require.Equal(t, RunNoop, eng.RunOnce(within(t, 5*time.Second), "widget.partitions").Result)
	e.exec(t, `UPDATE SCHEMA.besdk_job_lease SET expires_at = now() - interval '1 second'`)
	require.Equal(t, RunOK, eng.RunOnce(within(t, 5*time.Second), "widget.partitions").Result)
	require.Equal(t, int32(1), runs.Load())
	var holder string
	var released bool
	e.scan(t, `SELECT holder, expires_at < now() FROM SCHEMA.besdk_job_lease WHERE name = 'widget.partitions'`, &holder, &released)
	require.Equal(t, "conformance/widget/job-run:i1", holder)
	require.True(t, released, "the one-shot run gives the lease back")
}
