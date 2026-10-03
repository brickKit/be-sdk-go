package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func bareEngine(t *testing.T, reg prometheus.Registerer) *Engine {
	t.Helper()
	e, err := New(Config{ComponentID: "conformance/widget", InstanceID: "i1", Registerer: reg}, Declarations{})
	require.NoError(t, err)
	return e
}

func TestExecTimeoutCancelsTheRun(t *testing.T) {
	e := bareEngine(t, nil)
	start := time.Now()
	res, err := e.exec(context.Background(), RunInfo{Name: "widget.slow", Kind: "every"}, 50*time.Millisecond,
		func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	require.Equal(t, ResultTimeout, res)
	require.Error(t, err)
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestExecRecoversPanic(t *testing.T) {
	e := bareEngine(t, nil)
	res, err := e.exec(context.Background(), RunInfo{Name: "widget.boom"}, time.Second,
		func(context.Context) error { panic("boom") })
	require.Equal(t, ResultError, res)
	require.ErrorContains(t, err, "boom")
}

func TestExecClassifiesCancellation(t *testing.T) {
	e := bareEngine(t, nil)
	parent, cancel := context.WithCancelCause(context.Background())
	cancel(errLeaseLost)
	res, _ := e.exec(parent, RunInfo{Name: "widget.s"}, time.Second, func(ctx context.Context) error { return ctx.Err() })
	require.Equal(t, ResultLeaseLost, res)
	stopping, stop := context.WithCancel(context.Background())
	stop()
	res, _ = e.exec(stopping, RunInfo{Name: "widget.s"}, time.Second, func(ctx context.Context) error { return ctx.Err() })
	require.Equal(t, ResultCancelled, res)
}

func TestExecWrapAndContext(t *testing.T) {
	var wrapped RunInfo
	e, err := New(Config{ComponentID: "conformance/widget", InstanceID: "i1",
		Wrap: func(ctx context.Context, info RunInfo, run func(context.Context) error) error {
			wrapped = info
			return run(ctx)
		}}, Declarations{})
	require.NoError(t, err)
	var epoch int64
	var ok bool
	_, err = e.exec(context.Background(), RunInfo{Name: "widget.s", Kind: "singleton", Epoch: 7}, time.Second,
		func(ctx context.Context) error { epoch, ok = Epoch(ctx); return nil })
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(7), epoch)
	require.Equal(t, "widget.s", wrapped.Name)
	slot := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	_, err = e.exec(context.Background(), RunInfo{Name: "widget.c", Kind: "cron", Slot: slot}, time.Second,
		func(ctx context.Context) error {
			got, ok := Slot(ctx)
			require.True(t, ok)
			require.True(t, got.Equal(slot))
			_, ok = Epoch(ctx)
			require.False(t, ok, "a cron run has no fencing token")
			return nil
		})
	require.NoError(t, err)
}

func TestExecMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	e := bareEngine(t, reg)
	_, _ = e.exec(context.Background(), RunInfo{Name: "widget.a"}, time.Second, func(context.Context) error { return nil })
	_, _ = e.exec(context.Background(), RunInfo{Name: "widget.a"}, time.Second, func(context.Context) error { return errors.New("x") })
	require.Equal(t, 1.0, counterValue(t, e.m.runs.WithLabelValues("widget.a", "ok")))
	require.Equal(t, 1.0, counterValue(t, e.m.runs.WithLabelValues("widget.a", "error")))
	require.Greater(t, gaugeValue(t, e.m.lastSuccess.WithLabelValues("widget.a")), 0.0)
	e.m.queueDepth.WithLabelValues("widget.q", "ready").Set(0)
	e.m.queueOldest.WithLabelValues("widget.q").Set(0)
	e.m.recPending.WithLabelValues("widget.r").Set(0)
	e.m.recOldest.WithLabelValues("widget.r").Set(0)
	e.m.recGiveups.WithLabelValues("widget.r").Add(0)
	names := map[string]bool{}
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, n := range []string{"be_job_runs_total", "be_job_duration_seconds", "be_job_last_success_timestamp_seconds",
		"be_queue_depth", "be_queue_oldest_age_seconds", "be_reconcile_pending", "be_reconcile_oldest_age_seconds",
		"be_reconcile_giveups_total"} {
		require.True(t, names[n], "metric %s registered", n)
	}
}

func TestNewNeedsAStoreForJobs(t *testing.T) {
	_, err := New(Config{ComponentID: "c/x", InstanceID: "i"}, Declarations{Jobs: []Job{everyJob("widget.a")}})
	configErr(t, err, "PG_SCHEMA")
}

func TestBackoff(t *testing.T) {
	require.Equal(t, time.Second, backoffFor(nil, 1))
	require.Equal(t, 4*time.Second, backoffFor(nil, 3))
	require.Equal(t, 5*time.Minute, backoffFor(nil, 20))
	list := []time.Duration{time.Second, time.Minute}
	require.Equal(t, time.Second, backoffFor(list, 1))
	require.Equal(t, time.Minute, backoffFor(list, 2))
	require.Equal(t, time.Minute, backoffFor(list, 9))
}

func TestExitCodes(t *testing.T) {
	require.Equal(t, 0, RunOutcome{Result: RunOK}.ExitCode())
	require.Equal(t, 0, RunOutcome{Result: RunNoop}.ExitCode())
	require.Equal(t, 1, RunOutcome{Result: RunFailed}.ExitCode())
	require.Equal(t, 64, RunOutcome{Result: RunUnknown}.ExitCode())
}

func TestRunOnceUnknownName(t *testing.T) {
	e := bareEngine(t, nil)
	out := e.RunOnce(context.Background(), "widget.nope")
	require.Equal(t, RunUnknown, out.Result)
	require.Equal(t, 64, out.ExitCode())
}

func TestDisabledJobHasNoLoop(t *testing.T) {
	p, err := compile(Config{Overrides: `{"widget.b":{"enabled":false}}`},
		Declarations{Jobs: []Job{everyJob("widget.a"), everyJob("widget.b")}})
	require.NoError(t, err)
	e := &Engine{plan: p}
	loops := e.Loops()
	require.Len(t, loops, 1)
	require.Equal(t, "widget.a", loops[0].Name)
	require.Len(t, e.Jobs(), 2, "a disabled job is still declared: job run runs it (P14.5)")
}

func TestErrorText(t *testing.T) {
	long := make([]byte, 500)
	for i := range long {
		long[i] = 'x'
	}
	require.Len(t, errorText(errors.New(string(long))), maxErrorText)
	require.Equal(t, "", errorText(nil))
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	return m.GetCounter().GetValue()
}

func gaugeValue(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, g.Write(&m))
	return m.GetGauge().GetValue()
}
