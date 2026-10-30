package calls

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newDep(t *testing.T) *Dependency {
	m, err := NewMember(nil)
	require.NoError(t, err)
	d := m.Dependency("demo/c", "grpc")
	d.sleep = func(context.Context, time.Duration) error { return nil } // backoff is covered by the vectors
	return d
}

func withDeadline(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

var busy = errs.New(errs.ReasonServiceBusy, "busy")

func failing(n *atomic.Int32, e *errs.Error) func(context.Context) Result {
	return func(context.Context) Result {
		n.Add(1)
		return Result{Err: e, Code: e.Code, Remote: e}
	}
}

func TestRetriesOnlyIdempotentCallsAtMostThreeTimes(t *testing.T) {
	d := newDep(t)
	var n atomic.Int32
	err := d.Do(withDeadline(t, 5*time.Second), true, failing(&n, busy))
	assert.Equal(t, int32(3), n.Load())
	assert.True(t, errs.Is(err, "be", errs.ReasonDependencyUnavailable), "%v", err)
	assert.Equal(t, "demo/c", errs.From(err).Metadata["dependency"])

	n.Store(0)
	err = d.Do(withDeadline(t, 5*time.Second), false, failing(&n, busy))
	assert.Equal(t, int32(1), n.Load(), "a command is never retried")
	assert.True(t, errs.Is(err, "be", errs.ReasonDependencyUnavailable))
}

func TestRelayedFailuresAreNeitherRetriedNorCounted(t *testing.T) {
	d := newDep(t)
	var n atomic.Int32
	relayedErr := errs.New(errs.ReasonDependencyUnavailable, "further down").With("dependency", "demo/d")
	for i := 0; i < 30; i++ {
		err := d.Do(withDeadline(t, 5*time.Second), true, failing(&n, relayedErr))
		assert.Same(t, relayedErr, err, "passed upward unchanged")
	}
	assert.Equal(t, int32(30), n.Load(), "one attempt each")
	assert.Equal(t, Closed, d.Breaker().State(), "relayed failures do not open the breaker")
	assert.Equal(t, 10000, d.Budget().Tokens())
}

func TestBudgetStopsRetriesWhenTheDependencyKeepsFailing(t *testing.T) {
	d := newDep(t)
	var n atomic.Int32
	for i := 0; i < 4; i++ {
		_ = d.Do(withDeadline(t, 5*time.Second), true, failing(&n, busy))
	}
	// 10000 -> after 6 unavailable attempts 4000: no more retries, each call is one attempt
	before := n.Load()
	_ = d.Do(withDeadline(t, 5*time.Second), true, failing(&n, busy))
	assert.Equal(t, int32(1), n.Load()-before)
}

func TestBreakerOpensAndFailsFastWithoutSending(t *testing.T) {
	d := newDep(t)
	now := time.Now()
	d.now = func() time.Time { return now }
	var n atomic.Int32
	timeout := func(context.Context) Result {
		n.Add(1)
		return Result{Err: errors.New("deadline"), Code: errs.DeadlineExceeded}
	}
	for i := 0; i < BreakerWindow; i++ {
		err := d.Do(withDeadline(t, 5*time.Second), false, timeout)
		assert.True(t, errs.Is(err, "be", errs.ReasonResultUnknown), "a command that timed out: query first")
	}
	assert.Equal(t, Open, d.Breaker().State())
	sent := n.Load()
	err := d.Do(withDeadline(t, 5*time.Second), true, timeout)
	assert.True(t, errs.Is(err, "be", errs.ReasonCircuitOpen))
	assert.Equal(t, sent, n.Load(), "nothing sent while open")

	now = now.Add(BreakerOpenFirst)
	ok := func(context.Context) Result { return Result{Code: "OK"} }
	for i := 0; i < BreakerTrialsToClose; i++ {
		require.NoError(t, d.Do(withDeadline(t, 5*time.Second), true, ok))
	}
	assert.Equal(t, Closed, d.Breaker().State())
}

func TestOutboundLimit(t *testing.T) {
	d := newDep(t)
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(OutboundLimit)
	var wg sync.WaitGroup
	for i := 0; i < OutboundLimit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = d.Do(withDeadline(t, 5*time.Second), true, func(context.Context) Result {
				started.Done()
				<-release
				return Result{Code: "OK"}
			})
		}()
	}
	started.Wait()
	var n atomic.Int32
	err := d.Do(withDeadline(t, 5*time.Second), true, failing(&n, busy))
	assert.True(t, errs.Is(err, "be", errs.ReasonOutboundLimit))
	assert.Equal(t, "demo/c", errs.From(err).Metadata["dependency"])
	assert.Zero(t, n.Load(), "refused without queueing or sending")
	close(release)
	wg.Wait()
}

func TestDeadlines(t *testing.T) {
	d := newDep(t)
	var n atomic.Int32
	err := d.Do(context.Background(), true, failing(&n, busy))
	assert.True(t, errs.Is(err, "be", errs.ReasonDeadlineBudgetExhausted), "no deadline: not sent")
	err = d.Do(withDeadline(t, 40*time.Millisecond), true, failing(&n, busy))
	assert.True(t, errs.Is(err, "be", errs.ReasonDeadlineBudgetExhausted), "under 50 ms left: not sent")
	assert.Zero(t, n.Load())

	var seen time.Duration
	require.NoError(t, d.Do(withDeadline(t, 10*time.Second), true, func(ctx context.Context) Result {
		dl, _ := ctx.Deadline()
		seen = time.Until(dl)
		return Result{Code: "OK"}
	}))
	assert.InDelta(t, MaxAttemptTimeout.Seconds(), seen.Seconds(), 0.05, "one attempt gets at most 3 s")
	require.NoError(t, d.Do(withDeadline(t, time.Second), true, func(ctx context.Context) Result {
		dl, _ := ctx.Deadline()
		seen = time.Until(dl)
		return Result{Code: "OK"}
	}))
	assert.InDelta(t, 0.95, seen.Seconds(), 0.05, "remaining minus 50 ms")

	ctx, cancel := context.WithCancel(withDeadline(t, 5*time.Second))
	err = d.Do(ctx, true, func(context.Context) Result {
		cancel()
		return Result{Err: context.Canceled, Code: errs.Cancelled}
	})
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestCancelled))
}

// TestBackgroundCallsAreNeitherCountedNorHeldByTheBreaker: the SDK's own background calls to the authorization member
// (P4.27) go out while the breaker is open, and their failures and successes do not move it.
func TestBackgroundCallsAreNeitherCountedNorHeldByTheBreaker(t *testing.T) {
	m, err := NewMember(nil)
	require.NoError(t, err)
	d := m.Dependency("infra/authz", "grpc")
	d.sleep = func(context.Context, time.Duration) error { return nil }
	call := m.unary(d)
	var sent atomic.Int32
	down := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		sent.Add(1)
		return status.Error(codes.Unavailable, "connection refused")
	}
	up := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		sent.Add(1)
		return nil
	}
	for i := 0; i < 2*BreakerWindow; i++ {
		_ = call(withDeadline(t, 5*time.Second), "/m", nil, nil, nil, down, Idempotent(), Background())
	}
	assert.Equal(t, Closed, d.Breaker().State(), "background failures are not counted")

	for i := 0; i < BreakerWindow; i++ {
		_ = call(withDeadline(t, 5*time.Second), "/m", nil, nil, nil, down)
	}
	require.Equal(t, Open, d.Breaker().State(), "ordinary failures open it")
	before := sent.Load()
	require.NoError(t, call(withDeadline(t, 5*time.Second), "/m", nil, nil, nil, up, Idempotent(), Background()))
	assert.Equal(t, before+1, sent.Load(), "a background call is sent while the breaker is open")
	assert.Equal(t, Open, d.Breaker().State(), "and its success does not close it")
	err = call(withDeadline(t, 5*time.Second), "/m", nil, nil, nil, up, Idempotent())
	assert.True(t, errs.Is(err, "be", errs.ReasonCircuitOpen), "ordinary calls are still held: %v", err)
}
