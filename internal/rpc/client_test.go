package rpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

type actorKey struct{}

type actor struct{ sub, act string }

type clientMetric struct {
	target, method, code string
}

type recordingClientMetrics struct {
	mu       sync.Mutex
	handled  []clientMetric
	inflight map[string]int
	peak     int
}

func (r *recordingClientMetrics) Handled(target, method, code string, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handled = append(r.handled, clientMetric{target, method, code})
}

func (r *recordingClientMetrics) Inflight(target string, delta int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inflight == nil {
		r.inflight = map[string]int{}
	}
	r.inflight[target] += delta
	r.peak = max(r.peak, r.inflight[target])
}

// newTestConns builds a Conns for member erp/sales whose actor hook reads actorKey from the context.
func newTestConns(t *testing.T, mutate func(*ClientConfig)) *Conns {
	t.Helper()
	cfg := ClientConfig{
		CallerID: "erp/sales",
		Actor: func(ctx context.Context) (string, string) {
			a, _ := ctx.Value(actorKey{}).(actor)
			return a.sub, a.act
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c := NewConns(cfg)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func probeClient(t *testing.T, c *Conns, dep, addr string) probepb.ProbeClient {
	t.Helper()
	conn, err := c.Get(dep, addr)
	require.NoError(t, err)
	return probepb.NewProbeClient(conn)
}

// captureMD makes the server record each call's incoming metadata.
func captureMD(ts *testServer) chan metadata.MD {
	got := make(chan metadata.MD, 16)
	ts.probe.set(func(ctx context.Context, _, _ string) (string, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		got <- md
		return "ok", nil
	})
	return got
}

func TestClientSendsCallerRequestIDAndActorReadAtCallTime(t *testing.T) {
	ts := startServer(t, nil)
	got := captureMD(ts)
	c := newTestConns(t, func(cfg *ClientConfig) {
		cfg.RequestID = func(context.Context) string { return "req-42" }
	})
	pc := probeClient(t, c, "conformance/widget", ts.addr)

	ctx := context.WithValue(context.Background(), actorKey{}, actor{"u-1", `{"sub":"agent-7"}`})
	_, err := pc.Read(ctx, &probepb.ProbeRequest{})
	require.NoError(t, err)
	md := <-got
	require.Equal(t, []string{"erp/sales"}, md.Get("be-caller"))
	require.Equal(t, []string{"req-42"}, md.Get("x-request-id"))
	require.Equal(t, []string{"u-1"}, md.Get("be-actor-sub"))
	require.Equal(t, []string{`{"sub":"agent-7"}`}, md.Get("be-actor-act"))

	// the same connection, another user: the actor is read at call time, never at dial time
	ctx = context.WithValue(context.Background(), actorKey{}, actor{sub: "u-2"})
	_, err = pc.Read(ctx, &probepb.ProbeRequest{})
	require.NoError(t, err)
	md = <-got
	require.Equal(t, []string{"u-2"}, md.Get("be-actor-sub"))
	require.Empty(t, md.Get("be-actor-act"))

	// background work: no actor at all
	_, err = pc.Read(context.Background(), &probepb.ProbeRequest{})
	require.NoError(t, err)
	md = <-got
	require.Empty(t, md.Get("be-actor-sub"))
	require.Equal(t, []string{"erp/sales"}, md.Get("be-caller"))
}

func TestClientOverwritesMetadataTheCodeSet(t *testing.T) {
	ts := startServer(t, nil)
	got := captureMD(ts)
	pc := probeClient(t, newTestConns(t, nil), "conformance/widget", ts.addr)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "be-caller", "forged", "be-actor-sub", "forged")
	_, err := pc.Read(ctx, &probepb.ProbeRequest{})
	require.NoError(t, err)
	md := <-got
	require.Equal(t, []string{"erp/sales"}, md.Get("be-caller"))
	require.Empty(t, md.Get("be-actor-sub"))
}

func TestClientInventsARequestIDWhenTheContextHasNone(t *testing.T) {
	ts := startServer(t, nil)
	got := captureMD(ts)
	pc := probeClient(t, newTestConns(t, nil), "conformance/widget", ts.addr)
	_, err := pc.Read(context.Background(), &probepb.ProbeRequest{})
	require.NoError(t, err)
	require.Len(t, (<-got).Get("x-request-id"), 1)
}

// deadlineSeen makes the server report the remaining time of each call.
func deadlineSeen(ts *testServer) chan time.Duration {
	got := make(chan time.Duration, 4)
	ts.probe.set(func(ctx context.Context, _, _ string) (string, error) {
		d, _ := ctx.Deadline()
		got <- time.Until(d)
		return "ok", nil
	})
	return got
}

func TestClientDefaultDeadlineIs3s(t *testing.T) {
	ts := startServer(t, nil)
	got := deadlineSeen(ts)
	_, err := probeClient(t, newTestConns(t, nil), "d", ts.addr).Read(context.Background(), &probepb.ProbeRequest{})
	require.NoError(t, err)
	left := <-got
	require.LessOrEqual(t, left, 3*time.Second)
	require.Greater(t, left, 2500*time.Millisecond)
}

func TestClientDeadlineIsRemainingMinus50ms(t *testing.T) {
	ts := startServer(t, nil)
	got := deadlineSeen(ts)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := probeClient(t, newTestConns(t, nil), "d", ts.addr).Read(ctx, &probepb.ProbeRequest{})
	require.NoError(t, err)
	left := <-got
	require.LessOrEqual(t, left, 950*time.Millisecond)
	require.Greater(t, left, 700*time.Millisecond)
}

func TestClientDoesNotSendWithUnder50msLeft(t *testing.T) {
	ts := startServer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err := probeClient(t, newTestConns(t, nil), "d", ts.addr).Read(ctx, &probepb.ProbeRequest{})
	require.True(t, problem.Is(err, "be", "DEADLINE_BUDGET_EXHAUSTED"), "got %v", err)
	require.Equal(t, codes.DeadlineExceeded, problem.From(err).Code)
	require.Zero(t, ts.probe.calls.Load())
}

func TestClientRefusesANetworkCallInsideATransaction(t *testing.T) {
	ts := startServer(t, nil)
	c := newTestConns(t, func(cfg *ClientConfig) {
		cfg.InTx = func(context.Context) bool { return true }
	})
	err := problem.Catch(func() error {
		_, err := probeClient(t, c, "d", ts.addr).Read(context.Background(), &probepb.ProbeRequest{})
		return err
	})
	require.True(t, problem.Is(err, "be", "NETWORK_IN_TX"), "got %v", err)
	require.Equal(t, codes.Internal, problem.From(err).Code)
	require.Zero(t, ts.probe.calls.Load())
}

func TestClientRestoresADomainError(t *testing.T) {
	ts := startServer(t, nil)
	ts.probe.set(func(context.Context, string, string) (string, error) {
		return "", problem.New(codes.FailedPrecondition, "erp/x", "FOO", map[string]string{"n": "2"}, "only 2 left")
	})
	_, err := probeClient(t, newTestConns(t, nil), "erp/x", ts.addr).Read(context.Background(), &probepb.ProbeRequest{})
	var pe *problem.Error
	require.True(t, errors.As(err, &pe), "got %T %v", err, err)
	require.Equal(t, codes.FailedPrecondition, pe.Code)
	require.Equal(t, "erp/x", pe.Domain)
	require.Equal(t, "FOO", pe.Reason)
	require.Equal(t, map[string]string{"n": "2"}, pe.Metadata)
	require.Equal(t, "only 2 left", pe.Detail)
}

// failFirst makes the server answer UNAVAILABLE to the first n attempts of each method.
func failFirst(ts *testServer, n int64) map[string]*atomic.Int64 {
	counts := map[string]*atomic.Int64{"Read": {}, "Touch": {}, "Create": {}}
	ts.probe.set(func(_ context.Context, method, _ string) (string, error) {
		if counts[method].Add(1) <= n {
			return "", problem.Be("NOT_READY", nil)
		}
		return "ok", nil
	})
	return counts
}

func TestClientRetriesIdempotentMethodsOnUnavailable(t *testing.T) {
	ts := startServer(t, nil)
	counts := failFirst(ts, 2)
	pc := probeClient(t, newTestConns(t, nil), "d", ts.addr)
	_, err := pc.Read(context.Background(), &probepb.ProbeRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 3, counts["Read"].Load())
	_, err = pc.Touch(context.Background(), &probepb.ProbeRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 3, counts["Touch"].Load())
}

func TestClientStopsAfterThreeAttempts(t *testing.T) {
	ts := startServer(t, nil)
	counts := failFirst(ts, 100)
	_, err := probeClient(t, newTestConns(t, nil), "d", ts.addr).Read(context.Background(), &probepb.ProbeRequest{})
	require.Equal(t, codes.Unavailable, problem.From(err).Code)
	require.EqualValues(t, 3, counts["Read"].Load())
}

func TestClientDoesNotRetryOtherMethods(t *testing.T) {
	ts := startServer(t, nil)
	counts := failFirst(ts, 2)
	_, err := probeClient(t, newTestConns(t, nil), "d", ts.addr).Create(context.Background(), &probepb.ProbeRequest{})
	require.Equal(t, codes.Unavailable, problem.From(err).Code)
	require.EqualValues(t, 1, counts["Create"].Load())
}

func TestClientOneConnectionPerDependencyUnderLoad(t *testing.T) {
	ts := startServer(t, nil)
	c := newTestConns(t, nil)
	first, err := c.Get("conformance/widget", ts.addr)
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := c.Get("conformance/widget", ts.addr)
			if err == nil && conn != first {
				err = errors.New("another ClientConn")
			}
			if err == nil {
				_, err = probepb.NewProbeClient(conn).Read(context.Background(), &probepb.ProbeRequest{})
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, ts.lis.accepted.Load())
}

func TestClientStripsTheSchemeOfTheTarget(t *testing.T) {
	ts := startServer(t, nil)
	_, err := probeClient(t, newTestConns(t, nil), "d", "http://"+ts.addr+"/").Read(context.Background(), &probepb.ProbeRequest{})
	require.NoError(t, err)
}

func TestClientGetRefusesEmptyArgumentsAndAfterClose(t *testing.T) {
	c := NewConns(ClientConfig{CallerID: "erp/sales"})
	_, err := c.Get("", "127.0.0.1:1")
	require.Error(t, err)
	_, err = c.Get("d", "")
	require.Error(t, err)
	require.NoError(t, c.Close())
	_, err = c.Get("d", "127.0.0.1:1")
	require.Error(t, err)
}

func TestClientMetricsAndInflight(t *testing.T) {
	ts := startServer(t, nil)
	m := &recordingClientMetrics{}
	pc := probeClient(t, newTestConns(t, func(cfg *ClientConfig) { cfg.Metrics = m }), "conformance/widget", ts.addr)
	_, err := pc.Read(context.Background(), &probepb.ProbeRequest{})
	require.NoError(t, err)
	require.Equal(t, []clientMetric{{"conformance/widget", "betest.rpc.v1.Probe/Read", "OK"}}, m.handled)
	require.Equal(t, 1, m.peak)
	require.Equal(t, 0, m.inflight["conformance/widget"])
}

func TestClientStreamCarriesCaller(t *testing.T) {
	ts := startStreamServer(t)
	conn, err := newTestConns(t, nil).Get("d", ts.addr)
	require.NoError(t, err)
	cs, err := conn.NewStream(context.Background(), &streamDesc.Streams[0], "/betest.rpc.v1.StreamProbe/Echo")
	require.NoError(t, err)
	require.NoError(t, cs.SendMsg(&probepb.BatchRequest{}))
	require.NoError(t, cs.CloseSend())
	var out probepb.ProbeReply
	require.NoError(t, cs.RecvMsg(&out))
	require.Equal(t, "erp/sales", out.GetNote())
}

var _ grpc.ClientConnInterface = (*grpc.ClientConn)(nil)
