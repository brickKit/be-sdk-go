package rpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

func withCaller(ctx context.Context, kv ...string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, append([]string{"be-caller", "erp/sales"}, kv...)...)
}

func TestServerRejectsCallWithoutCaller(t *testing.T) {
	ts := startServer(t, nil)
	_, err := rawClient(t, ts.addr).Read(context.Background(), &probepb.ProbeRequest{Id: "1"})
	st, info, _ := errorInfo(t, err)
	require.Equal(t, codes.Unauthenticated, st.Code())
	require.NotNil(t, info)
	require.Equal(t, "MISSING_CALLER", info.GetReason())
	require.Equal(t, "be", info.GetDomain())
	require.Zero(t, ts.probe.calls.Load(), "component code must not run")
}

func TestServerPutsSystemPrincipalInContext(t *testing.T) {
	ts := startServer(t, nil)
	got := make(chan Caller, 1)
	ts.probe.set(func(ctx context.Context, _, _ string) (string, error) {
		c, ok := CallerFrom(ctx)
		require.True(t, ok)
		got <- c
		return "ok", nil
	})
	ctx := withCaller(context.Background(), "be-actor-sub", "u-1", "be-actor-act", `{"sub":"agent-7"}`)
	_, err := rawClient(t, ts.addr).Read(ctx, &probepb.ProbeRequest{Id: "1"})
	require.NoError(t, err)
	require.Equal(t, Caller{Caller: "erp/sales", ActorSub: "u-1", Act: `{"sub":"agent-7"}`}, <-got)
}

func TestCallerFromEmptyContext(t *testing.T) {
	_, ok := CallerFrom(context.Background())
	require.False(t, ok)
}

func TestHealthServiceIsExemptFromCaller(t *testing.T) {
	ts := startServer(t, nil)
	conn, err := grpc.NewClient(ts.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}

func TestServerDeadlineFloorWhenCallerSentNone(t *testing.T) {
	ts := startServer(t, nil)
	left := make(chan time.Duration, 1)
	ts.probe.set(func(ctx context.Context, _, _ string) (string, error) {
		d, ok := ctx.Deadline()
		require.True(t, ok, "a deadline must be set")
		left <- time.Until(d)
		return "ok", nil
	})
	_, err := rawClient(t, ts.addr).Read(withCaller(context.Background()), &probepb.ProbeRequest{})
	require.NoError(t, err)
	got := <-left
	require.Greater(t, got, 9*time.Second)
	require.LessOrEqual(t, got, 10*time.Second)
}

func TestServerKeepsTheCallersDeadline(t *testing.T) {
	ts := startServer(t, nil)
	left := make(chan time.Duration, 1)
	ts.probe.set(func(ctx context.Context, _, _ string) (string, error) {
		d, _ := ctx.Deadline()
		left <- time.Until(d)
		return "ok", nil
	})
	ctx, cancel := context.WithTimeout(withCaller(context.Background()), 2*time.Second)
	defer cancel()
	_, err := rawClient(t, ts.addr).Read(ctx, &probepb.ProbeRequest{})
	require.NoError(t, err)
	require.LessOrEqual(t, <-left, 2*time.Second)
}

func TestServerPanicBecomesInternalWithoutItsText(t *testing.T) {
	ts := startServer(t, nil)
	ts.probe.set(func(context.Context, string, string) (string, error) {
		panic("SELECT secret FROM vault")
	})
	_, err := rawClient(t, ts.addr).Read(withCaller(context.Background()), &probepb.ProbeRequest{})
	st, info, _ := errorInfo(t, err)
	require.Equal(t, codes.Internal, st.Code())
	require.NotContains(t, st.Message(), "secret")
	require.Equal(t, "INTERNAL", info.GetReason())
	require.Equal(t, "be", info.GetDomain())
	require.Empty(t, info.GetMetadata())

	// the server keeps serving, and the original goes to the log only, at ERROR
	ts.probe.set(nil)
	_, err = rawClient(t, ts.addr).Read(withCaller(context.Background()), &probepb.ProbeRequest{})
	require.NoError(t, err)
	line := findLog(t, ts, "Read", "INTERNAL")
	require.Equal(t, "ERROR", line["level"])
	require.Contains(t, line["error"], "SELECT secret FROM vault")
}

func TestServerBatchOverExplicitLimit(t *testing.T) {
	ts := startServer(t, nil)
	_, err := rawClient(t, ts.addr).BatchGet(withCaller(context.Background()), &probepb.BatchRequest{Ids: ids(4)})
	st, info, br := errorInfo(t, err)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Equal(t, "BATCH_TOO_LARGE", info.GetReason())
	require.Equal(t, "be", info.GetDomain())
	require.Equal(t, map[string]string{"field": "ids", "max": "3", "got": "4"}, info.GetMetadata())
	require.NotNil(t, br)
	require.Len(t, br.GetFieldViolations(), 1)
	require.Equal(t, "ids", br.GetFieldViolations()[0].GetField())
	require.Zero(t, ts.probe.calls.Load())
}

func TestServerBatchOverDefaultLimit(t *testing.T) {
	ts := startServer(t, nil)
	c := rawClient(t, ts.addr)
	_, err := c.BatchGet(withCaller(context.Background()), &probepb.BatchRequest{Tags: ids(500)})
	require.NoError(t, err)
	_, err = c.BatchGet(withCaller(context.Background()), &probepb.BatchRequest{Tags: ids(501)})
	_, info, br := errorInfo(t, err)
	require.Equal(t, map[string]string{"field": "tags", "max": "500", "got": "501"}, info.GetMetadata())
	require.NotNil(t, br)
}

func TestServerKeepsADomainError(t *testing.T) {
	ts := startServer(t, nil)
	ts.probe.set(func(context.Context, string, string) (string, error) {
		return "", problem.New(codes.FailedPrecondition, "erp/x", "FOO", map[string]string{"n": "2"}, "only 2 left")
	})
	_, err := rawClient(t, ts.addr).Read(withCaller(context.Background()), &probepb.ProbeRequest{})
	st, info, _ := errorInfo(t, err)
	require.Equal(t, codes.FailedPrecondition, st.Code())
	require.Equal(t, "only 2 left", st.Message())
	require.Equal(t, "erp/x", info.GetDomain())
	require.Equal(t, "FOO", info.GetReason())
	require.Equal(t, map[string]string{"n": "2"}, info.GetMetadata())
}

func TestServerHidesAPlainError(t *testing.T) {
	ts := startServer(t, nil)
	ts.probe.set(func(context.Context, string, string) (string, error) {
		return "", errors.New(`pq: relation "secret_table" does not exist`)
	})
	_, err := rawClient(t, ts.addr).Read(withCaller(context.Background()), &probepb.ProbeRequest{})
	st, info, _ := errorInfo(t, err)
	require.Equal(t, codes.Internal, st.Code())
	require.NotContains(t, st.Message(), "secret_table")
	require.Equal(t, "INTERNAL", info.GetReason())
}

func TestServerRefusesAMessageOver4MiB(t *testing.T) {
	ts := startServer(t, nil)
	conn, err := grpc.NewClient(ts.addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(16<<20)))
	require.NoError(t, err)
	defer conn.Close()
	big := strings.Repeat("x", 5<<20)
	_, err = probepb.NewProbeClient(conn).Read(withCaller(context.Background()), &probepb.ProbeRequest{Id: big})
	st, _, _ := errorInfo(t, err)
	require.Equal(t, codes.ResourceExhausted, st.Code())
	require.Zero(t, ts.probe.calls.Load())
}

// findLog returns the grpc_request line of a method with the given ErrorInfo reason ("" = success).
func findLog(t *testing.T, ts *testServer, method, reason string) map[string]any {
	t.Helper()
	for _, l := range ts.logs.lines(t) {
		if l["msg"] == "grpc_request" && l["rpc.method"] == method && (reason == "" && l["error.reason"] == nil ||
			reason != "" && l["error.reason"] == reason) {
			return l
		}
	}
	t.Fatalf("no grpc_request line for %s %s in %v", method, reason, ts.logs.lines(t))
	return nil
}

func TestServerAccessLogLine(t *testing.T) {
	ts := startServer(t, nil)
	ctx := withCaller(context.Background(), "x-request-id", "req-1")
	_, err := rawClient(t, ts.addr).Read(ctx, &probepb.ProbeRequest{})
	require.NoError(t, err)
	line := findLog(t, ts, "Read", "")
	require.Equal(t, "INFO", line["level"])
	require.Equal(t, "betest.rpc.v1.Probe", line["rpc.service"])
	require.Equal(t, "OK", line["rpc.grpc.status_code"])
	require.Equal(t, "erp/sales", line["caller"])
	require.Equal(t, "req-1", line["request_id"])
	require.Contains(t, line, "duration_ms")
}

func TestServerLogLevelFollowsTheCode(t *testing.T) {
	ts := startServer(t, nil)
	_, err := rawClient(t, ts.addr).BatchGet(withCaller(context.Background()), &probepb.BatchRequest{Ids: ids(9)})
	require.Error(t, err)
	line := findLog(t, ts, "BatchGet", "BATCH_TOO_LARGE")
	require.Equal(t, "INFO", line["level"])
	require.Equal(t, "INVALID_ARGUMENT", line["rpc.grpc.status_code"])
	require.Equal(t, "INVALID_ARGUMENT", line["error.code"])
}

func TestServerMetricsSeeEveryOutcome(t *testing.T) {
	ts := startServer(t, nil)
	c := rawClient(t, ts.addr)
	_, _ = c.Read(withCaller(context.Background()), &probepb.ProbeRequest{})
	_, _ = c.Read(context.Background(), &probepb.ProbeRequest{})
	require.Equal(t, []serverMetric{
		{"betest.rpc.v1.Probe", "Read", "OK"},
		{"betest.rpc.v1.Probe", "Read", "UNAUTHENTICATED"},
	}, ts.metrics.all())
}

func TestServerInboundHookGetsRequestIDAndCaller(t *testing.T) {
	type seen struct {
		id string
		c  Caller
	}
	got := make(chan seen, 1)
	ts := startServer(t, func(c *ServerConfig) {
		c.Inbound = func(ctx context.Context, requestID string, caller Caller) context.Context {
			got <- seen{requestID, caller}
			return ctx
		}
	})
	_, err := rawClient(t, ts.addr).Read(withCaller(context.Background(), "x-request-id", "r-9"), &probepb.ProbeRequest{})
	require.NoError(t, err)
	require.Equal(t, seen{"r-9", Caller{Caller: "erp/sales"}}, <-got)
}

func TestServerInventsARequestIDWhenNoneCame(t *testing.T) {
	got := make(chan string, 1)
	ts := startServer(t, func(c *ServerConfig) {
		c.Inbound = func(ctx context.Context, requestID string, _ Caller) context.Context {
			got <- requestID
			return ctx
		}
	})
	_, err := rawClient(t, ts.addr).Read(withCaller(context.Background()), &probepb.ProbeRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, <-got)
}

func TestServerMaxConnectionAgeRotatesConnectionsWithoutErrors(t *testing.T) {
	ts := startServer(t, func(c *ServerConfig) { c.MaxConnectionAge = 200 * time.Millisecond })
	c := rawClient(t, ts.addr)
	end := time.Now().Add(1200 * time.Millisecond)
	for time.Now().Before(end) {
		_, err := c.Create(withCaller(context.Background()), &probepb.ProbeRequest{})
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}
	require.GreaterOrEqual(t, ts.lis.accepted.Load(), int64(3))
}

func TestServerRendersStatusMessagesInTheDefaultLocale(t *testing.T) {
	ts := startServer(t, func(c *ServerConfig) { c.Locale = "en" })
	_, err := rawClient(t, ts.addr).Read(context.Background(), &probepb.ProbeRequest{})
	st, _, _ := errorInfo(t, err)
	en := problem.NewCatalogue()
	_, want := en.Text(problem.Be("MISSING_CALLER", nil), "en")
	require.Equal(t, want, st.Message())
	_, zh := en.Text(problem.Be("MISSING_CALLER", nil), "zh")
	require.NotEqual(t, zh, st.Message())
}
