package grpcx_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/internal/echotest"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func init() { gin.SetMode(gin.ReleaseMode) }

type sayFunc = echotest.Say

var echoDesc = echotest.Desc

func deps(id string) grpcx.Deps {
	m := obs.Member{ID: id, Version: "3.0.0"}
	return grpcx.Deps{Member: m, Logger: obs.NewLogger(m, slog.LevelError, io.Discard), Registry: prometheus.NewRegistry(),
		Tracer: sdktrace.NewTracerProvider().Tracer(id)}
}

// serveGRPC starts a member's system surface and returns its address.
func serveGRPC(t *testing.T, id string, say sayFunc) string {
	var s grpcx.Services
	s.Register(&echoDesc, say, map[string]grpcx.Method{"Say": {Operation: id + ".say"}})
	srv, err := grpcx.NewServer(deps(id), &s)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

func member(t *testing.T) *calls.Member {
	m, err := calls.NewMember(prometheus.NewRegistry())
	require.NoError(t, err)
	t.Cleanup(m.Close)
	return m
}

type seen struct {
	mu        sync.Mutex
	requestID string
	traceID   string
	remaining time.Duration
}

func (s *seen) record(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestID = reqctx.RequestID(ctx)
	s.traceID = trace.SpanContextFromContext(ctx).TraceID().String()
	dl, _ := ctx.Deadline()
	s.remaining = time.Until(dl)
}

func TestGRPCChainRetriesAtOneLayerOnly(t *testing.T) {
	var cHits, bHits atomic.Int32
	var atC, atB seen
	cAddr := serveGRPC(t, "demo/c", func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		cHits.Add(1)
		atC.record(ctx)
		return nil, errs.New(errs.ReasonServiceBusy, "c is busy")
	})
	bOut := member(t)
	bAddr := serveGRPC(t, "demo/b", func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		bHits.Add(1)
		atB.record(ctx)
		conn, err := bOut.GRPC("demo/c", cAddr)
		if err != nil {
			return nil, err
		}
		out := new(wrapperspb.StringValue)
		return out, conn.Invoke(ctx, "/demo.Echo/Say", in, out, calls.Idempotent())
	})

	aOut := member(t)
	conn, err := aOut.GRPC("demo/b", bAddr)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(reqctx.WithRequestID(context.Background(), "rid-chain-1"), time.Second)
	defer cancel()
	err = conn.Invoke(ctx, "/demo.Echo/Say", wrapperspb.String("hi"), new(wrapperspb.StringValue), calls.Idempotent())

	assert.Equal(t, int32(3), cHits.Load(), "B retried C: the only layer that retries")
	assert.Equal(t, int32(1), bHits.Load(), "A did not retry B's relayed failure")
	e := errs.From(err)
	assert.Equal(t, errs.ReasonDependencyUnavailable, e.Reason)
	assert.Equal(t, "demo/c", e.Metadata["dependency"], "the relayed error reached A unchanged")
	assert.Equal(t, calls.Closed, bOut.Dependency("demo/c", "grpc").Breaker().State())
	assert.Equal(t, 10000, aOut.Dependency("demo/b", "grpc").Budget().Tokens(), "A's budget untouched by a relayed failure")

	assert.Equal(t, "rid-chain-1", atB.requestID)
	assert.Equal(t, "rid-chain-1", atC.requestID, "the request ID travels the whole chain")
	assert.Equal(t, atB.traceID, atC.traceID, "one trace")
	assert.LessOrEqual(t, atB.remaining, 950*time.Millisecond)
	assert.Less(t, atC.remaining, atB.remaining-40*time.Millisecond, "the deadline only shrinks")
}

func TestGRPCComponentErrorsPassThroughUnchanged(t *testing.T) {
	short := errs.Define("demo/c", "STOCK_SHORT", errs.FailedPrecondition, 0, errs.RetryNo, "Not enough stock")
	cAddr := serveGRPC(t, "demo/c", func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, short.New("only 2 left").With("sku", "A-1").WithViolations(errs.Violation{Field: "/qty", Reason: "TOO_LARGE"})
	})
	conn, err := member(t).GRPC("demo/c", cAddr)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = conn.Invoke(ctx, "/demo.Echo/Say", wrapperspb.String("x"), new(wrapperspb.StringValue), calls.IdempotencyKey("k1"))
	e := errs.From(err)
	assert.Equal(t, "demo/c", e.Domain)
	assert.Equal(t, "STOCK_SHORT", e.Reason)
	assert.Equal(t, errs.FailedPrecondition, e.Code)
	assert.Equal(t, "A-1", e.Metadata["sku"])
	assert.Equal(t, []errs.Violation{{Field: "/qty", Reason: "TOO_LARGE"}}, e.Violations)
}

func TestUndeclaredMethodsAreRefused(t *testing.T) {
	var s grpcx.Services
	s.Register(&echoDesc, sayFunc(nil), map[string]grpcx.Method{"Shout": {Operation: "x"}})
	_, err := grpcx.NewServer(deps("demo/x"), &s)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "/demo.Echo/Say: no operation declared")
	assert.Contains(t, err.Error(), "/demo.Echo/Shout: declared but not a method")
}

// ---------------------------------------------------------------- user surface

func serveHTTP(t *testing.T, id string, routes *httpx.Routes) string {
	d := deps(id)
	e, err := httpx.NewEngine(httpx.Deps{Member: d.Member, Logger: d.Logger, Registry: d.Registry, Tracer: d.Tracer,
		Platform: httpx.Platform{Ready: func() (bool, string, []string) { return true, "ready", nil }, Describe: func() any { return nil }}}, routes)
	require.NoError(t, err)
	s := httptest.NewServer(e)
	t.Cleanup(s.Close)
	return s.URL
}

func TestHTTPChainRetriesAtOneLayerOnlyAndForwardsTheUser(t *testing.T) {
	var cHits, bHits atomic.Int32
	var auth, rid, timeoutMs string
	var mu sync.Mutex
	var c httpx.Routes
	c.GET("/api/c", "demo.c.read", func(x *httpx.Context) error {
		cHits.Add(1)
		mu.Lock()
		auth, rid, timeoutMs = x.Request().Header.Get("Authorization"), x.Request().Header.Get(calls.HeaderRequestID),
			x.Request().Header.Get(calls.HeaderTimeoutMs)
		mu.Unlock()
		return errs.New(errs.ReasonServiceBusy, "c is busy").WithRetryAfter(time.Second)
	})
	cURL := serveHTTP(t, "demo/c", &c)
	bOut := member(t)
	var b httpx.Routes
	b.GET("/api/b", "demo.b.read", func(x *httpx.Context) error {
		bHits.Add(1)
		return bOut.HTTP("demo/c", cURL).Do(x.Ctx(), calls.Request{Method: "GET", Path: "/api/c", ForwardUser: true}, nil)
	})
	bURL := serveHTTP(t, "demo/b", &b)

	ctx, cancel := context.WithTimeout(reqctx.WithUserCredential(reqctx.WithRequestID(context.Background(), "rid-http-1"),
		"Bearer user-token"), 2*time.Second)
	defer cancel()
	err := member(t).HTTP("demo/b", bURL).Do(ctx, calls.Request{Method: "GET", Path: "/api/b", ForwardUser: true}, nil)

	assert.Equal(t, int32(3), cHits.Load())
	assert.Equal(t, int32(1), bHits.Load())
	e := errs.From(err)
	assert.Equal(t, errs.ReasonDependencyUnavailable, e.Reason)
	assert.Equal(t, "demo/c", e.Metadata["dependency"])
	assert.Equal(t, time.Second, e.RetryAfter, "C's Retry-After travels up with the failure")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "Bearer user-token", auth, "the user's own credential reached C")
	assert.Equal(t, "rid-http-1", rid)
	assert.NotEmpty(t, timeoutMs)
}

func TestHTTPOutboundLimitAnswerIsNotRetried(t *testing.T) {
	var hits atomic.Int32
	var c httpx.Routes
	c.GET("/api/c", "demo.c.read", func(*httpx.Context) error {
		hits.Add(1)
		return errs.New(errs.ReasonOutboundLimit, "full") // 503, but RESOURCE_EXHAUSTED
	})
	cURL := serveHTTP(t, "demo/c", &c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := member(t).HTTP("demo/c", cURL).Do(ctx, calls.Request{Method: "GET", Path: "/api/c"}, nil)
	assert.Equal(t, int32(1), hits.Load())
	assert.True(t, errs.Is(err, "be", errs.ReasonOutboundLimit), "%v", err)
}

func TestHTTPConnectFailureIsRetriedThenReported(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	m := member(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = m.HTTP("demo/gone", "http://"+addr).Do(ctx, calls.Request{Method: "GET", Path: "/api/x"}, nil)
	assert.True(t, errs.Is(err, "be", errs.ReasonDependencyUnavailable), "%v", err)
	assert.Equal(t, 10000-3*1000, m.Dependency("demo/gone", "http").Budget().Tokens(), "three attempts, each unavailable")
}
