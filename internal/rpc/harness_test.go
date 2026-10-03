package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

// probeFunc is what the test server does for one call: method is the rpc's short name, id the
// request's id (empty for BatchGet).
type probeFunc func(ctx context.Context, method, id string) (string, error)

type probeServer struct {
	probepb.UnimplementedProbeServer
	fn    atomic.Pointer[probeFunc]
	calls atomic.Int64
}

func (p *probeServer) set(fn probeFunc) {
	if fn == nil {
		p.fn.Store(nil)
		return
	}
	p.fn.Store(&fn)
}

func (p *probeServer) run(ctx context.Context, method, id string) (*probepb.ProbeReply, error) {
	p.calls.Add(1)
	fn := p.fn.Load()
	if fn == nil {
		return &probepb.ProbeReply{Note: method + ":ok"}, nil
	}
	note, err := (*fn)(ctx, method, id)
	if err != nil {
		return nil, err
	}
	return &probepb.ProbeReply{Note: note}, nil
}

func (p *probeServer) Read(ctx context.Context, r *probepb.ProbeRequest) (*probepb.ProbeReply, error) {
	return p.run(ctx, "Read", r.GetId())
}

func (p *probeServer) Touch(ctx context.Context, r *probepb.ProbeRequest) (*probepb.ProbeReply, error) {
	return p.run(ctx, "Touch", r.GetId())
}

func (p *probeServer) Create(ctx context.Context, r *probepb.ProbeRequest) (*probepb.ProbeReply, error) {
	return p.run(ctx, "Create", r.GetId())
}

func (p *probeServer) BatchGet(ctx context.Context, _ *probepb.BatchRequest) (*probepb.ProbeReply, error) {
	return p.run(ctx, "BatchGet", "")
}

// countingListener counts accepted TCP connections.
type countingListener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines decodes every JSON log line written so far.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &m))
		out = append(out, m)
	}
	return out
}

type serverMetric struct {
	service, method, code string
}

type recordingServerMetrics struct {
	mu   sync.Mutex
	seen []serverMetric
}

func (r *recordingServerMetrics) Handled(service, method, code string, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, serverMetric{service, method, code})
}

func (r *recordingServerMetrics) all() []serverMetric {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]serverMetric(nil), r.seen...)
}

type testServer struct {
	probe   *probeServer
	addr    string
	lis     *countingListener
	logs    *syncBuffer
	metrics *recordingServerMetrics
}

// startServer runs NewServer on 127.0.0.1:0 with the probe and health services; mutate adjusts the
// config before the server is built.
func startServer(t *testing.T, mutate func(*ServerConfig)) *testServer {
	t.Helper()
	return startServerWith(t, mutate, nil)
}

// startServerWith is startServer with extra services registered by register.
func startServerWith(t *testing.T, mutate func(*ServerConfig), register func(*grpc.Server)) *testServer {
	t.Helper()
	ts := &testServer{probe: &probeServer{}, logs: &syncBuffer{}, metrics: &recordingServerMetrics{}}
	cfg := ServerConfig{
		Logger:  slog.New(slog.NewJSONHandler(ts.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Metrics: ts.metrics,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	srv := NewServer(cfg)
	probepb.RegisterProbeServer(srv, ts.probe)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	if register != nil {
		register(srv)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ts.lis = &countingListener{Listener: l}
	ts.addr = l.Addr().String()
	go func() { _ = srv.Serve(ts.lis) }()
	t.Cleanup(srv.Stop)
	return ts
}

// rawClient dials the server with plain grpc-go: no SDK interceptors, metadata set by the test.
func rawClient(t *testing.T, addr string) probepb.ProbeClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return probepb.NewProbeClient(conn)
}

// errorInfo returns the ErrorInfo and BadRequest details of a status error.
func errorInfo(t *testing.T, err error) (*status.Status, *errdetails.ErrorInfo, *errdetails.BadRequest) {
	t.Helper()
	st, ok := status.FromError(err)
	require.True(t, ok, "not a status error: %v", err)
	var info *errdetails.ErrorInfo
	var br *errdetails.BadRequest
	for _, d := range st.Details() {
		switch v := d.(type) {
		case *errdetails.ErrorInfo:
			info = v
		case *errdetails.BadRequest:
			br = v
		}
	}
	return st, info, br
}

const (
	timeoutShort = 2 * time.Second
	tick         = 5 * time.Millisecond
)
