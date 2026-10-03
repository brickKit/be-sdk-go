package rpc

import (
	"context"
	"io"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// Server parameters of P7.4 and P7.5.
const (
	DefaultMaxConnectionAge = 5 * time.Minute  // GRPC_MAX_CONNECTION_AGE default
	MaxConnectionAgeGrace   = 30 * time.Second // longer than every deadline the server accepts
	KeepaliveMinTime        = 20 * time.Second // pings more often are refused
	MaxRecvMsgSize          = 4 << 20          // 4 MiB
	InboundDeadlineFloor    = 10 * time.Second // when the caller sent no grpc-timeout
)

// ServerMetrics receives one observation per finished inbound call, for
// be_grpc_server_handled_total{service,method,code} and be_grpc_server_duration_seconds (P18.3).
// code is the canonical upper-snake name of the status the caller received.
type ServerMetrics interface {
	Handled(service, method, code string, elapsed time.Duration)
}

// ServerConfig configures the gRPC server of one member (P7.4, P7.5). Every field is per member;
// nothing falls back to a process global.
type ServerConfig struct {
	MaxConnectionAge time.Duration      // GRPC_MAX_CONNECTION_AGE; zero means DefaultMaxConnectionAge
	Logger           *slog.Logger       // the member's logger; nil discards
	Catalogue        *problem.Catalogue // renders error text; nil means the be reasons only
	Locale           string             // DEFAULT_LOCALE, the language of status messages
	// Instrumentation, passed explicitly to otelgrpc (never the globals, P18.1); nil means no-op.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator
	Metrics        ServerMetrics // nil records nothing
	// Inbound, when set, derives the context component code runs in from the call's request ID
	// (x-request-id, or a new one) and system principal, e.g. to add request-scoped log fields.
	Inbound func(ctx context.Context, requestID string, c Caller) context.Context
	// Domain is the member's component ID: an error with a reason but no domain (besdk.Errorf) is its.
	Domain string
	// UserFacing lists the full method names ("/pkg.Service/Method") of user-facing rpcs kept in the
	// contract: they answer UNAUTHENTICATED / TOKEN_INVALID before any component code runs, because a
	// system call never carries a user token (P7.3, stage-B ruling).
	UserFacing []string
}

// NewServer returns the gRPC server of one member with the parameters of P7.5 and the interceptor
// chain of P7.4, unary and streaming alike: panic recovery with error normalisation, the access log
// and RED metrics (outermost, so every outcome is normalised, logged and counted) → identity →
// deadline floor → decode check (REQUEST_INVALID) and batch limit → the service; a user-facing rpc
// (ServerConfig.UserFacing) is refused at the identity layer. Tracing is otelgrpc's stats handler, which encloses
// the whole call. Services are registered on the returned server as usual (generated
// Register…Server); the batch limits cover all of them, because they are read from each request's
// own message descriptor.
func NewServer(c ServerConfig) *grpc.Server {
	s := newServerChain(c)
	age := c.MaxConnectionAge
	if age <= 0 {
		age = DefaultMaxConnectionAge
	}
	return grpc.NewServer(
		grpc.MaxRecvMsgSize(MaxRecvMsgSize),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge: age, MaxConnectionAgeGrace: MaxConnectionAgeGrace}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: KeepaliveMinTime, PermitWithoutStream: false}),
		grpc.ForceServerCodecV2(newRequestCodec(&s.bad)),
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelOptions(c.TracerProvider, c.MeterProvider, c.Propagator)...)),
		grpc.ChainUnaryInterceptor(s.reportUnary, s.identityUnary, s.deadlineUnary, s.batchUnary),
		grpc.ChainStreamInterceptor(s.reportStream, s.identityStream, s.deadlineStream, s.batchStream),
	)
}

// otelOptions passes the member's providers and the propagator explicitly; a nil one becomes a
// no-op, so otelgrpc never reads the process globals (P18.1, r1-01).
func otelOptions(tp trace.TracerProvider, mp metric.MeterProvider, p propagation.TextMapPropagator) []otelgrpc.Option {
	if tp == nil {
		tp = tracenoop.NewTracerProvider()
	}
	if mp == nil {
		mp = metricnoop.NewMeterProvider()
	}
	if p == nil {
		p = propagation.NewCompositeTextMapPropagator()
	}
	return []otelgrpc.Option{otelgrpc.WithTracerProvider(tp), otelgrpc.WithMeterProvider(mp), otelgrpc.WithPropagators(p)}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
