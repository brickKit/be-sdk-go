package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Client parameters of P7.6, P7.7 and P7.9.
const (
	DefaultBulkhead       = 64              // concurrent calls per (member, dependency)
	OutboundDeadline      = 3 * time.Second // the longest outbound deadline
	OutboundDeadlineSlack = 50 * time.Millisecond
	KeepaliveTime         = 30 * time.Second // ping after this long idle on an active call
	KeepaliveTimeout      = 10 * time.Second
)

// ClientMetrics receives the client RED metrics (P18.3): one Handled per finished call for
// be_grpc_client_handled_total{target,method,code} and be_grpc_client_duration_seconds, and
// Inflight(+1 / −1) around each call for be_outbound_inflight{target}. target is the dependency ID,
// method the full rpc name without the leading slash ("erp.inventory.v1.InventoryService/Reserve").
type ClientMetrics interface {
	Handled(target, method, code string, elapsed time.Duration)
	Inflight(target string, delta int)
}

// ClientConfig configures the outbound gRPC connections of one member (P7.6–P7.9, P7.12).
type ClientConfig struct {
	CallerID string       // be-caller: the member's component ID
	Logger   *slog.Logger // nil discards
	// Instrumentation, passed explicitly to otelgrpc (never the globals, P18.1); nil means no-op.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator
	Bulkhead       int // concurrent calls per dependency; zero means DefaultBulkhead
	// Hooks read at call time from the call's context; nil hooks mean "none".
	RequestID func(ctx context.Context) string                // "" → a new UUIDv7
	Actor     func(ctx context.Context) (sub, actJSON string) // the user behind the call
	InTx      func(ctx context.Context) bool                  // the unit of work holds a transaction
	Metrics   ClientMetrics                                   // nil records nothing
}

type connKey struct{ dep, target string }

// Conns holds one member's outbound connections: one lazily created ClientConn per (dependency,
// target), reused by every call (P7.6), and one bulkhead per dependency (P7.9). Safe for concurrent
// use.
type Conns struct {
	cfg ClientConfig
	log *slog.Logger

	scOnce sync.Once
	sc     string
	scErr  error

	mu        sync.Mutex
	closed    bool
	conns     map[connKey]*grpc.ClientConn
	bulkheads map[string]*bulkhead
}

// NewConns returns the outbound connection set of one member. Nothing is dialled until Get.
func NewConns(c ClientConfig) *Conns {
	if c.Bulkhead <= 0 {
		c.Bulkhead = DefaultBulkhead
	}
	log := c.Logger
	if log == nil {
		log = discardLogger()
	}
	return &Conns{cfg: c, log: log, conns: map[connKey]*grpc.ClientConn{}, bulkheads: map[string]*bulkhead{}}
}

// Get returns the connection to a dependency (P7.6): dep is its component ID, target its gRPC
// address (`<DEP>_GRPC_ENDPOINT` or a family's `*_GRPC_URL`; a leading http:// and a trailing / are
// stripped). The first call creates the connection with the retry service config of every service
// in protoregistry.GlobalFiles (P7.8) and the client chain of P7.12; later calls return the same one.
func (c *Conns) Get(dep, target string) (*grpc.ClientConn, error) {
	target = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(target, "http://"), "https://"), "/")
	if dep == "" || target == "" {
		return nil, fmt.Errorf("rpc: dependency %q at %q: both are required", dep, target)
	}
	c.scOnce.Do(func() { c.sc, c.scErr = DefaultServiceConfig(protoregistry.GlobalFiles) })
	if c.scErr != nil {
		return nil, fmt.Errorf("rpc: service config: %w", c.scErr)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("rpc: connections are closed")
	}
	key := connKey{dep, target}
	if conn, ok := c.conns[key]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(target, c.dialOptions(dep, c.bulkheadOf(dep))...)
	if err != nil {
		return nil, fmt.Errorf("rpc: dependency %s at %s: %w", dep, target, err)
	}
	c.conns[key] = conn
	return conn, nil
}

// bulkheadOf returns the dependency's bulkhead; the caller holds c.mu.
func (c *Conns) bulkheadOf(dep string) *bulkhead {
	b, ok := c.bulkheads[dep]
	if !ok {
		b = &bulkhead{limit: int64(c.cfg.Bulkhead)}
		c.bulkheads[dep] = b
	}
	return b
}

func (c *Conns) dialOptions(dep string, b *bulkhead) []grpc.DialOption {
	ch := &clientChain{dep: dep, cfg: &c.cfg, log: c.log, bulkhead: b}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: KeepaliveTime, Timeout: KeepaliveTimeout, PermitWithoutStream: false}),
		grpc.WithDefaultServiceConfig(c.sc),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelOptions(c.cfg.TracerProvider, c.cfg.MeterProvider, c.cfg.Propagator)...)),
		grpc.WithChainUnaryInterceptor(ch.deadlineUnary, ch.bulkheadUnary, ch.metadataUnary, ch.metricsUnary, ch.txGuardUnary),
		grpc.WithChainStreamInterceptor(ch.deadlineStream, ch.bulkheadStream, ch.metadataStream, ch.metricsStream, ch.txGuardStream),
	}
}

// Close closes every connection (at the member's stop); Get fails afterwards.
func (c *Conns) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	for k, conn := range c.conns {
		errs = append(errs, conn.Close())
		delete(c.conns, k)
	}
	return errors.Join(errs...)
}
