package besdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/httpx"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
	"google.golang.org/grpc"
)

// ErrDependencyAbsent: an optional dependency is not installed, so its address variable does not
// exist (P2.5); the component degrades.
var ErrDependencyAbsent = errors.New("besdk: dependency not installed")

// Outbound limits of P7.7 and P7.9.
const (
	outboundBulkhead = 64
	outboundDeadline = 3 * time.Second
	outboundSlack    = 50 * time.Millisecond
)

// outbound is one member's outbound side: cached gRPC connections, user-plane clients and the
// shared metrics.
type outbound struct {
	rt    *Runtime
	conns *rpc.Conns
	grpcM *telemetry.GRPCClientMetrics
	httpM *telemetry.HTTPClientMetrics
	inTx  func(context.Context) bool
	mu    sync.Mutex
	users map[string]*UserHTTP
}

func newOutbound(rt *Runtime, inTx func(context.Context) bool) (*outbound, error) {
	gm, err := telemetry.NewGRPCClientMetrics(rt.tel.Registerer())
	if err != nil {
		return nil, err
	}
	hm, err := telemetry.NewHTTPClientMetrics(rt.tel.Registerer())
	if err != nil {
		return nil, err
	}
	o := &outbound{rt: rt, grpcM: gm, httpM: hm, inTx: inTx, users: map[string]*UserHTTP{}}
	o.conns = rpc.NewConns(rpc.ClientConfig{CallerID: rt.id, Logger: rt.log, Bulkhead: outboundBulkhead,
		TracerProvider: rt.tel.TracerProvider(), MeterProvider: rt.tel.MeterProvider(), Propagator: rt.tel.Propagator(),
		RequestID: httpx.RequestID, Actor: actorOf, InTx: inTx, Metrics: grpcClientMetrics{gm}})
	return o, nil
}

func actorOf(ctx context.Context) (sub, act string) {
	if a, err := AccessFrom(ctx); err == nil {
		return a.user.Sub, actJSON(a.user.Act)
	}
	return "", ""
}

// Conn returns the member's cached connection to a dependency's named port (P7.6): one TCP
// connection in steady state, every call through the client chain of P7.12. An optional dependency
// that is not installed answers ErrDependencyAbsent.
func (rt *Runtime) Conn(dep, port string) (*grpc.ClientConn, error) {
	o, err := rt.outbound()
	if err != nil {
		return nil, err
	}
	addr, err := rt.endpoint(dep, port)
	if err != nil {
		return nil, err
	}
	return o.conns.Get(dep, addr)
}

// endpoint reads <DEP>[_<PORT>]_ENDPOINT and strips the scheme (P2.6).
func (rt *Runtime) endpoint(dep, port string) (string, error) {
	name, err := config.EndpointName(dep, port)
	if err != nil {
		return "", err
	}
	raw, present := rt.cfg.vals.String(name)
	addr, ok, err := config.Endpoint(raw, present)
	switch {
	case err != nil:
		return "", problem.Wrap(err, "INTERNAL", nil)
	case !ok:
		return "", ErrDependencyAbsent
	}
	return addr, nil
}

func (rt *Runtime) outbound() (*outbound, error) {
	if rt.deps.out == nil {
		return nil, fmt.Errorf("besdk: outbound calls are available once the runtime serves")
	}
	return rt.deps.out, nil
}

// budget is the outbound deadline of P7.7: min(3 s, remaining − 50 ms); under 50 ms the call is not
// sent.
func budget(ctx context.Context) (time.Duration, error) {
	d := outboundDeadline
	if dl, ok := ctx.Deadline(); ok {
		rem := time.Until(dl) - outboundSlack
		if rem <= 0 {
			return 0, problem.Be("DEADLINE_BUDGET_EXHAUSTED", nil)
		}
		d = min(d, rem)
	}
	return d, nil
}

func networkInTx(what string) error {
	e := problem.Be("NETWORK_IN_TX", map[string]string{"call": what})
	e.Cause = fmt.Errorf("outbound call %s inside a transaction (P8.4)", what)
	return e
}

func outboundLimit(dep string) error {
	return problem.Be("OUTBOUND_LIMIT", map[string]string{"target": dep})
}
