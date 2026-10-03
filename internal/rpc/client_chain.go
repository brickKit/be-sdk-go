package rpc

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// bulkhead counts the calls in flight to one dependency; it never queues (P7.9).
type bulkhead struct {
	limit    int64
	inflight atomic.Int64
}

func (b *bulkhead) acquire() bool {
	if b.inflight.Add(1) > b.limit {
		b.inflight.Add(-1)
		return false
	}
	return true
}

func (b *bulkhead) release() { b.inflight.Add(-1) }

// clientChain holds the client interceptors of one dependency, in the order of P7.12: default
// deadline → bulkhead → metadata → RED metrics → transaction guard.
type clientChain struct {
	dep      string
	cfg      *ClientConfig
	log      *slog.Logger
	bulkhead *bulkhead
}

// outboundContext applies P7.7: min(3 s, remaining − 50 ms); with 50 ms or less left the call is
// not sent (DEADLINE_BUDGET_EXHAUSTED).
func outboundContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	budget := OutboundDeadline
	if d, ok := ctx.Deadline(); ok {
		left := time.Until(d) - OutboundDeadlineSlack
		if left <= 0 {
			return ctx, func() {}, problem.Be("DEADLINE_BUDGET_EXHAUSTED", nil)
		}
		budget = min(budget, left)
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	return ctx, cancel, nil
}

// restore returns every error a call ends with as a *problem.Error, the dependency's domain and
// reason kept (P4.2, P4.9).
func restore(err error) error {
	if err == nil {
		return nil
	}
	return problem.From(err)
}

func (ch *clientChain) deadlineUnary(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, cancel, err := outboundContext(ctx)
	defer cancel()
	if err != nil {
		return err
	}
	return restore(invoker(ctx, method, req, reply, cc, opts...))
}

func (ch *clientChain) bulkheadUnary(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if !ch.bulkhead.acquire() {
		return problem.Be("OUTBOUND_LIMIT", nil)
	}
	defer ch.bulkhead.release()
	return invoker(ctx, method, req, reply, cc, opts...)
}

// outgoing sets the metadata of P7.2 on the call, replacing whatever the code put there: be-caller
// always, x-request-id (the context's, or a new one), and the actor read from the context now.
func (ch *clientChain) outgoing(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set(MDCaller, ch.cfg.CallerID)
	rid := ""
	if ch.cfg.RequestID != nil {
		rid = ch.cfg.RequestID(ctx)
	}
	if rid == "" {
		rid = newRequestID()
	}
	md.Set(MDRequestID, rid)
	md.Delete(MDActorSub)
	md.Delete(MDActorAct)
	if ch.cfg.Actor != nil {
		sub, act := ch.cfg.Actor(ctx)
		if sub != "" {
			md.Set(MDActorSub, sub)
		}
		if act != "" {
			md.Set(MDActorAct, act)
		}
	}
	return metadata.NewOutgoingContext(ctx, md)
}

func (ch *clientChain) metadataUnary(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(ch.outgoing(ctx), method, req, reply, cc, opts...)
}

// observe records one finished call and returns a function that ends the in-flight count.
func (ch *clientChain) observe() func(method string, err error) {
	start := time.Now()
	m := ch.cfg.Metrics
	if m == nil {
		return func(string, error) {}
	}
	m.Inflight(ch.dep, 1)
	return func(method string, err error) {
		m.Inflight(ch.dep, -1)
		code := codes.OK
		if err != nil {
			code = problem.From(err).Code
		}
		m.Handled(ch.dep, strings.TrimPrefix(method, "/"), problem.CodeName(code), time.Since(start))
	}
}

func (ch *clientChain) metricsUnary(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	done := ch.observe()
	err := invoker(ctx, method, req, reply, cc, opts...)
	done(method, err)
	return err
}

// inTx is the transaction guard of P8.4: a call started while the unit of work holds a transaction
// is a programming error, refused before anything is sent.
func (ch *clientChain) inTx(ctx context.Context, method string) error {
	if ch.cfg.InTx == nil || !ch.cfg.InTx(ctx) {
		return nil
	}
	ch.log.LogAttrs(ctx, slog.LevelError, "grpc call inside a transaction refused",
		slog.String("rpc.target", ch.dep), slog.String("rpc.method", strings.TrimPrefix(method, "/")),
		slog.String("error.reason", "NETWORK_IN_TX"))
	return problem.Abort(problem.Be("NETWORK_IN_TX", nil))
}

func (ch *clientChain) txGuardUnary(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if err := ch.inTx(ctx, method); err != nil {
		return err
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}
