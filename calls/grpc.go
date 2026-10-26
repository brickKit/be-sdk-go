package calls

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Metadata keys of the system surface (P4.2).
const (
	MetadataRequestID      = "x-request-id"
	MetadataIdempotencyKey = "idempotency-key"
	MetadataAuthorization  = "authorization"
)

// Credentials supplies the service identity carried by system-surface calls ("Bearer <token>"). The identity chapter
// provides the implementation; without one no credential is sent.
type Credentials interface {
	ServiceAuthorization(ctx context.Context) (string, error)
}

// SetCredentials sets an additional service credential carried in metadata (a token-based identity profile).
func (m *Member) SetCredentials(c Credentials) { m.mu.Lock(); m.creds = c; m.mu.Unlock() }

// SetTLS sets the member's mTLS service identity; system-surface calls are made only with it (P8.5).
func (m *Member) SetTLS(t *identity.ServiceTLS) { m.mu.Lock(); m.tls = t; m.mu.Unlock() }

type idempotentOption struct{ grpc.EmptyCallOption }
type backgroundOption struct{ grpc.EmptyCallOption }
type keyOption struct {
	grpc.EmptyCallOption
	key string
}

// Idempotent marks a call to a method the contract declares free of side effects or idempotent: only such calls are
// retried (P4.6).
func Idempotent() grpc.CallOption { return idempotentOption{} }

// Background marks the SDK's own background call to the authorization member (view verification, definition
// submission; P4.27): one at a time per member on its own schedule, so the breaker neither counts it nor holds it.
func Background() grpc.CallOption { return backgroundOption{} }

// IdempotencyKey sends the command's idempotency key, the same on every attempt (P4.2).
func IdempotencyKey(key string) grpc.CallOption { return keyOption{key: key} }

// GRPC returns the member's connection to a dependency's system surface at target (host:port). Every unary call on
// it goes through the dependency's limit, breaker, budget and deadline (Dependency.Do).
func (m *Member) GRPC(id, target string) (*grpc.ClientConn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := id + " " + target
	if c, ok := m.conns[key]; ok {
		return c, nil
	}
	if m.tls == nil {
		return nil, fmt.Errorf("system surface of %s: this member has no service identity (SERVICE_TLS_*)", id)
	}
	dep := m.dependencyLocked(id, "grpc")
	// passthrough hands the host name to the dialer, which resolves it for every connection: a dependency that
	// restarted on another address (a container's new IP) is reached at once, where gRPC's own resolver waits at
	// least 30 s between resolutions (P4.28). The reconnection backoff is capped for the same reason.
	conn, err := grpc.NewClient("passthrough:///"+target,
		grpc.WithTransportCredentials(credentials.NewTLS(m.tls.ClientConfig(id))),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: reconnect, MinConnectTimeout: 5 * time.Second}),
		grpc.WithUnaryInterceptor(m.unary(dep)))
	if err != nil {
		return nil, fmt.Errorf("system surface of %s at %s: %w", id, target, err)
	}
	if m.conns == nil {
		m.conns = map[string]*grpc.ClientConn{}
	}
	m.conns[key] = conn
	return conn, nil
}

// reconnect is gRPC's default backoff with the longest pause cut from 120 s to 5 s: after a dependency has been
// down a while its connection is retried every few seconds, not every two minutes.
var reconnect = backoff.Config{BaseDelay: time.Second, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 5 * time.Second}

func (m *Member) unary(dep *Dependency) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		idempotent, background, key := false, false, ""
		for _, o := range opts {
			switch x := o.(type) {
			case idempotentOption:
				idempotent = true
			case backgroundOption:
				background = true
			case keyOption:
				key = x.key
			}
		}
		m.mu.Lock()
		creds := m.creds
		m.mu.Unlock()
		auth := ""
		if creds != nil {
			a, err := creds.ServiceAuthorization(ctx)
			if err != nil {
				return errs.From(err)
			}
			auth = a
		}
		return dep.do(ctx, idempotent, !background, func(actx context.Context) Result {
			md, _ := metadata.FromOutgoingContext(actx)
			md = md.Copy()
			if id := reqctx.RequestID(actx); id != "" {
				md.Set(MetadataRequestID, id)
			}
			if key != "" {
				md.Set(MetadataIdempotencyKey, key)
			}
			if auth != "" {
				md.Set(MetadataAuthorization, auth)
			}
			obs.Propagator.Inject(actx, MetadataCarrier(md))
			err := invoker(metadata.NewOutgoingContext(actx, md), method, req, reply, cc, opts...)
			if err == nil {
				return Result{Code: "OK"}
			}
			if remote, code, ok := errs.FromGRPC(err); ok {
				return Result{Err: remote, Code: code, Remote: remote}
			}
			switch {
			case errors.Is(actx.Err(), context.DeadlineExceeded):
				return Result{Err: err, Code: errs.DeadlineExceeded}
			case errors.Is(actx.Err(), context.Canceled):
				return Result{Err: err, Code: errs.Cancelled}
			}
			return Result{Err: err, Code: errs.CodeOf(status.Code(err))}
		})
	}
}

// MetadataCarrier adapts gRPC metadata to the trace propagator.
type MetadataCarrier metadata.MD

// Get returns the first value of a key.
func (c MetadataCarrier) Get(key string) string {
	if v := metadata.MD(c).Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

// Set sets a key.
func (c MetadataCarrier) Set(key, value string) { metadata.MD(c).Set(key, value) }

// Keys lists the keys.
func (c MetadataCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}
