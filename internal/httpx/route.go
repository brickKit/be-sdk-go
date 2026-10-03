package httpx

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

type holderKey struct{}

// holder is shared between the outer handler and the router inside it for one request: the route
// template and its deadline (known only after routing), the error a handler answered with, and the
// fields the access-log line must carry (sub, perm, …).
type holder struct {
	base     context.Context // cancelled when the outer handler returns
	routed   chan struct{}   // closed once SetRoute ran
	mu       sync.Mutex
	route    string
	deadline time.Duration
	err      *problem.Error
	attrs    []slog.Attr
	cleanup  []func()
	once     sync.Once
}

func withHolder(ctx context.Context, h *holder) context.Context {
	if h.base == nil {
		h.base = ctx
	}
	return context.WithValue(ctx, holderKey{}, h)
}

func holderOf(ctx context.Context) *holder {
	h, _ := ctx.Value(holderKey{}).(*holder)
	return h
}

// SetRoute records the matched route template and its deadline (P3.4, P3.13) and returns the request
// context bounded by that deadline instead of the default one. Called once per request by the router,
// before the guard and the handler.
func SetRoute(ctx context.Context, template string, deadline time.Duration) context.Context {
	h := holderOf(ctx)
	if h == nil {
		c, cancel := context.WithTimeout(ctx, deadline) // only without the outer handler (tests)
		context.AfterFunc(c, cancel)
		return c
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deadline)
	stop := context.AfterFunc(h.base, cancel)
	h.mu.Lock()
	h.route, h.deadline = template, deadline
	h.cleanup = append(h.cleanup, func() { stop(); cancel() })
	h.mu.Unlock()
	h.once.Do(func() { close(h.routed) })
	return rctx
}

// AddAccessAttrs adds fields to the request's access-log line (P18.4: sub and perm).
func AddAccessAttrs(ctx context.Context, attrs ...slog.Attr) {
	if h := holderOf(ctx); h != nil {
		h.mu.Lock()
		h.attrs = append(h.attrs, attrs...)
		h.mu.Unlock()
	}
}

func (h *holder) snapshot() (route string, deadline time.Duration, err *problem.Error, attrs []slog.Attr) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.route, h.deadline, h.err, append([]slog.Attr(nil), h.attrs...)
}

func (h *holder) setErr(e *problem.Error) {
	h.mu.Lock()
	h.err = e
	h.mu.Unlock()
}

func (h *holder) close() {
	h.mu.Lock()
	fns := h.cleanup
	h.cleanup = nil
	h.mu.Unlock()
	for _, f := range fns {
		f()
	}
}

type requestIDKey struct{}

// RequestID is the request's ID (P3.2), "" outside a request.
func RequestID(ctx context.Context) string {
	s, _ := ctx.Value(requestIDKey{}).(string)
	return s
}

// WithRequestID returns ctx carrying a request ID (used by the gRPC server and tests too).
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}
