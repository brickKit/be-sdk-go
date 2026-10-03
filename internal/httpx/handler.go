// Package httpx is the outer HTTP layer every member's main port is served through (be-protocol P3):
// trace extraction and the server span, the request ID, the route deadline answered as 504 even when a
// handler ignores it, panic recovery, the problem+json error body, the access-log line and the RED
// observation. The router (gin) runs inside it and tells it the route through SetRoute.
package httpx

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Handler serves one member's main port around Inner.
type Handler struct {
	Inner           http.Handler
	Tracer          trace.Tracer                                                  // the member's own (P18.1)
	Propagator      propagation.TextMapPropagator                                 // the platform's, passed explicitly
	Logger          *slog.Logger                                                  // the member's
	Catalogue       *problem.Catalogue                                            // reason texts
	Locale          string                                                        // DEFAULT_LOCALE
	DefaultDeadline time.Duration                                                 // HTTP_DEFAULT_TIMEOUT, for requests no route claimed
	Grace           time.Duration                                                 // how long after the deadline a handler may still answer with its own reason (default 200ms)
	Observe         func(method, route string, status int, d time.Duration)       // RED metrics
	Fields          func(ctx context.Context, attrs ...slog.Attr) context.Context // request-scoped log fields; nil = none
	Quiet           func(path string) bool                                        // access-log lines of these paths go out at debug
}

type result struct {
	timedOut bool
	panicked any
	stack    []byte
}

// ServeHTTP implements P3.2–P3.5 and P3.10 for one request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := h.Propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := h.Tracer.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	reqID := r.Header.Get("X-Request-Id")
	if reqID == "" {
		reqID = span.SpanContext().TraceID().String()
	}
	w.Header().Set("X-Request-Id", reqID)
	ctx = WithRequestID(ctx, reqID)
	if h.Fields != nil {
		ctx = h.Fields(ctx, slog.String("request_id", reqID))
	}
	base, cancelBase := context.WithCancel(ctx)
	hold := &holder{base: base, routed: make(chan struct{}), deadline: h.DefaultDeadline}
	defer func() { cancelBase(); hold.close() }()
	rctx, cancel := context.WithTimeout(withHolder(base, hold), h.DefaultDeadline)
	defer cancel()

	buf := newBuffer()
	res := h.run(w, r.WithContext(rctx), buf, hold, start)
	status := h.answer(w, r.WithContext(ctx), buf, hold, res)

	route, _, perr, attrs := hold.snapshot()
	span.SetName(r.Method + " " + route)
	span.SetAttributes(attribute.String("http.request.method", r.Method), attribute.String("http.route", route),
		attribute.Int("http.response.status_code", status))
	if status >= 500 {
		span.SetStatus(codes.Error, http.StatusText(status))
	}
	h.accessLog(ctx, r, route, status, time.Since(start), perr, res, attrs)
	if h.Observe != nil {
		h.Observe(r.Method, route, status, time.Since(start))
	}
}

// run executes the inner handler in its own goroutine and waits for it, for the route deadline (known
// once the router called SetRoute) and then for Grace, so a handler that noticed its cancelled context
// can still answer with its own reason (STATEMENT_TIMEOUT, a dependency's DEADLINE_EXCEEDED).
func (h *Handler) run(w http.ResponseWriter, r *http.Request, buf *buffer, hold *holder, start time.Time) result {
	done := make(chan result, 1)
	go func() {
		var res result
		defer func() {
			if p := recover(); p != nil {
				res.panicked, res.stack = p, debug.Stack()
			}
			done <- res
		}()
		h.Inner.ServeHTTP(buf, r)
	}()
	deadline := start.Add(h.DefaultDeadline)
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	routed := hold.routed
	for {
		select {
		case res := <-done:
			return res
		case <-routed:
			routed = nil
			_, d, _, _ := hold.snapshot()
			deadline = start.Add(d)
			timer.Reset(time.Until(deadline))
			_ = http.NewResponseController(w).SetWriteDeadline(deadline.Add(h.grace() + 5*time.Second))
		case <-timer.C:
			select {
			case res := <-done:
				return res
			case <-time.After(h.grace()):
				return result{timedOut: true}
			}
		}
	}
}

func (h *Handler) grace() time.Duration {
	if h.Grace > 0 {
		return h.Grace
	}
	return 200 * time.Millisecond
}

// answer writes what the client receives and returns its status.
func (h *Handler) answer(w http.ResponseWriter, r *http.Request, buf *buffer, hold *holder, res result) int {
	switch {
	case res.panicked != nil:
		e := problem.Wrap(fmt.Errorf("panic: %v\n%s", res.panicked, res.stack), "INTERNAL", nil)
		return WriteProblemHold(w, r, h.Catalogue, h.Locale, e, hold)
	case res.timedOut:
		return WriteProblemHold(w, r, h.Catalogue, h.Locale, problem.Be("DEADLINE_BUDGET_EXHAUSTED", nil), hold)
	}
	buf.copyTo(w)
	return buf.code()
}

func (h *Handler) accessLog(ctx context.Context, r *http.Request, route string, status int, d time.Duration,
	perr *problem.Error, res result, attrs []slog.Attr) {
	level := slog.LevelInfo
	if h.Quiet != nil && h.Quiet(r.URL.Path) {
		level = slog.LevelDebug
	}
	all := []slog.Attr{slog.String("http.request.method", r.Method), slog.String("http.route", route),
		slog.Int("http.response.status_code", status), slog.Int64("duration_ms", d.Milliseconds())}
	all = append(all, attrs...)
	if perr != nil {
		if l, logged := problem.LogLevel(perr.Code); logged && l > level {
			level = l
		}
		all = append(all, slog.String("error", perr.Error()), slog.String("error.code", problem.CodeName(perr.Code)),
			slog.String("error.reason", perr.Reason))
	}
	h.Logger.LogAttrs(ctx, level, "http_request", all...)
}
