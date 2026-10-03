package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type observed struct {
	mu   sync.Mutex
	rows []string
}

func (o *observed) add(method, route string, status int, _ time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rows = append(o.rows, method+" "+route+" "+http.StatusText(status))
}

func newTestHandler(inner http.Handler) (*Handler, *bytes.Buffer, *observed) {
	var logs bytes.Buffer
	obs := &observed{}
	tp := sdktrace.NewTracerProvider()
	h := &Handler{
		Inner: inner, Tracer: tp.Tracer("t"), Propagator: propagation.TraceContext{},
		Logger: slog.New(traceHandler{slog.NewJSONHandler(&lockedWriter{w: &logs}, nil)}), Catalogue: problem.NewCatalogue(),
		Locale: "en", DefaultDeadline: 10 * time.Second, Grace: 50 * time.Millisecond, Observe: obs.add,
	}
	return h, &logs, obs
}

type lockedWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) problem.Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != problem.ContentType {
		t.Fatalf("content type %q body %s", ct, rec.Body)
	}
	var p problem.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body: %v %s", err, rec.Body)
	}
	return p
}

func TestHandlerAnswers504WhenRouteDeadlinePasses(t *testing.T) {
	release := make(chan struct{})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := SetRoute(r.Context(), "/x/y/slow", 100*time.Millisecond)
		<-release // ignores ctx on purpose: a handler that runs past its deadline
		_ = ctx
		w.WriteHeader(200)
		_, _ = w.Write([]byte("late"))
	})
	h, _, obs := newTestHandler(inner)
	rec := httptest.NewRecorder()
	start := time.Now()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x/y/slow", nil))
	close(release)
	if el := time.Since(start); el > time.Second {
		t.Fatalf("took %v", el)
	}
	p := decodeProblem(t, rec)
	if rec.Code != 504 || p.Reason != "DEADLINE_BUDGET_EXHAUSTED" || p.Code != "DEADLINE_EXCEEDED" {
		t.Fatalf("got %d %+v", rec.Code, p)
	}
	if strings.Contains(rec.Body.String(), "late") {
		t.Fatal("late output leaked")
	}
	if len(obs.rows) != 1 || obs.rows[0] != "GET /x/y/slow Gateway Timeout" {
		t.Fatalf("observed %v", obs.rows)
	}
}

func TestHandlerKeepsHandlersOwnTimeoutAnswer(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := SetRoute(r.Context(), "/x/y/db", 50*time.Millisecond)
		<-ctx.Done() // a statement cancelled by the deadline: the handler answers STATEMENT_TIMEOUT
		WriteProblem(w, r, problem.NewCatalogue(), "en", problem.Be("STATEMENT_TIMEOUT", nil))
	})
	h, _, _ := newTestHandler(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x/y/db", nil))
	if p := decodeProblem(t, rec); rec.Code != 504 || p.Reason != "STATEMENT_TIMEOUT" {
		t.Fatalf("got %d %+v", rec.Code, p)
	}
}

func TestHandlerRecoversPanicWithoutLeaking(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("secret table widgets exploded")
	})
	h, logs, _ := newTestHandler(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/x/y/z", nil))
	p := decodeProblem(t, rec)
	if rec.Code != 500 || p.Reason != "INTERNAL" || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
	if p.TraceID == "" || p.RequestID == "" {
		t.Fatalf("trace and request ids required: %+v", p)
	}
	if !strings.Contains(logs.String(), "secret table widgets exploded") {
		t.Fatalf("panic not logged: %s", logs)
	}
}

func TestHandlerRequestIDAndTrace(t *testing.T) {
	var seenTrace, seenReq string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenReq = RequestID(r.Context())
		seenTrace = TraceID(r.Context())
		w.WriteHeader(204)
	})
	h, logs, _ := newTestHandler(inner)
	// no inbound id: the trace id is used, and the caller's trace is continued
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if seenTrace != "4bf92f3577b34da6a3ce929d0e0e4736" || seenReq != seenTrace || rec.Header().Get("X-Request-Id") != seenTrace {
		t.Fatalf("trace=%s req=%s header=%s", seenTrace, seenReq, rec.Header().Get("X-Request-Id"))
	}
	// an inbound id is kept
	req = httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("X-Request-Id", "abc-1")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if seenReq != "abc-1" || rec.Header().Get("X-Request-Id") != "abc-1" {
		t.Fatalf("kept id: %s %s", seenReq, rec.Header().Get("X-Request-Id"))
	}
	var line map[string]any
	first := strings.SplitN(strings.TrimSpace(logs.String()), "\n", 2)[0]
	if err := json.Unmarshal([]byte(first), &line); err != nil {
		t.Fatal(err)
	}
	if line["msg"] != "http_request" || line["http.response.status_code"] != float64(204) || line["trace_id"] == nil {
		t.Fatalf("access log: %s", first)
	}
}

func TestHandlerUnroutedUsesDefaultDeadline(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dl, ok := r.Context().Deadline()
		if !ok || time.Until(dl) > 10*time.Second || time.Until(dl) < 9*time.Second {
			t.Errorf("default deadline missing: %v %v", dl, ok)
		}
		w.WriteHeader(404)
	})
	h, _, _ := newTestHandler(inner)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/nope", nil))
	if rec.Code != 404 {
		t.Fatal(rec.Code)
	}
}

func TestSetRouteShortensContext(t *testing.T) {
	ctx := withHolder(context.Background(), &holder{routed: make(chan struct{})})
	ctx2 := SetRoute(ctx, "/a", 20*time.Millisecond)
	select {
	case <-ctx2.Done():
	case <-time.After(time.Second):
		t.Fatal("route deadline not applied to the context")
	}
}

// traceHandler stands in for logx, which adds trace_id from the context to every line.
type traceHandler struct{ slog.Handler }

func (h traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := TraceID(ctx); id != "" {
		r.AddAttrs(slog.String("trace_id", id))
	}
	return h.Handler.Handle(ctx, r)
}
