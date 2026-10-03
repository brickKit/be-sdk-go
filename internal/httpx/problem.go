package httpx

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"go.opentelemetry.io/otel/trace"
)

// WriteProblem answers e as problem+json (P4.1) with Retry-After when it applies (P4.2), and records
// e for the request's access-log line. It returns the status written.
func WriteProblem(w http.ResponseWriter, r *http.Request, cat *problem.Catalogue, locale string, e *problem.Error) int {
	return WriteProblemHold(w, r, cat, locale, e, holderOf(r.Context()))
}

// WriteProblemHold is WriteProblem with an explicit request holder.
func WriteProblemHold(w http.ResponseWriter, r *http.Request, cat *problem.Catalogue, locale string,
	e *problem.Error, hold *holder) int {
	if hold != nil {
		hold.setErr(e)
	}
	body := cat.Problem(e, problem.Request{Path: r.URL.Path, RequestID: RequestID(r.Context()),
		TraceID: TraceID(r.Context())}, locale)
	if ra := problem.RetryAfterHeader(problem.Public(e).Code, e.RetryAfter); ra != "" {
		w.Header().Set("Retry-After", ra)
	}
	if e.Reason == "TOKEN_STALE" && e.Domain == problem.DomainBe {
		w.Header().Set("WWW-Authenticate", `Bearer error="token_stale"`)
	}
	w.Header().Set("Content-Type", problem.ContentType)
	w.WriteHeader(body.Status)
	_ = json.NewEncoder(w).Encode(body)
	return body.Status
}

// TraceID is the active span's trace ID, "" when there is none.
func TraceID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}
