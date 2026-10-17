package errs

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
)

// ProblemContentType is the media type of every 4xx and 5xx user-surface response (P3.1).
const ProblemContentType = "application/problem+json"

// Problem is the user-surface error body (schemas/problem.schema.json of the protocol).
type Problem struct {
	Type       string            `json:"type"`
	Title      string            `json:"title"`
	Status     int               `json:"status"`
	Detail     string            `json:"detail"`
	Reason     string            `json:"reason"`
	Domain     string            `json:"domain"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	TraceID    string            `json:"trace_id"`
	Violations []Violation       `json:"violations,omitempty"`
}

// Problem renders the error for the user surface. INTERNAL never carries anything but the generic detail (P3.4).
func (e *Error) Problem(traceID string) Problem {
	p := Problem{Type: "urn:be:" + e.Domain + ":" + e.Reason, Title: e.Title, Status: e.HTTP, Detail: e.Detail,
		Reason: e.Reason, Domain: e.Domain, Metadata: e.Metadata, TraceID: traceID, Violations: e.Violations}
	if e.Code == Internal || e.Code == Unknown || e.Code == DataLoss {
		p.Reason, p.Domain, p.Type, p.Detail, p.Metadata, p.Violations = ReasonInternal, PlatformDomain,
			"urn:be:be:"+ReasonInternal, internalDetail, nil, nil
		p.Title = platform[ReasonInternal].Title
		p.Status = 500
	}
	if p.Title == "" {
		p.Title = p.Reason
	}
	if p.Detail == "" {
		p.Detail = p.Title
	}
	return p
}

// WriteHTTP writes the error as problem+json, with Retry-After when the error says when to retry (P3.6).
func (e *Error) WriteHTTP(w http.ResponseWriter, traceID string) {
	p := e.Problem(traceID)
	h := w.Header()
	h.Set("Content-Type", ProblemContentType)
	if e.RetryAfter > 0 {
		h.Set("Retry-After", strconv.Itoa(int(math.Ceil(e.RetryAfter.Seconds()))))
	}
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}
