package errs

import (
	"encoding/json"
	"mime"
	"net/http"
	"strconv"
	"time"
)

// FromHTTP reads a user-surface error response. ok is false when the body is not problem+json with a reason and a
// domain (a proxy's page, a server outside the protocol); the caller then judges by the status alone.
func FromHTTP(resp *http.Response, body []byte) (e *Error, ok bool) {
	if mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mt != ProblemContentType {
		return nil, false
	}
	var p Problem
	if json.Unmarshal(body, &p) != nil || p.Reason == "" || p.Domain == "" {
		return nil, false
	}
	code := codeOfHTTP(p.Domain, p.Reason, resp.StatusCode)
	e = &Error{Reason: p.Reason, Domain: p.Domain, Code: code, HTTP: resp.StatusCode, Title: p.Title, Detail: p.Detail,
		Metadata: p.Metadata, Violations: p.Violations}
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s > 0 {
		e.RetryAfter = time.Duration(s) * time.Second
	}
	return e, true
}

// CodeOfHTTP is the code of a user-surface answer (protocol vector call-outcomes): a platform reason's catalog code,
// otherwise by status.
func codeOfHTTP(domain, reason string, status int) Code {
	if domain == PlatformDomain {
		if entry, ok := platform[reason]; ok {
			return entry.Code
		}
	}
	return CodeOfStatus(status)
}

// CodeOfStatus maps an HTTP status without a protocol body to a code; only the statuses that decide retries and
// circuit breaking are distinguished.
func CodeOfStatus(status int) Code {
	switch {
	case status < 400:
		return "OK"
	case status == 502, status == 503:
		return Unavailable
	case status == 504:
		return DeadlineExceeded
	case status == 499:
		return Cancelled
	case status >= 500:
		return Internal
	}
	for c, s := range codeHTTP {
		if s == status && (c == InvalidArgument || c == Unauthenticated || c == PermissionDenied || c == NotFound ||
			c == Aborted || c == ResourceExhausted) {
			return c
		}
	}
	return FailedPrecondition
}
