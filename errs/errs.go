// Package errs is the one error object of protocol chapter 03: a reason with its domain, gRPC code and HTTP status.
// Handlers return *Error (or any error, which becomes INTERNAL); the HTTP and gRPC layers render it.
package errs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"
)

// Code is a gRPC status code name (INVALID_ARGUMENT, UNAVAILABLE, ...).
type Code string

// The gRPC codes the protocol uses.
const (
	InvalidArgument    Code = "INVALID_ARGUMENT"
	FailedPrecondition Code = "FAILED_PRECONDITION"
	OutOfRange         Code = "OUT_OF_RANGE"
	Unauthenticated    Code = "UNAUTHENTICATED"
	PermissionDenied   Code = "PERMISSION_DENIED"
	NotFound           Code = "NOT_FOUND"
	AlreadyExists      Code = "ALREADY_EXISTS"
	Aborted            Code = "ABORTED"
	ResourceExhausted  Code = "RESOURCE_EXHAUSTED"
	Cancelled          Code = "CANCELLED"
	Internal           Code = "INTERNAL"
	Unknown            Code = "UNKNOWN"
	DataLoss           Code = "DATA_LOSS"
	Unimplemented      Code = "UNIMPLEMENTED"
	Unavailable        Code = "UNAVAILABLE"
	DeadlineExceeded   Code = "DEADLINE_EXCEEDED"
)

var codeHTTP = map[Code]int{
	InvalidArgument: 400, FailedPrecondition: 400, OutOfRange: 400, Unauthenticated: 401, PermissionDenied: 403,
	NotFound: 404, AlreadyExists: 409, Aborted: 409, ResourceExhausted: 429, Cancelled: 499, Internal: 500,
	Unknown: 500, DataLoss: 500, Unimplemented: 501, Unavailable: 503, DeadlineExceeded: 504,
}

// HTTPStatus maps a gRPC code to its HTTP status (P3 code table); unknown codes are 500.
func HTTPStatus(c Code) int {
	if s, ok := codeHTTP[c]; ok {
		return s
	}
	return 500
}

// LogLevel is the level an error of this code is logged at (P3.9); ok=false means "not logged as an error" (CANCELLED).
func LogLevel(c Code) (level slog.Level, ok bool) {
	switch c {
	case Internal, Unknown, DataLoss:
		return slog.LevelError, true
	case Unavailable, DeadlineExceeded:
		return slog.LevelWarn, true
	case Cancelled:
		return 0, false
	}
	return slog.LevelInfo, true
}

// AccessLogLevel is the level of a request's access log line by HTTP status (P3.9).
func AccessLogLevel(status int) slog.Level {
	switch status {
	case 500:
		return slog.LevelError
	case 503, 504:
		return slog.LevelWarn
	}
	return slog.LevelInfo
}

// Violation is one field error of REQUEST_INVALID.
type Violation struct {
	Field       string `json:"field"`
	Reason      string `json:"reason"`
	Description string `json:"description"`
}

// Error is the error object. Build platform errors with New, component errors with Define(...).New.
type Error struct {
	Reason     string
	Domain     string
	Code       Code
	HTTP       int
	Title      string
	Detail     string
	Metadata   map[string]string
	Violations []Violation
	RetryAfter time.Duration
	cause      error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s/%s: %s: %v", e.Domain, e.Reason, e.Detail, e.cause)
	}
	return fmt.Sprintf("%s/%s: %s", e.Domain, e.Reason, e.Detail)
}

// Unwrap returns the cause given to Wrap (logged, never sent).
func (e *Error) Unwrap() error { return e.cause }

func (e *Error) clone() *Error {
	c := *e
	c.Metadata = maps.Clone(e.Metadata)
	c.Violations = append([]Violation(nil), e.Violations...)
	return &c
}

// With returns a copy with one metadata entry added (values are strings, AIP-193).
func (e *Error) With(key, value string) *Error {
	c := e.clone()
	if c.Metadata == nil {
		c.Metadata = map[string]string{}
	}
	c.Metadata[key] = value
	return c
}

// WithViolations returns a copy carrying field errors.
func (e *Error) WithViolations(v ...Violation) *Error {
	c := e.clone()
	c.Violations = append(c.Violations, v...)
	return c
}

// WithRetryAfter returns a copy telling the caller when to retry (Retry-After, RetryInfo).
func (e *Error) WithRetryAfter(d time.Duration) *Error {
	c := e.clone()
	c.RetryAfter = d
	return c
}

// Wrap returns a copy keeping cause for the logs; the cause never reaches the caller.
func (e *Error) Wrap(cause error) *Error {
	c := e.clone()
	c.cause = cause
	return c
}

// Is reports whether err carries this reason and domain.
func Is(err error, domain, reason string) bool {
	var e *Error
	return errors.As(err, &e) && e.Domain == domain && e.Reason == reason
}

// From turns any error into the error object: *Error as is, context cancellation and deadlines into
// REQUEST_CANCELLED and DEADLINE_EXCEEDED, anything else into INTERNAL with the original kept as the cause.
func From(err error) *Error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	switch {
	case errors.Is(err, context.Canceled):
		return New(ReasonRequestCancelled, "the request was cancelled").Wrap(err)
	case errors.Is(err, context.DeadlineExceeded):
		return New(ReasonDeadlineExceeded, "the request ran out of time").Wrap(err)
	}
	return New(ReasonInternal, internalDetail).Wrap(err)
}

const internalDetail = "an internal error occurred; quote the trace id when reporting it"
