package besdk

import (
	"errors"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"google.golang.org/grpc/codes"
)

// FieldViolation is one field error of a request (BadRequest / problem `violations`).
type FieldViolation = problem.Violation

// Errorf returns an error of the component's own domain (P4.1): a canonical code, a reason listed in
// the component's contracts/errors.yaml, string metadata (the reason's template parameters) and a
// message in the deployment's default language. The SDK fills the domain with the component's ID and
// maps it to problem+json or a gRPC status; a component never writes a ToStatus.
func Errorf(code codes.Code, reason string, meta map[string]string, format string, args ...any) error {
	return problem.New(code, "", reason, meta, fmt.Sprintf(format, args...))
}

// WithViolations adds field errors to an error made by Errorf (or any protocol error).
func WithViolations(err error, v ...FieldViolation) error {
	e := problem.From(err)
	c := *e
	c.Violations = append(append([]problem.Violation(nil), e.Violations...), v...)
	return &c
}

// WithRetryAfter adds a retry delay (Retry-After / RetryInfo, P4.2).
func WithRetryAfter(err error, d time.Duration) error {
	e := problem.From(err)
	c := *e
	c.RetryAfter = d
	return &c
}

// ReasonOf returns the domain and reason an error carries (a dependency's error keeps its own).
func ReasonOf(err error) (domain, reason string, ok bool) {
	var e *problem.Error
	if !errors.As(err, &e) || e.Reason == "" {
		return "", "", false
	}
	return e.Domain, e.Reason, true
}
