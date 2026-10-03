package problem

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
)

// DomainBe is the domain of the reserved reasons (P4.1).
const DomainBe = "be"

// Violation is one field error (gRPC BadRequest.FieldViolation; problem body `violations`).
type Violation struct {
	Field       string `json:"field"`
	Reason      string `json:"reason"`
	Description string `json:"description"`
}

// Error is the one error value of the protocol (P4). Code zero (OK) means "unclassified": such an
// error always leaves the process as the generic INTERNAL body (P4.3).
type Error struct {
	Code       codes.Code
	Domain     string            // component ID, family ID, or "be"
	Reason     string            // UPPER_SNAKE
	Metadata   map[string]string // the catalogue template's parameters; strings only (P4.8)
	Detail     string            // the message in the deployment's default language; empty = from the catalogue
	Violations []Violation
	RetryAfter time.Duration // becomes Retry-After / RetryInfo
	Cause      error         // the original error, for the log only (P4.3)
}

// New builds an error of a domain.
func New(code codes.Code, domain, reason string, meta map[string]string, detail string) *Error {
	return &Error{Code: code, Domain: domain, Reason: reason, Metadata: meta, Detail: detail}
}

// Be builds an error with a reserved reason of domain be; its code comes from the catalogue. An
// unknown reason is a programming error and becomes INTERNAL (never a reason outside errors-be.yaml,
// P4.7).
func Be(reason string, meta map[string]string) *Error {
	r, ok := beCatalogue().lookup(DomainBe, reason)
	if !ok {
		return &Error{Code: codes.Internal, Domain: DomainBe, Reason: "INTERNAL",
			Cause: fmt.Errorf("unknown be reason %q", reason)}
	}
	return &Error{Code: r.code, Domain: DomainBe, Reason: reason, Metadata: meta}
}

// Wrap returns a be error that keeps cause for the log.
func Wrap(cause error, reason string, meta map[string]string) *Error {
	e := Be(reason, meta)
	e.Cause = cause
	return e
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString(CodeName(e.Code))
	if e.Reason != "" {
		b.WriteString(" " + e.Domain + ":" + e.Reason)
	}
	if e.Detail != "" {
		b.WriteString(": " + e.Detail)
	}
	if e.Cause != nil {
		b.WriteString(": " + e.Cause.Error())
	}
	return b.String()
}

// Unwrap exposes the cause to errors.Is / errors.As.
func (e *Error) Unwrap() error { return e.Cause }

// HTTPStatus is the error's HTTP status (P4.2).
func (e *Error) HTTPStatus() int {
	if e.Code == codes.OK {
		return 500
	}
	return HTTPStatus(e.Code, e.Domain, e.Reason)
}

// Is reports whether err carries domain:reason.
func Is(err error, domain, reason string) bool {
	var e *Error
	return errors.As(err, &e) && e.Domain == domain && e.Reason == reason
}

// From returns the protocol error inside err, or classifies a plain error: context cancellation is
// CANCELLED, an expired deadline DEADLINE_EXCEEDED / DEADLINE_BUDGET_EXHAUSTED (P3.4), anything else
// INTERNAL with err as its cause. nil stays nil.
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
		return &Error{Code: codes.Canceled, Cause: err}
	case errors.Is(err, context.DeadlineExceeded):
		return Wrap(err, "DEADLINE_BUDGET_EXHAUSTED", nil)
	}
	if st, ok := grpcStatusOf(err); ok {
		return FromStatus(st)
	}
	return &Error{Code: codes.Internal, Domain: DomainBe, Reason: "INTERNAL", Cause: err}
}

// Public is what a caller may see (P4.3): INTERNAL, UNKNOWN and DATA_LOSS, an unclassified error, and
// an error with a reason but no domain all become reason INTERNAL of domain be with empty metadata, no
// violations and no detail (the catalogue's generic text is rendered); UNKNOWN, DATA_LOSS, CANCELLED
// and a bare code without reason and domain keep their code. Every other error is returned unchanged.
func Public(e *Error) *Error {
	generic := e.Code == codes.OK || e.Code == codes.Internal || e.Code == codes.Unknown ||
		e.Code == codes.DataLoss || e.Reason == "" || e.Domain == ""
	if !generic {
		return e
	}
	code := e.Code
	// A bare code without any identity (a transport failure, a dependency that answered without a
	// problem body) keeps its code, so a caller still sees 503 / 504; a reason without a domain is
	// unclassified and becomes INTERNAL (vectors errors/problem).
	bare := e.Reason == "" && e.Domain == "" && code != codes.OK
	if !bare && code != codes.Unknown && code != codes.DataLoss && code != codes.Canceled {
		code = codes.Internal
	}
	return &Error{Code: code, Domain: DomainBe, Reason: "INTERNAL", Metadata: map[string]string{}}
}

// StringMetadata converts decoded JSON metadata to strings only, refusing any non-string value
// (METADATA_NOT_STRING, P4.8).
func StringMetadata(m map[string]any) (map[string]string, error) {
	out := make(map[string]string, len(m))
	for k, v := range m {
		s, ok := v.(string)
		if !ok {
			return nil, &Invalid{Reason: "METADATA_NOT_STRING", Detail: fmt.Sprintf("metadata %q is %T", k, v)}
		}
		out[k] = s
	}
	return out, nil
}

// WithDomain returns e with domain filled in when e has a reason but no domain: an error a component
// raised with Errorf belongs to that component (P4.1). Other errors are returned unchanged.
func WithDomain(e *Error, domain string) *Error {
	if e == nil || e.Reason == "" || e.Domain != "" || domain == "" {
		return e
	}
	c := *e
	c.Domain = domain
	return &c
}
