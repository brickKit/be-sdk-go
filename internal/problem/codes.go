// Package problem is the error model of be-protocol P4: one error value carrying a canonical gRPC
// code and an AIP-193 identity (domain, reason, metadata), its RFC 9457 problem+json body, its gRPC
// status with details, the reserved reasons of domain `be`, the SQLSTATE classification of P10.4 and
// the log level of P4.6. It has no I/O and no mutable package state.
package problem

import (
	"fmt"
	"net/http"

	"google.golang.org/grpc/codes"
)

// Invalid is the error of a malformed input to this package (a vector error class, e.g. CODE_UNKNOWN).
type Invalid struct {
	Reason string
	Detail string
}

func (e *Invalid) Error() string { return e.Reason + ": " + e.Detail }

var codeNames = [...]string{
	codes.OK: "OK", codes.Canceled: "CANCELLED", codes.Unknown: "UNKNOWN",
	codes.InvalidArgument: "INVALID_ARGUMENT", codes.DeadlineExceeded: "DEADLINE_EXCEEDED",
	codes.NotFound: "NOT_FOUND", codes.AlreadyExists: "ALREADY_EXISTS",
	codes.PermissionDenied: "PERMISSION_DENIED", codes.ResourceExhausted: "RESOURCE_EXHAUSTED",
	codes.FailedPrecondition: "FAILED_PRECONDITION", codes.Aborted: "ABORTED",
	codes.OutOfRange: "OUT_OF_RANGE", codes.Unimplemented: "UNIMPLEMENTED", codes.Internal: "INTERNAL",
	codes.Unavailable: "UNAVAILABLE", codes.DataLoss: "DATA_LOSS", codes.Unauthenticated: "UNAUTHENTICATED",
}

// CodeName is the canonical upper-snake name of a gRPC code (CANCELLED, not Canceled).
func CodeName(c codes.Code) string {
	if int(c) < len(codeNames) {
		return codeNames[c]
	}
	return fmt.Sprintf("CODE(%d)", uint32(c))
}

// ParseCode parses a canonical code name, exactly (upper case); anything else is CODE_UNKNOWN.
func ParseCode(name string) (codes.Code, error) {
	for i, n := range codeNames {
		if n == name {
			return codes.Code(i), nil
		}
	}
	return 0, &Invalid{Reason: "CODE_UNKNOWN", Detail: fmt.Sprintf("%q is not a canonical gRPC code name", name)}
}

var httpOf = map[codes.Code]int{
	codes.OK: http.StatusOK, codes.Canceled: 499, codes.Unknown: 500, codes.InvalidArgument: 400,
	codes.DeadlineExceeded: 504, codes.NotFound: 404, codes.AlreadyExists: 409, codes.PermissionDenied: 403,
	codes.ResourceExhausted: 429, codes.FailedPrecondition: 400, codes.Aborted: 409, codes.OutOfRange: 400,
	codes.Unimplemented: 501, codes.Internal: 500, codes.Unavailable: 503, codes.DataLoss: 500,
	codes.Unauthenticated: 401,
}

// HTTPStatus maps a code to its HTTP status (P4.2 table); the one exception is be/BODY_TOO_LARGE → 413.
func HTTPStatus(c codes.Code, domain, reason string) int {
	if domain == DomainBe && reason == "BODY_TOO_LARGE" {
		return http.StatusRequestEntityTooLarge
	}
	if s, ok := httpOf[c]; ok {
		return s
	}
	return http.StatusInternalServerError
}

// codeFromStatus restores a code from a bare HTTP status, when a dependency answered without a
// problem body (vectors errors/codes restore_http): no reason is invented.
func codeFromStatus(status int) codes.Code {
	switch status {
	case 400, 413:
		return codes.InvalidArgument
	case 401:
		return codes.Unauthenticated
	case 403:
		return codes.PermissionDenied
	case 404:
		return codes.NotFound
	case 409:
		return codes.Aborted
	case 429:
		return codes.ResourceExhausted
	case 499:
		return codes.Canceled
	case 501:
		return codes.Unimplemented
	case 502, 503:
		return codes.Unavailable
	case 504:
		return codes.DeadlineExceeded
	}
	if status >= 400 && status < 500 {
		return codes.FailedPrecondition
	}
	return codes.Unknown
}
