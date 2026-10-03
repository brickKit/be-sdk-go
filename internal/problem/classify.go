package problem

import (
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"google.golang.org/grpc/codes"
)

var reasonName = regexp.MustCompile(`^[A-Z][A-Z0-9]*(_[A-Z0-9]+)*$`)

// ValidateReason checks a reason name (P4.1) and that a component does not raise a reserved name in
// its own domain (P4.7): REASON_NAME_INVALID or REASON_RESERVED.
func ValidateReason(reason, domain string) error {
	if !reasonName.MatchString(reason) {
		return &Invalid{Reason: "REASON_NAME_INVALID", Detail: fmt.Sprintf("%q is not UPPER_SNAKE", reason)}
	}
	if domain != DomainBe && beCatalogue().Has(DomainBe, reason) {
		return &Invalid{Reason: "REASON_RESERVED", Detail: fmt.Sprintf("%q is a reserved reason of domain be", reason)}
	}
	return nil
}

// LogLevel is the level the runtime logs an error at, by code (P4.6); false = not logged.
func LogLevel(c codes.Code) (slog.Level, bool) {
	switch c {
	case codes.OK, codes.Canceled:
		return 0, false
	case codes.Internal, codes.Unknown, codes.DataLoss:
		return slog.LevelError, true
	case codes.Unavailable, codes.DeadlineExceeded:
		return slog.LevelWarn, true
	}
	return slog.LevelInfo, true
}

// ContextState is what the unit of work's context said when a statement failed.
type ContextState int

const (
	ContextNone             ContextState = iota // the context is live
	ContextDeadlineExceeded                     // the deadline passed
	ContextCancelled                            // the caller went away
)

// MaxTxAttempts is how many times a transaction body runs on 40001 / 40P01 (P10.4).
const MaxTxAttempts = 3

// Classification is the decision for one failed transaction attempt (P10.4).
type Classification struct {
	Retry     bool
	BaseDelay time.Duration // 10 ms · 2^(attempt−1); the caller adds jitter
	Err       *Error        // when not retried
}

// ClassifySQLState decides what a failed attempt becomes (P10.4, vectors errors/sqlstate). attempt is
// 1-based. mapped is the component's own mapping of a unique violation (23505), if any.
func ClassifySQLState(state string, attempt int, ctx ContextState, mapped *Error) Classification {
	switch state {
	case "40001", "40P01":
		if attempt < MaxTxAttempts {
			return Classification{Retry: true, BaseDelay: 10 * time.Millisecond << (attempt - 1)}
		}
		return Classification{Err: Be("TX_CONFLICT", nil)}
	case "55P03":
		return Classification{Err: Be("LOCK_TIMEOUT", nil)}
	case "57014":
		if ctx == ContextCancelled {
			return Classification{Err: &Error{Code: codes.Canceled}}
		}
		return Classification{Err: Be("STATEMENT_TIMEOUT", nil)}
	case "25P04":
		return Classification{Err: Be("STATEMENT_TIMEOUT", nil)}
	case "53300":
		return Classification{Err: Be("DB_TOO_MANY_CONNECTIONS", nil)}
	case "BE001":
		return Classification{Err: Be("UNIT_SEALED", nil)}
	case "23505":
		if mapped != nil {
			return Classification{Err: mapped}
		}
	}
	return Classification{Err: Be("INTERNAL", nil)}
}
