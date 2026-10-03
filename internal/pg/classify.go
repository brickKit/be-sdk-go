package pg

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// decision is what one failed transaction attempt becomes (P10.4).
type decision struct {
	retry  bool
	base   time.Duration // 10 ms · 2^(attempt−1), before jitter
	reason string        // the SQLSTATE that caused a retry, be_tx_retries_total{sqlstate}
	err    error         // when not retried
}

// classifyAttempt decides what a failed attempt becomes (P10.4). attempt is 1-based; ctxErr is the
// unit of work's ctx.Err() after the failure.
//
// Decision tree: a *problem.Error anywhere in the chain passes through unchanged; 40001 / 40P01
// retry while attempt < maxAttempts; any other SQLSTATE (and a retryable one on the last attempt)
// maps through problem.ClassifySQLState with the original error as Cause (class 08 is
// DEPENDENCY_UNAVAILABLE); without a SQLSTATE a cancelled ctx is CANCELLED, an expired deadline
// STATEMENT_TIMEOUT, a lost connection DEPENDENCY_UNAVAILABLE {dependency: db}, anything else INTERNAL.
func classifyAttempt(err error, attempt, maxAttempts int, ctxErr error) decision {
	var pe *problem.Error
	if errors.As(err, &pe) {
		return decision{err: err}
	}
	state := SQLState(err)
	if (state == "40001" || state == "40P01") && attempt < maxAttempts {
		return decision{retry: true, base: 10 * time.Millisecond << (attempt - 1), reason: state}
	}
	cs := contextState(err, ctxErr)
	if state != "" {
		e := problem.ClassifySQLState(state, problem.MaxTxAttempts, cs, nil).Err
		e.Cause = err
		return decision{err: e}
	}
	switch cs {
	case problem.ContextCancelled:
		return decision{err: problem.Wrap(err, "REQUEST_CANCELLED", nil)}
	case problem.ContextDeadlineExceeded:
		return decision{err: problem.Wrap(err, "STATEMENT_TIMEOUT", nil)}
	}
	if IsConnectionFailure(err) {
		return decision{err: problem.DBUnavailable(err)}
	}
	return decision{err: problem.Wrap(err, "INTERNAL", nil)}
}

// contextState says what the unit of work's context (or the error itself) reports.
func contextState(err, ctxErr error) problem.ContextState {
	switch {
	case errors.Is(ctxErr, context.Canceled):
		return problem.ContextCancelled
	case errors.Is(ctxErr, context.DeadlineExceeded):
		return problem.ContextDeadlineExceeded
	case errors.Is(err, context.Canceled):
		return problem.ContextCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return problem.ContextDeadlineExceeded
	}
	return problem.ContextNone
}

// jitter spreads a retry delay by ± 50 % (P10.4).
func jitter(base time.Duration) time.Duration {
	return base/2 + time.Duration(rand.Int64N(int64(base)))
}
