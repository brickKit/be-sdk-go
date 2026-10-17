package calls

import (
	"errors"

	"github.com/brickKit/be-sdk-go/v3/errs"
)

// Result is what one attempt returned, as the transport saw it.
type Result struct {
	Err           error       // nil on success
	Code          errs.Code   // the answer's code; "OK" on success
	Remote        *errs.Error // the protocol error the dependency sent, nil when it sent none
	ConnectFailed bool        // the connection failed before the request was sent
}

// Class is how an attempt counts (protocol vector call-outcomes).
type Class string

// Classes.
const (
	ClassSuccess       Class = "success"
	ClassUnavailable   Class = "unavailable"
	ClassConnectFailed Class = "connect_failed"
	ClassDeadline      Class = "deadline"
	ClassRelayed       Class = "relayed"
	ClassOther         Class = "other"
	ClassCancelled     Class = "cancelled"
)

// relayed are the platform reasons a dependency uses to report failures further down (P4.18).
var relayed = map[string]bool{errs.ReasonDependencyUnavailable: true, errs.ReasonCircuitOpen: true,
	errs.ReasonDeadlineBudgetExhausted: true, errs.ReasonResultUnknown: true}

// Classify places one attempt's result; callerCancelled is true when the caller's own context was cancelled.
func Classify(r Result, callerCancelled bool) Class {
	switch {
	case callerCancelled && r.Err != nil:
		return ClassCancelled
	case r.ConnectFailed:
		return ClassConnectFailed
	case r.Err == nil:
		return ClassSuccess
	case r.Remote != nil && r.Remote.Domain == errs.PlatformDomain && relayed[r.Remote.Reason]:
		return ClassRelayed
	case r.Code == errs.Unavailable:
		return ClassUnavailable
	case r.Code == errs.DeadlineExceeded:
		return ClassDeadline
	}
	return ClassOther
}

// BreakerOutcome is what the circuit breaker records for a class (P4.11).
func (c Class) BreakerOutcome() string {
	switch c {
	case ClassSuccess:
		return BreakerSuccess
	case ClassUnavailable, ClassConnectFailed, ClassDeadline:
		return BreakerFailure
	}
	return BreakerIgnored
}

// BudgetOutcome is what the retry budget records (P4.8).
func (c Class) BudgetOutcome() string {
	switch c {
	case ClassSuccess:
		return BudgetSuccess
	case ClassUnavailable, ClassConnectFailed:
		return BudgetUnavailable
	}
	return BudgetOther
}

// RetryOutcome is the outcome fed to ShouldRetry.
func (c Class) RetryOutcome() string {
	switch c {
	case ClassUnavailable:
		return OutcomeUnavailable
	case ClassConnectFailed:
		return OutcomeConnectFailed
	}
	return OutcomeNone
}

// Upward is the error the caller answers when this was the final attempt (P4.19).
func Upward(c Class, r Result, idempotent bool, dependency string) error {
	switch c {
	case ClassSuccess:
		return nil
	case ClassUnavailable, ClassConnectFailed:
		e := errs.New(errs.ReasonDependencyUnavailable, "a dependency is unavailable").With("dependency", dependency).Wrap(r.Err)
		if r.Remote != nil && r.Remote.RetryAfter > 0 {
			e = e.WithRetryAfter(r.Remote.RetryAfter)
		}
		return e
	case ClassDeadline:
		reason, detail := errs.ReasonDeadlineExceeded, "a dependency did not answer in time"
		if !idempotent {
			reason, detail = errs.ReasonResultUnknown, "a dependency did not answer in time; query the result before repeating"
		}
		return errs.New(reason, detail).With("dependency", dependency).Wrap(r.Err)
	case ClassCancelled:
		return errs.New(errs.ReasonRequestCancelled, "the request was cancelled").Wrap(r.Err)
	}
	if r.Remote != nil {
		return r.Remote
	}
	if r.Err == nil {
		return errors.New("calls: an attempt failed without an error")
	}
	return errs.From(r.Err)
}
