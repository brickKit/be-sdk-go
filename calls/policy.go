// Package calls is the outbound side of protocol chapter 04: per (member, dependency) a concurrency limit, a circuit
// breaker and a retry budget; per call a deadline that only shrinks, at most three attempts for idempotent methods
// when the dependency itself is unavailable, and one answer upward (P4.19). The HTTP and gRPC clients both go
// through Dependency.Do.
package calls

import (
	"sync"
	"time"
)

// Parameters of protocol chapter 04.
const (
	MaxAttemptTimeout    = 3000 * time.Millisecond
	DeadlineReserve      = 50 * time.Millisecond
	MaxAttempts          = 3
	BackoffBase          = 100 * time.Millisecond
	BackoffCap           = 1000 * time.Millisecond
	OutboundLimit        = 64
	BreakerWindow        = 20
	BreakerOpenFor       = 30 * time.Second
	BreakerTrialsToClose = 3
)

// AttemptTimeout is P4.4: min(3000 ms, remaining - 50 ms); not sent when less than 50 ms remain or there is no
// deadline at all (hasDeadline false).
func AttemptTimeout(remaining time.Duration, hasDeadline bool) (time.Duration, bool) {
	if !hasDeadline || remaining < DeadlineReserve {
		return 0, false
	}
	return min(MaxAttemptTimeout, remaining-DeadlineReserve), true
}

// Retry outcomes fed to the decision (protocol vector retry-decision).
const (
	OutcomeUnavailable   = "UNAVAILABLE"
	OutcomeConnectFailed = "CONNECT_FAILED_BEFORE_SEND"
	OutcomeNone          = "NONE"
)

// ShouldRetry is P4.6 and P4.7: only idempotent methods, only when the dependency itself is unavailable, at most
// MaxAttempts attempts, only while the budget allows, and only when after the backoff enough time remains for another
// attempt.
func ShouldRetry(idempotent bool, outcome string, attemptsMade int, budgetAllows bool, afterBackoff time.Duration,
	hasDeadline bool) bool {
	if !idempotent || (outcome != OutcomeUnavailable && outcome != OutcomeConnectFailed) {
		return false
	}
	if attemptsMade >= MaxAttempts || !budgetAllows {
		return false
	}
	_, ok := AttemptTimeout(afterBackoff, hasDeadline)
	return ok
}

// MaxBackoff is the cap of the full-jitter backoff after failed failures: min(1000, 100 * 2^(n-1)) ms.
func MaxBackoff(failed int) time.Duration {
	if failed < 1 {
		return 0
	}
	if failed > 4 {
		return BackoffCap
	}
	return min(BackoffCap, BackoffBase<<(failed-1))
}

// Budget is the retry budget of P4.8 in thousandths: gRPC retryThrottling with maxTokens 10 and tokenRatio 0.1.
type Budget struct {
	mu     sync.Mutex
	tokens int
}

// Budget outcomes.
const (
	BudgetSuccess     = "success"
	BudgetUnavailable = "unavailable"
	BudgetOther       = "other"
)

const (
	budgetMax       = 10000
	budgetSuccess   = 100
	budgetFailure   = 1000
	budgetThreshold = 5000
)

// NewBudget returns a full bucket.
func NewBudget() *Budget { return &Budget{tokens: budgetMax} }

// Record counts one attempt's outcome.
func (b *Budget) Record(outcome string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch outcome {
	case BudgetSuccess:
		b.tokens = min(budgetMax, b.tokens+budgetSuccess)
	case BudgetUnavailable:
		b.tokens = max(0, b.tokens-budgetFailure)
	}
}

// Tokens returns the current tokens in thousandths.
func (b *Budget) Tokens() int { b.mu.Lock(); defer b.mu.Unlock(); return b.tokens }

// AllowsRetry is true while the tokens are strictly above half.
func (b *Budget) AllowsRetry() bool { return b.Tokens() > budgetThreshold }
