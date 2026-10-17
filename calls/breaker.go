package calls

import (
	"sync"
	"time"
)

// Breaker states.
const (
	Closed   = "closed"
	Open     = "open"
	HalfOpen = "half_open"
)

// Breaker outcomes.
const (
	BreakerSuccess = "success"
	BreakerFailure = "failure"
	BreakerIgnored = "ignored"
)

// Breaker is the circuit breaker of P4.11-P4.14 for one (member, dependency).
type Breaker struct {
	mu       sync.Mutex
	state    string
	window   []bool // true: dependency failure
	openedAt time.Time
	trial    uint64 // the permit holding the half-open trial slot, 0 when free
	trialOK  int
	next     uint64
}

// Permit is one admitted call; give it back to End.
type Permit struct{ id uint64 }

// NewBreaker returns a closed breaker.
func NewBreaker() *Breaker { return &Breaker{state: Closed} }

// State returns the current state.
func (b *Breaker) State() string { b.mu.Lock(); defer b.mu.Unlock(); return b.state }

// Start admits a call or rejects it (CIRCUIT_OPEN). now must come from a monotonic clock.
func (b *Breaker) Start(now time.Time) (Permit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	p := Permit{id: b.next}
	if b.state == Open {
		if now.Sub(b.openedAt) < BreakerOpenFor {
			return Permit{}, false
		}
		b.state, b.trial, b.trialOK = HalfOpen, 0, 0
	}
	if b.state == HalfOpen {
		if b.trial != 0 {
			return Permit{}, false
		}
		b.trial = p.id
	}
	return p, true
}

// End records the outcome of an admitted call.
func (b *Breaker) End(p Permit, now time.Time, outcome string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case HalfOpen:
		if b.trial != p.id {
			return // a late result of a call admitted before the breaker opened
		}
		b.trial = 0
		switch outcome {
		case BreakerFailure:
			b.state, b.openedAt, b.trialOK = Open, now, 0
		case BreakerSuccess:
			if b.trialOK++; b.trialOK >= BreakerTrialsToClose {
				b.state, b.window = Closed, nil
			}
		}
	case Closed:
		if outcome == BreakerIgnored {
			return
		}
		b.window = append(b.window, outcome == BreakerFailure)
		if len(b.window) > BreakerWindow {
			b.window = b.window[len(b.window)-BreakerWindow:]
		}
		failures := 0
		for _, f := range b.window {
			if f {
				failures++
			}
		}
		if len(b.window) == BreakerWindow && failures*2 > BreakerWindow {
			b.state, b.openedAt, b.window = Open, now, nil
		}
	}
}
