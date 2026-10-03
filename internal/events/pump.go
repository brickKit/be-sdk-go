package events

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/pg"
)

// Pump constants (P12.1).
const (
	BatchSize    = 256                    // rows claimed per round = acknowledgements in flight
	BusyPoll     = 200 * time.Millisecond // polling while rows are flowing
	IdlePoll     = 2 * time.Second        // polling while idle
	StatsEvery   = 5 * time.Second        // be_outbox_pending / be_outbox_oldest_age_seconds refresh
	BatchTimeout = 20 * time.Second       // publish + mark of one batch, below the 30 s claim
	MinBackoff   = time.Second            // first retry of a failed row
	MaxBackoff   = time.Minute            // retry ceiling
	maxLastError = 200                    // last_error is short and never holds the payload
)

// Publisher is the part of the bus the pump and the dead-lettering use (P12.1): one error per
// message, nil once the bus confirmed it stored it. *jetstream.Bus implements it.
type Publisher interface {
	PublishBatch(ctx context.Context, ms []jetstream.Message) []error
}

// PumpMetrics reports the outbox to the root's metrics (P18): every hook is optional.
type PumpMetrics struct {
	Pending   func(n int64)         // be_outbox_pending
	OldestAge func(seconds float64) // be_outbox_oldest_age_seconds
	Published func(subject string)  // be_events_published_total{subject}
}

// Pump publishes one member's outbox (P12.1). Run it under the caller's supervision; several
// pumps (replicas) on one schema publish each row once, the claim being FOR UPDATE SKIP LOCKED.
type Pump struct {
	Store       *pg.Store
	Bus         Publisher
	ComponentID string // ce-source; the version and contract file come from each row's ce-dataschema
	Logger      *slog.Logger
	Metrics     PumpMetrics
}

// Run pumps until ctx is cancelled, then finishes the batch in hand (publish and mark, on a short
// detached context) and returns nil (P12.1). A database or bus failure is logged and retried;
// a row is never dropped.
func (p *Pump) Run(ctx context.Context) error {
	if p.Store == nil || p.Bus == nil || p.ComponentID == "" {
		return errors.New("events: pump needs Store, Bus and ComponentID")
	}
	return p.newRunner(pgOutbox{store: p.Store}).run(ctx)
}

// outbox is the pump's view of besdk_outbox (pgOutbox in production).
type outbox interface {
	claim(ctx context.Context) ([]claimedRow, error)
	markPublished(ctx context.Context, rows []claimedRow) error
	markFailed(ctx context.Context, rows []failedRow) error
	stats(ctx context.Context) (pending int64, oldestSeconds float64, err error)
}

// claimedRow is a row the claim returned.
type claimedRow struct {
	outboxRow
	attempts int
}

// failedRow goes back to PENDING after delay.
type failedRow struct {
	id        string
	createdAt time.Time
	delay     time.Duration
	err       string
}

// pumpRunner is one Run: the pump, its outbox and the outage state.
type pumpRunner struct {
	*Pump
	ob     outbox
	log    *slog.Logger
	outage bool // the bus is unavailable: logged once when it starts and once when it ends
}

func (p *Pump) newRunner(ob outbox) *pumpRunner {
	return &pumpRunner{Pump: p, ob: ob, log: logger(p.Logger)}
}

func (r *pumpRunner) run(ctx context.Context) error {
	var wait time.Duration
	var lastStats time.Time
	for {
		if time.Since(lastStats) >= StatsEvery {
			r.refreshStats(ctx)
			lastStats = time.Now()
		}
		n, err := r.round(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			r.log.Warn("outbox claim failed; retrying", "err", err)
			wait = IdlePoll
		} else {
			wait = nextWait(wait, n)
		}
		if wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil
			case <-t.C:
			}
		}
	}
}

// nextWait adapts polling (P12.1): straight on after a full batch, BusyPoll after a non-empty one,
// doubling from BusyPoll up to IdlePoll while idle.
func nextWait(prev time.Duration, claimed int) time.Duration {
	switch {
	case claimed >= BatchSize:
		return 0
	case claimed > 0, prev < BusyPoll:
		return BusyPoll
	default:
		return min(prev*2, IdlePoll)
	}
}

// pumpBackoff is the delay before a row that failed on its attempts-th claim is retried (P12.1):
// 1 s doubling up to 1 min.
func pumpBackoff(attempts int) time.Duration {
	if attempts <= 1 {
		return MinBackoff
	}
	if attempts > 7 {
		return MaxBackoff
	}
	return min(MinBackoff<<(attempts-1), MaxBackoff)
}

func (r *pumpRunner) refreshStats(ctx context.Context) {
	n, age, err := r.ob.stats(ctx)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("outbox gauges not refreshed", "err", err)
		}
		return
	}
	if r.Metrics.Pending != nil {
		r.Metrics.Pending(n)
	}
	if r.Metrics.OldestAge != nil {
		r.Metrics.OldestAge(age)
	}
}
