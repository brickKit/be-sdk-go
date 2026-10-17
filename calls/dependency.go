package calls

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// Dependency is one (member, dependency, surface): its concurrency limit, breaker and retry budget.
type Dependency struct {
	ID      string // the dependency's component ID
	Surface string // "http" or "grpc"
	limit   chan struct{}
	breaker *Breaker
	budget  *Budget
	now     func() time.Time // the breaker's clock
	sleep   func(context.Context, time.Duration) error
	metrics *metrics
}

// Breaker returns the dependency's circuit breaker (for metrics and tests).
func (d *Dependency) Breaker() *Breaker { return d.breaker }

// Budget returns the dependency's retry budget.
func (d *Dependency) Budget() *Budget { return d.budget }

// Do runs one call: at most MaxAttempts attempts, each given its own shrinking timeout. attempt must honour its
// context and report what came back. The returned error is the caller's answer upward (P4.19).
func (d *Dependency) Do(ctx context.Context, idempotent bool, attempt func(context.Context) Result) error {
	if db.InTx(ctx) {
		return errs.New(errs.ReasonInternal, "").Wrap(errors.New("NETWORK_IN_TX: an outbound call inside a database transaction"))
	}
	select {
	case d.limit <- struct{}{}:
		defer func() { <-d.limit }()
	default:
		d.metrics.rejected(d, "outbound_limit")
		return errs.New(errs.ReasonOutboundLimit, "too many calls to a dependency are in progress").
			With("dependency", d.ID).WithRetryAfter(time.Second)
	}
	var last Result
	var lastClass Class
	for n := 1; ; n++ {
		remaining, hasDeadline := remainingOf(ctx)
		timeout, send := AttemptTimeout(remaining, hasDeadline)
		if !send {
			if n > 1 {
				return Upward(lastClass, last, idempotent, d.ID)
			}
			d.metrics.rejected(d, "deadline_budget")
			return errs.New(errs.ReasonDeadlineBudgetExhausted, "not enough time was left to call a dependency").With("dependency", d.ID)
		}
		permit, admitted := d.breaker.Start(d.now())
		d.metrics.state(d)
		if !admitted {
			if n > 1 {
				return Upward(lastClass, last, idempotent, d.ID)
			}
			d.metrics.rejected(d, "circuit_open")
			return errs.New(errs.ReasonCircuitOpen, "a dependency is failing; calls to it are paused").
				With("dependency", d.ID).WithRetryAfter(time.Second)
		}
		actx, cancel := context.WithTimeout(ctx, timeout)
		r := attempt(actx)
		cancel()
		class := Classify(r, errors.Is(ctx.Err(), context.Canceled))
		d.breaker.End(permit, d.now(), class.BreakerOutcome())
		d.budget.Record(class.BudgetOutcome())
		d.metrics.attempt(d, class)
		d.metrics.state(d)
		if class == ClassSuccess {
			return nil
		}
		last, lastClass = r, class
		backoff := time.Duration(rand.Int64N(int64(MaxBackoff(n)) + 1))
		left, hasDeadline := remainingOf(ctx)
		if !ShouldRetry(idempotent, class.RetryOutcome(), n, d.budget.AllowsRetry(), left-backoff, hasDeadline) {
			return Upward(class, r, idempotent, d.ID)
		}
		if err := d.sleep(ctx, backoff); err != nil {
			return Upward(ClassCancelled, Result{Err: err}, idempotent, d.ID)
		}
	}
}

// remainingOf reads the deadline against the real clock; only the breaker's clock is replaceable (tests).
func remainingOf(ctx context.Context) (time.Duration, bool) {
	dl, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(dl), true
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Member holds one member's dependencies: each (dependency, surface) is created once and kept for the member's life.
type Member struct {
	mu      sync.Mutex
	deps    map[string]*Dependency
	metrics *metrics
	now     func() time.Time
	client  *http.Client
	conns   map[string]*grpc.ClientConn
	creds   Credentials
}

// NewMember creates the member's outbound state; reg is the member's own registry (nil: no metrics).
func NewMember(reg prometheus.Registerer) (*Member, error) {
	m, err := newMetrics(reg)
	if err != nil {
		return nil, err
	}
	return &Member{deps: map[string]*Dependency{}, metrics: m, now: time.Now}, nil
}

// Dependency returns the state of one dependency on one surface.
func (m *Member) Dependency(id, surface string) *Dependency {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dependencyLocked(id, surface)
}

func (m *Member) dependencyLocked(id, surface string) *Dependency {
	key := id + " " + surface
	if d, ok := m.deps[key]; ok {
		return d
	}
	d := &Dependency{ID: id, Surface: surface, limit: make(chan struct{}, OutboundLimit), breaker: NewBreaker(),
		budget: NewBudget(), now: m.now, sleep: sleepCtx, metrics: m.metrics}
	m.deps[key] = d
	return d
}

type metrics struct {
	attempts *prometheus.CounterVec
	rejects  *prometheus.CounterVec
	open     *prometheus.GaugeVec
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	m := &metrics{
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "be_outbound_attempts_total",
			Help: "Outbound call attempts by dependency, surface and how they counted."}, []string{"dependency", "surface", "class"}),
		rejects: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "be_outbound_rejected_total",
			Help: "Outbound calls not sent, by dependency, surface and reason."}, []string{"dependency", "surface", "reason"}),
		open: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "be_outbound_breaker_open",
			Help: "1 while the circuit breaker of a dependency is not closed."}, []string{"dependency", "surface"}),
	}
	if reg != nil {
		for _, c := range []prometheus.Collector{m.attempts, m.rejects, m.open} {
			if err := reg.Register(c); err != nil {
				return nil, err
			}
		}
	}
	return m, nil
}

func (m *metrics) attempt(d *Dependency, c Class) {
	m.attempts.WithLabelValues(d.ID, d.Surface, string(c)).Inc()
}

func (m *metrics) rejected(d *Dependency, reason string) {
	m.rejects.WithLabelValues(d.ID, d.Surface, reason).Inc()
}

func (m *metrics) state(d *Dependency) {
	v := 0.0
	if d.breaker.State() != Closed {
		v = 1
	}
	m.open.WithLabelValues(d.ID, d.Surface).Set(v)
}
