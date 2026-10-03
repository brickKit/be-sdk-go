package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
)

// Consumer timing (P12.5, P12.9).
const (
	DefaultConcurrency = 4                                // deliveries handled at once per subscription
	KeepAliveEvery     = envelope.AckWait / 3             // InProgress while a handler runs: 10 s
	HandlerTimeout     = envelope.AckWait - 5*time.Second // a handler's deadline: 25 s
	SettleTimeout      = 5 * time.Second                  // Ack, and the dead-letter publish
)

// Results of be_consumer_handled_total (P18).
const (
	ResultApplied = "applied"
	ResultSkipped = "skipped"
	ResultNak     = "nak"
	ResultDLQ     = "dlq"
)

// Subscription consumes one subject through the component's durable (P12.5–P12.7). Exactly one of
// Apply and Run is set.
type Subscription struct {
	Subject             string
	Consumer            string // the cursor's consumer (projection) name; "" is the component's default (P12.6)
	AggregateType       string // the producer contract's x-aggregate-type
	TransactionDocument bool   // the producer contract's x-transaction-document (P11.8)
	// Apply runs inside the cursor's transaction and writes locally only (P12.7).
	Apply func(ctx context.Context, tx *pg.Tx, ev envelope.Event) error
	// Run runs outside any transaction, may call the network, is idempotent on a business key; the
	// cursor advances after it succeeded (P12.7).
	Run func(ctx context.Context, ev envelope.Event) error
	// Validate, when set, checks the payload against the producer's contract; a violation is
	// permanent (P12.7).
	Validate    func(payload []byte) error
	MaxDeliver  int             // EVENTS_MAX_DELIVER, resolved by the caller; ≤ 0 = 8
	Backoff     []time.Duration // EVENTS_BACKOFF, resolved by the caller; empty = 1s,10s,1m,5m,15m,30m,1h
	Concurrency int             // ≤ 0 = 4
}

// ConsumerMetrics reports a subscription to the root's metrics (P18); every hook is optional.
type ConsumerMetrics struct {
	Handled      func(subject, result string)          // be_consumer_handled_total{subject,result}
	Lag          func(subject string, seconds float64) // be_consumer_lag_seconds{subject}
	DeadLettered func(subject string)                  // be_dlq_messages_total{subject}
}

// Consumer runs one subscription of one member (P12.5–P12.9, P11.8).
type Consumer struct {
	Bus          Source
	Store        *pg.Store
	ComponentID  string // names the durable: in a shell the member's ID
	Subscription Subscription
	Publisher    Publisher // the dead letters (P12.7)
	Logger       *slog.Logger
	Metrics      ConsumerMetrics
	// Context, when set, derives the handler's context: log fields, the span link of P18.1 and
	// the "event being handled" that gives causation to events published from it (P12.8).
	Context func(ctx context.Context, ev envelope.Event, in Inbound) context.Context
}

// Run consumes the durable until ctx is cancelled and every handler it started has returned. It
// fails at once on an invalid configuration; bus outages are survived by the Source.
func (c *Consumer) Run(ctx context.Context) error {
	if c.Bus == nil || c.Store == nil {
		return errors.New("events: consumer needs Bus and Store")
	}
	r, err := c.newRunner(pgCursors{store: c.Store})
	if err != nil {
		return err
	}
	return r.run(ctx)
}

// cursorKey is the primary key of besdk_event_cursor (P12.6).
type cursorKey struct{ consumer, aggregateType, aggregateID string }

// cursors is the consumer's view of besdk_event_cursor (pgCursors in production).
type cursors interface {
	// apply upserts the cursor and runs fn in the same transaction; false = stale or duplicate,
	// fn not run.
	apply(ctx context.Context, k cursorKey, ev envelope.Event, fn func(context.Context, *pg.Tx) error) (bool, error)
	current(ctx context.Context, k cursorKey) (version int64, ok bool, err error)
	advance(ctx context.Context, k cursorKey, ev envelope.Event) error
}

// consumerRunner is one validated subscription.
type consumerRunner struct {
	*Consumer
	cur                      cursors
	stream, durable, dlqSubj string
	accept                   envelope.Subscription
	log                      *slog.Logger
}

func (c *Consumer) newRunner(cur cursors) (*consumerRunner, error) {
	s := c.Subscription
	if (s.Apply == nil) == (s.Run == nil) {
		return nil, fmt.Errorf("events: subscription %s needs exactly one of Apply and Run", s.Subject)
	}
	if c.Publisher == nil {
		return nil, fmt.Errorf("events: subscription %s has no dead-letter publisher", s.Subject)
	}
	durable, dlq, err := envelope.Durable(c.ComponentID, s.Subject)
	if err != nil {
		return nil, fmt.Errorf("events: subscription %s: %w", s.Subject, err)
	}
	stream, _, err := envelope.Stream(s.Subject)
	if err != nil {
		return nil, err
	}
	return &consumerRunner{Consumer: c, cur: cur, stream: stream, durable: durable, dlqSubj: dlq,
		accept: envelope.Subscription{ComponentID: c.ComponentID, Subject: s.Subject,
			AggregateType: s.AggregateType, TransactionDocument: s.TransactionDocument},
		log: logger(c.Logger).With("subject", s.Subject, "durable", durable)}, nil
}

func (r *consumerRunner) run(ctx context.Context) error {
	n := r.Subscription.Concurrency
	if n <= 0 {
		n = DefaultConcurrency
	}
	return r.Bus.Consume(ctx, r.stream, r.durable, n, r.handle)
}
