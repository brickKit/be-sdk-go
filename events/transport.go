package events

import (
	"context"
	"errors"
	"time"
)

// Message is what travels: a subject, the envelope headers and the payload. The message ID (for the transport's
// deduplication window) is the ce-id header.
type Message struct {
	Subject string
	Headers map[string]string
	Data    []byte
}

// ID returns the message's event ID.
func (m Message) ID() string { return m.Headers[HeaderID] }

// Defaults of P6.19 and P6.20.
const (
	DefaultMaxDeliver   = 3
	DefaultAckWait      = 30 * time.Second
	DefaultBackoffStart = time.Second
	DefaultBackoffMax   = 30 * time.Second
)

// SubscriptionSpec declares a durable subscription (P6.18).
type SubscriptionSpec struct {
	Durable    string   // DurableName(componentID, name)
	Streams    []string // the event streams it reads
	Filters    []string // subjects (wildcards allowed); empty: every subject of the streams
	MaxDeliver int
	AckWait    time.Duration
	Backoff    []time.Duration // between deliveries; the last value repeats
	// Concurrency is how many messages one instance handles at once (default 1).
	Concurrency int
}

func (s SubscriptionSpec) withDefaults() SubscriptionSpec {
	if s.MaxDeliver <= 0 {
		s.MaxDeliver = DefaultMaxDeliver
	}
	if s.AckWait <= 0 {
		s.AckWait = DefaultAckWait
	}
	if len(s.Backoff) == 0 {
		for d := DefaultBackoffStart; d < DefaultBackoffMax; d *= 2 {
			s.Backoff = append(s.Backoff, d)
		}
		s.Backoff = append(s.Backoff, DefaultBackoffMax)
	}
	if s.Concurrency <= 0 {
		s.Concurrency = 1
	}
	return s
}

// Defaults applies the protocol's defaults to unset fields.
func (s SubscriptionSpec) Defaults() SubscriptionSpec { return s.withDefaults() }

// BackoffFor returns the delay before delivery number attempt+1.
func (s SubscriptionSpec) BackoffFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > len(s.Backoff) {
		return s.Backoff[len(s.Backoff)-1]
	}
	return s.Backoff[attempt-1]
}

// Delivery is one delivery of a message to a subscription.
type Delivery struct {
	Message
	Attempt int // 1 for the first delivery
	// InProgress extends the ack wait of a slow message (P6.19).
	InProgress func() error
}

// Outcome is what the handler decided.
type Outcome int

// Outcomes.
const (
	Ack  Outcome = iota // processed and committed
	Nak                 // failed; redeliver after the backoff
	Term                // never deliver again (dead-lettered)
)

// Handler handles one delivery.
type Handler func(ctx context.Context, d *Delivery) Outcome

// Subscription is a running subscription.
type Subscription interface {
	Stop() error
}

// Transport is the port both adapters implement (P6.16-P6.23).
type Transport interface {
	// EnsureStream creates the stream's storage if missing, never changing an existing one.
	EnsureStream(ctx context.Context, s Stream) error
	// Publish returns after the transport durably accepted the message.
	Publish(ctx context.Context, m Message) error
	// Subscribe creates the durable subscription if missing (never changing an existing one) and delivers to h.
	Subscribe(ctx context.Context, spec SubscriptionSpec, h Handler) (Subscription, error)
	// Notify sends a best-effort notification: not stored, not redelivered (P6.22).
	Notify(ctx context.Context, subject string, data []byte) error
	// Listen receives best-effort notifications while running.
	Listen(ctx context.Context, subject string, fn func(data []byte)) (Subscription, error)
	// MaxMessage is the largest message (headers and payload) the transport accepts.
	MaxMessage() int
	Close() error
}

// ErrTooLarge is returned by Publish for a message over the transport's limit (P6.21).
var ErrTooLarge = errors.New("events: the message exceeds the transport's limit")
