package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"go.opentelemetry.io/otel/propagation"
)

// Received is one event as a handler sees it.
type Received struct {
	ID         string
	Type       string
	Source     string // urn:be:<component>
	RecordID   string
	Version    int64
	Projection Projection
	Stream     string
	Partition  int
	Seq        int64
	Epoch      string
	Impl       string
	Hops       int
	Data       []byte // the payload, claim checks already resolved
	Headers    map[string]string
}

// Decode unmarshals the payload.
func (r Received) Decode(v any) error { return json.Unmarshal(r.Data, v) }

// HandleFunc processes one event inside the consumer's transaction; the inbox entry commits with it (P6.25).
type HandleFunc func(ctx context.Context, tx db.Tx, e Received) error

// Sub declares one subscription of a module.
type Sub struct {
	Name    string   // unique within the component
	Streams []string // event streams it reads
	Types   []string // event types it handles; empty: every type of the streams
	Handle  HandleFunc
	// MaxDeliver, AckWait, Concurrency override the protocol's defaults; the deployment's
	// BUS_SUBSCRIPTION_CONCURRENCY overrides Concurrency (P6.31).
	MaxDeliver  int
	Concurrency int
	// Ordered marks a subscription that depends on arrival order: it always handles one message at a time, whatever
	// the deployment sets.
	Ordered bool
}

// ConcurrencyKey is the deployment's setting of how many messages each subscription of a member handles at once.
const ConcurrencyKey = "BUS_SUBSCRIPTION_CONCURRENCY"

// ConsumerKeys are the keys of a member that subscribes (P6.31).
func ConsumerKeys() []config.Key {
	lo, hi := int64(1), int64(64)
	return []config.Key{{Name: ConcurrencyKey, Type: config.Integer, Min: &lo, Max: &hi}}
}

// Concurrency is a subscription's effective concurrency (P6.31): 1 when ordered, else the deployment's setting
// (configured, 0 when unset), else the declaration, else 1.
func Concurrency(s Sub, configured int) int {
	switch {
	case s.Ordered:
		return 1
	case configured > 0:
		return configured
	case s.Concurrency > 0:
		return s.Concurrency
	}
	return 1
}

// Subscriptions collects a module's subscriptions.
type Subscriptions struct{ list []Sub }

// Add declares a subscription.
func (s *Subscriptions) Add(sub Sub) { s.list = append(s.list, sub) }

// List returns them.
func (s *Subscriptions) List() []Sub { return s.list }

// Subscriber is implemented by modules that consume events.
type Subscriber interface {
	Subscriptions(s *Subscriptions) error
}

// Consumer runs one subscription of a member.
type Consumer struct {
	Member string // component ID
	Sub    Sub
	DB     *db.DB
	Claims ClaimReader
	Log    *slog.Logger
	// Configured is the member's BUS_SUBSCRIPTION_CONCURRENCY, 0 when unset (P6.31).
	Configured int
}

// Spec is the transport subscription of this consumer.
func (c *Consumer) Spec() SubscriptionSpec {
	var filters []string
	for _, st := range c.Sub.Streams {
		for _, typ := range c.Sub.Types {
			filters = append(filters, "be."+st+".*."+typ)
		}
	}
	return SubscriptionSpec{Durable: DurableName(c.Member, c.Sub.Name), Streams: c.Sub.Streams, Filters: filters,
		MaxDeliver: c.Sub.MaxDeliver, Concurrency: Concurrency(c.Sub, c.Configured)}.Defaults()
}

// Handle is the transport handler: claim check, hop limit, inbox and business write in one transaction; a failure on
// the last delivery becomes a dead letter (P6.20).
func (c *Consumer) Handle(ctx context.Context, d *Delivery) Outcome {
	spec := c.Spec()
	err := c.process(ctx, d)
	if err == nil {
		return Ack
	}
	if d.Attempt < spec.MaxDeliver {
		c.Log.Warn("an event failed; it will be delivered again", "subscription", c.Sub.Name, "event_id", d.ID(),
			"attempt", d.Attempt, "error", err.Error())
		return Nak
	}
	if derr := c.deadLetter(context.WithoutCancel(ctx), d, err); derr != nil {
		c.Log.Error("an event failed for the last time and its dead letter could not be written", "subscription",
			c.Sub.Name, "event_id", d.ID(), "error", derr.Error())
		return Nak // not lost: the transport keeps it until the dead letter is written or the limit stops it
	}
	c.Log.Error("an event failed for the last time; recorded as a dead letter", "subscription", c.Sub.Name,
		"event_id", d.ID(), "attempts", d.Attempt, "error", err.Error())
	return Term
}

func (c *Consumer) process(ctx context.Context, d *Delivery) error {
	r, err := received(d)
	if err != nil {
		return err
	}
	if r.Hops > MaxHops {
		return fmt.Errorf("event %s: %d hops exceed the limit of %d", r.ID, r.Hops, MaxHops)
	}
	if r.Data, err = ResolveClaim(ctx, d.Message, c.Claims); err != nil {
		return err
	}
	carrier := propagation.MapCarrier{}
	for _, k := range []string{"traceparent", "tracestate"} {
		if v := d.Headers["ce-"+k]; v != "" {
			carrier[k] = v
		}
	}
	ctx = WithCause(obs.Propagator.Extract(ctx, carrier), r.ID, r.Hops)
	return c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO be_inbox (subscription, event_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
			c.Sub.Name, r.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // already applied: a redelivery takes effect once
		}
		return c.Sub.Handle(ctx, tx, r)
	})
}

func (c *Consumer) deadLetter(ctx context.Context, d *Delivery, cause error) error {
	headers, _ := json.Marshal(d.Headers)
	return c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO be_dead_letter (subscription, event_id, type, headers, payload, error, attempts)
			VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING`, c.Sub.Name, d.ID(), d.Headers[HeaderType],
			headers, d.Data, cause.Error(), d.Attempt)
		return err
	})
}

func received(d *Delivery) (Received, error) {
	h := d.Headers
	r := Received{ID: h[HeaderID], Type: h[HeaderType], Source: h[HeaderSource], RecordID: h[HeaderSubject],
		Projection: Projection(h[HeaderProjection]), Stream: h[HeaderStream], Epoch: h[HeaderEpoch], Impl: h[HeaderImpl],
		Headers: h}
	if r.ID == "" || r.Type == "" {
		return r, errors.New("the envelope has no ce-id or ce-type")
	}
	var err error
	num := func(key string) int64 {
		if v, ok := h[key]; ok && err == nil {
			var n int64
			if n, err = strconv.ParseInt(v, 10, 64); err != nil {
				err = fmt.Errorf("envelope header %s: %q", key, v)
			}
			return n
		}
		return 0
	}
	r.Version, r.Seq = num(HeaderVersion), num(HeaderSeq)
	r.Partition, r.Hops = int(num(HeaderPartition)), int(num(HeaderHops))
	return r, err
}
