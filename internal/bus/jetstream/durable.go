package jetstream

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Durable consumer constants (P12.5). There is deliberately no server BackOff and no server
// MaxDeliver: the runtime counts deliveries and applies delays itself (P12.7).
const (
	DurableAckWait           = 30 * time.Second
	DurableMaxAckPending     = 256
	DurableMaxDeliver        = -1
	DurableInactiveThreshold = 30 * 24 * time.Hour
)

// durableConfig is the P12.5 server-side configuration of a pull durable. ackWait > 0 overrides
// DurableAckWait (tests only).
func durableConfig(durable, filterSubject string, ackWait time.Duration) jetstream.ConsumerConfig {
	if ackWait <= 0 {
		ackWait = DurableAckWait
	}
	return jetstream.ConsumerConfig{
		Durable: durable, FilterSubject: filterSubject,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait,
		MaxAckPending: DurableMaxAckPending, MaxDeliver: DurableMaxDeliver,
		DeliverPolicy: jetstream.DeliverAllPolicy, InactiveThreshold: DurableInactiveThreshold,
	}
}

// EnsureDurable creates the pull durable when it does not exist and never updates it (P12.5).
// It looks the durable up first; when it is missing it creates it with CreateConsumer (create-only:
// a concurrent creation with another config fails instead of changing it, and is then read back).
// For an existing durable it returns one warning per field that differs from the P12.5 constants;
// an operator's change stays.
func (b *Bus) EnsureDurable(ctx context.Context, stream, durable, filterSubject string) (warnings []string, err error) {
	want := durableConfig(durable, filterSubject, b.ackWait)
	c, err := b.js.Consumer(ctx, stream, durable)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		_, err = b.js.CreateConsumer(ctx, stream, want)
		if err == nil {
			return nil, nil
		}
		if !errors.Is(err, jetstream.ErrConsumerExists) {
			return nil, fmt.Errorf("jetstream: create durable %s on %s: %w", durable, stream, classify(err))
		}
		c, err = b.js.Consumer(ctx, stream, durable) // created concurrently with another config
	}
	if err != nil {
		return nil, fmt.Errorf("jetstream: look up durable %s on %s: %w", durable, stream, classify(err))
	}
	return durableDrift(want, c.CachedInfo().Config), nil
}

// durableDrift lists, by protocol field name, every P12.5 setting of got that differs from want.
func durableDrift(want, got jetstream.ConsumerConfig) []string {
	var w []string
	add := func(field string, gotV, wantV any) {
		w = append(w, fmt.Sprintf("durable %s: %s is %v, protocol value %v (left unchanged)", want.Durable, field, gotV, wantV))
	}
	if got.FilterSubject != want.FilterSubject {
		add("filter_subject", got.FilterSubject, want.FilterSubject)
	}
	if got.AckPolicy != want.AckPolicy {
		add("ack_policy", got.AckPolicy, want.AckPolicy)
	}
	if got.AckWait != want.AckWait {
		add("ack_wait", got.AckWait, want.AckWait)
	}
	if got.MaxAckPending != want.MaxAckPending {
		add("max_ack_pending", got.MaxAckPending, want.MaxAckPending)
	}
	if got.MaxDeliver != want.MaxDeliver {
		add("max_deliver", got.MaxDeliver, want.MaxDeliver)
	}
	if !slices.Equal(got.BackOff, want.BackOff) {
		add("backoff", got.BackOff, "none")
	}
	if got.DeliverPolicy != want.DeliverPolicy {
		add("deliver_policy", got.DeliverPolicy, want.DeliverPolicy)
	}
	if got.InactiveThreshold != want.InactiveThreshold {
		add("inactive_threshold", got.InactiveThreshold, want.InactiveThreshold)
	}
	return w
}
