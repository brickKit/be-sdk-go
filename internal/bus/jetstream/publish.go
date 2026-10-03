package jetstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Message is one event to publish (P12.1). Header is sent verbatim (names as given); ID becomes
// Nats-Msg-Id, the key of JetStream's duplicate window (envelope: Nats-Msg-Id = ce-id).
type Message struct {
	Subject string
	ID      string
	Header  map[string]string
	Data    []byte
}

// PubAck is the bus's confirmation that it stored the message (P12.1). Duplicate is true when the
// duplicate window dropped it as a copy of an earlier message with the same ID: still stored once.
type PubAck struct {
	Stream    string
	Sequence  uint64
	Duplicate bool
}

// Publish publishes one message and returns only after the PubAck (P12.1). While the bus is
// unreachable it fails at once with an error wrapping ErrUnavailable.
func (b *Bus) Publish(ctx context.Context, m Message) (PubAck, error) {
	if !b.nc.IsConnected() {
		return PubAck{}, notConnected()
	}
	a, err := b.js.PublishMsg(ctx, natsMsg(m), jetstream.WithMsgID(m.ID))
	if err != nil {
		return PubAck{}, fmt.Errorf("jetstream: publish %s: %w", m.Subject, classify(err))
	}
	return PubAck{Stream: a.Stream, Sequence: a.Sequence, Duplicate: a.Duplicate}, nil
}

// PublishBatch publishes ms asynchronously, at most MaxPublishInFlight acknowledgements in flight
// on the connection, waits for each PubAck and returns one error per message (nil = stored;
// P12.1). A message whose PubAck did not arrive gets an error and must be retried.
func (b *Bus) PublishBatch(ctx context.Context, ms []Message) []error {
	errs := make([]error, len(ms))
	if !b.nc.IsConnected() {
		for i := range errs {
			errs[i] = notConnected()
		}
		return errs
	}
	done := make(chan struct{}, len(ms))
	for i, m := range ms {
		if err := b.inflight.Acquire(ctx, 1); err != nil {
			errs[i] = fmt.Errorf("jetstream: publish %s: %w", m.Subject, classify(err))
			done <- struct{}{}
			continue
		}
		f, err := b.js.PublishMsgAsync(natsMsg(m), jetstream.WithMsgID(m.ID))
		if err != nil {
			b.inflight.Release(1)
			errs[i] = fmt.Errorf("jetstream: publish %s: %w", m.Subject, classify(err))
			done <- struct{}{}
			continue
		}
		go func() {
			defer func() { b.inflight.Release(1); done <- struct{}{} }()
			errs[i] = awaitAck(ctx, f, m.Subject)
		}()
	}
	for range ms {
		<-done
	}
	return errs
}

// awaitAck waits for one async PubAck; the JetStream context's async timeout bounds the wait.
func awaitAck(ctx context.Context, f jetstream.PubAckFuture, subject string) error {
	select {
	case <-f.Ok():
		return nil
	case err := <-f.Err():
		return fmt.Errorf("jetstream: publish %s: %w", subject, classify(err))
	case <-ctx.Done():
		return fmt.Errorf("jetstream: publish %s: %w", subject, classify(ctx.Err()))
	}
}

func natsMsg(m Message) *nats.Msg {
	h := make(nats.Header, len(m.Header)+1)
	for k, v := range m.Header {
		h[k] = []string{v} // verbatim: no canonicalisation of the name
	}
	return &nats.Msg{Subject: m.Subject, Header: h, Data: m.Data}
}

func notConnected() error {
	return fmt.Errorf("jetstream: not connected: %w", ErrUnavailable)
}

// classify wraps err with ErrUnavailable when it means the bus could not be reached or did not
// answer in time, so callers retry (P12.1); other errors are returned as they are.
func classify(err error) error {
	unavailable := []error{
		nats.ErrConnectionClosed, nats.ErrConnectionDraining, nats.ErrConnectionReconnecting,
		nats.ErrDisconnected, nats.ErrReconnectBufExceeded, nats.ErrTimeout, nats.ErrNoServers,
		nats.ErrNoResponders, jetstream.ErrNoStreamResponse, jetstream.ErrAsyncPublishTimeout,
		jetstream.ErrJetStreamNotEnabled, jetstream.ErrJetStreamPublisherClosed,
		jetstream.ErrTooManyStalledMsgs, context.DeadlineExceeded,
	}
	for _, u := range unavailable {
		if errors.Is(err, u) {
			return fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
	}
	return err
}
