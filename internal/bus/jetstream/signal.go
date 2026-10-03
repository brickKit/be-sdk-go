package jetstream

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

// Signal publishes a best-effort poke with a core NATS publish (P12.10): not stored, not
// redelivered, may be lost; every user of one also polls.
func (b *Bus) Signal(subject string, data []byte) error {
	if !b.nc.IsConnected() {
		return notConnected()
	}
	if err := b.nc.Publish(subject, data); err != nil {
		return fmt.Errorf("jetstream: signal %s: %w", subject, classify(err))
	}
	return nil
}

// OnSignal subscribes fn to a best-effort poke subject with a core NATS subscription (P12.10). In
// a shell every member subscribes separately. fn runs on the subscription's goroutine, one poke at
// a time.
func (b *Bus) OnSignal(subject string, fn func([]byte)) (unsubscribe func(), err error) {
	s, err := b.nc.Subscribe(subject, func(m *nats.Msg) { fn(m.Data) })
	if err != nil {
		return nil, fmt.Errorf("jetstream: subscribe signal %s: %w", subject, classify(err))
	}
	return func() { _ = s.Unsubscribe() }, nil
}
