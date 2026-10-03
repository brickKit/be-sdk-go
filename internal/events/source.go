package events

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
)

// Settler settles one delivery exactly once (P12.6, P12.7, P12.9). *jetstream.Delivery
// implements it.
type Settler interface {
	Ack(ctx context.Context) error
	NakWithDelay(delay time.Duration) error
	Term() error
	KeepAlive(ctx context.Context, every time.Duration) (stop func())
}

// Inbound is one delivery of a durable (P12.5): the message, the broker's delivery count and
// stream sequence, and its settlement.
type Inbound struct {
	Subject      string
	Header       map[string]string
	Data         []byte
	NumDelivered uint64 // the broker's delivery count n (P12.7)
	StreamSeq    uint64 // names the dead-letter message (P12.7)
	Settle       Settler
}

// Source is the consuming side of the bus adapter (P12.12): it pulls from a durable and runs h on
// each delivery, at most concurrency at once, until ctx is cancelled (P12.9).
type Source interface {
	Consume(ctx context.Context, stream, durable string, concurrency int, h func(context.Context, Inbound)) error
}

// JetStream adapts the JetStream bus to Source.
type JetStream struct{ Bus *jetstream.Bus }

// Consume implements Source over jetstream.Bus.Consume.
func (j JetStream) Consume(ctx context.Context, stream, durable string, concurrency int, h func(context.Context, Inbound)) error {
	return j.Bus.Consume(ctx, stream, durable, concurrency, func(ctx context.Context, d *jetstream.Delivery) {
		h(ctx, Inbound{Subject: d.Subject, Header: d.Header, Data: d.Data, NumDelivered: d.NumDelivered,
			StreamSeq: d.StreamSeq, Settle: d})
	})
}
