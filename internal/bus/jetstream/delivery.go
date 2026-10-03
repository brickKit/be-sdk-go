package jetstream

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Delivery is one message handed to a Consume handler. The handler settles it exactly once with
// Ack, NakWithDelay or Term; a delivery left unsettled is redelivered after the durable's ack_wait.
type Delivery struct {
	Subject      string
	Header       map[string]string // first value per name, names as sent
	Data         []byte
	NumDelivered uint64 // the broker's delivery count n of this message (P12.7)
	StreamSeq    uint64 // the stream sequence (the DLQ message ID uses it, P12.7)
	msg          jetstream.Msg
}

func newDelivery(m jetstream.Msg, md *jetstream.MsgMetadata) *Delivery {
	h := make(map[string]string, len(m.Headers()))
	for k, v := range m.Headers() {
		if len(v) > 0 {
			h[k] = v[0]
		}
	}
	return &Delivery{
		Subject: m.Subject(), Header: h, Data: m.Data(),
		NumDelivered: md.NumDelivered, StreamSeq: md.Sequence.Stream, msg: m,
	}
}

// Ack acknowledges the delivery and waits for the server to confirm it (P12.6: after the cursor
// committed).
func (d *Delivery) Ack(ctx context.Context) error { return d.msg.DoubleAck(ctx) }

// NakWithDelay asks for redelivery not before delay: the runtime-side backoff of P12.7
// (EVENTS_BACKOFF[n − 1]); the durable has no server BackOff.
func (d *Delivery) NakWithDelay(delay time.Duration) error { return d.msg.NakWithDelay(delay) }

// Term stops redelivery of this message for good (after dead-lettering, P12.7).
func (d *Delivery) Term() error { return d.msg.Term() }

// InProgress resets the ack_wait timer of this delivery (P12.9).
func (d *Delivery) InProgress() error { return d.msg.InProgress() }

// KeepAlive sends InProgress every `every` (P12.9: ack_wait / 3 = 10 s; zero or negative means
// that default) until stop is called or ctx is done. stop is idempotent and returns after the
// sender stopped, so no InProgress follows an Ack.
func (d *Delivery) KeepAlive(ctx context.Context, every time.Duration) (stop func()) {
	if every <= 0 {
		every = DurableAckWait / 3
	}
	quit := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				_ = d.msg.InProgress()
			case <-quit:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(quit) })
		<-finished
	}
}
