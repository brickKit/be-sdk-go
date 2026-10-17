package events_test

import (
	"context"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/transporttest"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/stretchr/testify/assert"
)

func memoryFactory(t transporttest.T) (events.Transport, string) {
	tr := events.NewMemory(64 << 10)
	t.Cleanup(func() { _ = tr.Close() })
	return tr, "t" + ids.New().String()[24:]
}

func TestMemoryTransportPassesTheSuite(t *testing.T) { transporttest.Run(t, memoryFactory) }

// lossyAcks forgets acknowledgements: every acked message comes back after the ack wait.
type lossyAcks struct{ *events.Memory }

func (l lossyAcks) Subscribe(ctx context.Context, s events.SubscriptionSpec, h events.Handler) (events.Subscription, error) {
	return l.Memory.Subscribe(ctx, s, func(ctx context.Context, d *events.Delivery) events.Outcome {
		h(ctx, d)
		_ = d.InProgress
		return events.Nak // the ack is lost on the way
	})
}

func TestTheSuiteFailsAnAdapterThatLosesAcks(t *testing.T) { // P6.24
	t.Parallel()
	var acked transporttest.Case
	for _, c := range transporttest.Cases {
		if c.Name == "an acknowledged message is never delivered again" {
			acked = c
		}
	}
	failed, msgs := transporttest.Probe(acked, func(t transporttest.T) (events.Transport, string) {
		tr, s := memoryFactory(t)
		return lossyAcks{tr.(*events.Memory)}, s
	})
	assert.True(t, failed, "the suite must see red")
	assert.NotEmpty(t, msgs)
}
