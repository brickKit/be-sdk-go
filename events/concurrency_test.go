package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTheEffectiveConcurrencyOfASubscription(t *testing.T) { // P6.31
	for _, c := range []struct {
		name       string
		sub        Sub
		configured int
		want       int
	}{
		{"nothing set: one at a time", Sub{}, 0, 1},
		{"the declaration", Sub{Concurrency: 2}, 0, 2},
		{"the deployment over the declaration", Sub{Concurrency: 2}, 8, 8},
		{"the deployment without a declaration", Sub{}, 4, 4},
		{"ordered stays one", Sub{Ordered: true, Concurrency: 4}, 8, 1},
	} {
		assert.Equal(t, c.want, Concurrency(c.sub, c.configured), c.name)
		got := (&Consumer{Member: "demo/a", Sub: Sub{Name: "s", Streams: []string{"x"}, Concurrency: c.sub.Concurrency,
			Ordered: c.sub.Ordered}, Configured: c.configured}).Spec().Concurrency
		assert.Equal(t, c.want, got, c.name+": the transport subscription")
	}
}
