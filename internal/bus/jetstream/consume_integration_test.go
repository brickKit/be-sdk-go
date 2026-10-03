package jetstream

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func durableFor(subj string) string { return "test_consumer__" + strings.ReplaceAll(subj, ".", "__") }

// P12.5: a durable's first creation delivers what is already in the stream.
func TestDeliverAllCatchesUp(t *testing.T) {
	b := testBus(t)
	seg, stream := testStream(t, b)
	subj := seg + ".order.created.v1"
	publishN(t, b, subj, 3)
	d := durableFor(subj)
	if _, err := b.EnsureDurable(context.Background(), stream, d, subj); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	consumeFor(t, b, stream, d, 4, 2*time.Second, func(ctx context.Context, m *Delivery) {
		if m.Subject != subj || m.StreamSeq == 0 || m.NumDelivered != 1 {
			t.Errorf("delivery fields: %+v", m)
		}
		n.Add(1)
		_ = m.Ack(ctx)
	})
	if n.Load() != 3 {
		t.Fatalf("handled %d of 3 pre-existing messages", n.Load())
	}
}

// P12.7: NakWithDelay redelivers not before the delay, with the delivery count incremented.
func TestNakWithDelay(t *testing.T) {
	b := testBus(t)
	seg, stream := testStream(t, b)
	subj := seg + ".order.created.v1"
	d := durableFor(subj)
	if _, err := b.EnsureDurable(context.Background(), stream, d, subj); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), Message{Subject: subj, ID: "x", Header: map[string]string{"ce-id": "x"}, Data: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var at []time.Time
	var nums []uint64
	consumeFor(t, b, stream, d, 4, 2*time.Second, func(ctx context.Context, m *Delivery) {
		mu.Lock()
		at, nums = append(at, time.Now()), append(nums, m.NumDelivered)
		first := len(at) == 1
		mu.Unlock()
		if m.Header["ce-id"] != "x" {
			t.Errorf("header ce-id = %q", m.Header["ce-id"])
		}
		if first {
			_ = m.NakWithDelay(300 * time.Millisecond)
			return
		}
		_ = m.Ack(ctx)
	})
	if len(at) != 2 || nums[0] != 1 || nums[1] != 2 {
		t.Fatalf("deliveries %v", nums)
	}
	if gap := at[1].Sub(at[0]); gap < 300*time.Millisecond {
		t.Fatalf("redelivered after %s, want >= 300ms", gap)
	}
}

// P12.9: a slow handler that reports progress every ack_wait/3 is executed once.
func TestKeepAliveStopsRedelivery(t *testing.T) {
	b := testBus(t)
	b.ackWait = time.Second // test-only: a short AckWait
	seg, stream := testStream(t, b)
	subj := seg + ".order.created.v1"
	d := durableFor(subj)
	if _, err := b.EnsureDurable(context.Background(), stream, d, subj); err != nil {
		t.Fatal(err)
	}
	publishN(t, b, subj, 1)
	var n atomic.Int32
	consumeFor(t, b, stream, d, 4, 4*time.Second, func(ctx context.Context, m *Delivery) {
		n.Add(1)
		stop := m.KeepAlive(ctx, b.ackWait/3)
		time.Sleep(2500 * time.Millisecond)
		stop()
		_ = m.Ack(ctx)
	})
	if n.Load() != 1 {
		t.Fatalf("slow handler executed %d times, want 1", n.Load())
	}
}

// Control for the test above: without KeepAlive the same handler is redelivered, so the test
// above really exercises InProgress.
func TestWithoutKeepAliveRedelivers(t *testing.T) {
	b := testBus(t)
	b.ackWait = time.Second
	seg, stream := testStream(t, b)
	subj := seg + ".order.created.v1"
	d := durableFor(subj)
	if _, err := b.EnsureDurable(context.Background(), stream, d, subj); err != nil {
		t.Fatal(err)
	}
	publishN(t, b, subj, 1)
	var n atomic.Int32
	consumeFor(t, b, stream, d, 4, 4*time.Second, func(ctx context.Context, m *Delivery) {
		if n.Add(1) == 1 {
			time.Sleep(2500 * time.Millisecond)
		}
		_ = m.Ack(ctx)
	})
	if n.Load() < 2 {
		t.Fatalf("slow handler without KeepAlive executed %d times, want >= 2", n.Load())
	}
}

// P12.9: at most `concurrency` handlers at once; Consume returns after in-flight handlers returned.
func TestConsumeConcurrencyBound(t *testing.T) {
	b := testBus(t)
	seg, stream := testStream(t, b)
	subj := seg + ".order.created.v1"
	d := durableFor(subj)
	if _, err := b.EnsureDurable(context.Background(), stream, d, subj); err != nil {
		t.Fatal(err)
	}
	publishN(t, b, subj, 20)
	var cur, peak, done atomic.Int32
	consumeFor(t, b, stream, d, 3, 3*time.Second, func(ctx context.Context, m *Delivery) {
		c := cur.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		cur.Add(-1)
		_ = m.Ack(ctx)
		done.Add(1)
	})
	if peak.Load() > 3 || peak.Load() < 2 {
		t.Fatalf("peak concurrency %d, want 2..3", peak.Load())
	}
	if done.Load() != 20 {
		t.Fatalf("handled %d of 20", done.Load())
	}
	if cur.Load() != 0 {
		t.Fatalf("Consume returned with %d handlers still running", cur.Load())
	}
}

// Term stops redelivery.
func TestTerm(t *testing.T) {
	b := testBus(t)
	b.ackWait = time.Second
	seg, stream := testStream(t, b)
	subj := seg + ".order.created.v1"
	d := durableFor(subj)
	if _, err := b.EnsureDurable(context.Background(), stream, d, subj); err != nil {
		t.Fatal(err)
	}
	publishN(t, b, subj, 1)
	var n atomic.Int32
	consumeFor(t, b, stream, d, 4, 2500*time.Millisecond, func(ctx context.Context, m *Delivery) {
		n.Add(1)
		_ = m.Term()
	})
	if n.Load() != 1 {
		t.Fatalf("terminated message delivered %d times", n.Load())
	}
}
