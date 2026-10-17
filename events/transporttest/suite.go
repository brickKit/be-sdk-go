// Package transporttest is the transport conformance suite of protocol P6.16-P6.24: every adapter runs the same
// checks. Cases take a small testing interface so the suite can also be run against a broken adapter and be seen
// to fail.
package transporttest

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/ids"
)

// T is the part of testing.TB the cases use.
type T interface {
	Helper()
	Errorf(format string, args ...any)
	FailNow()
	Cleanup(func())
}

// Factory returns a fresh transport and a unique stream name for one case (cases never share durables or streams).
type Factory func(t T) (events.Transport, string)

// Case is one check.
type Case struct {
	Name string
	Run  func(t T, f Factory)
}

// Timing used by the cases: short enough for tests, long enough for real brokers.
const (
	AckWait = 1500 * time.Millisecond
	Backoff = 200 * time.Millisecond
)

func fail(t T, format string, args ...any) {
	t.Helper()
	t.Errorf(format, args...)
	t.FailNow()
}

func must(t T, err error) {
	t.Helper()
	if err != nil {
		fail(t, "%v", err)
	}
}

func msg(stream string, partition int, typ string, data string) events.Message {
	id := ids.New().String()
	return events.Message{Subject: events.Subject(stream, partition, typ),
		Headers: map[string]string{events.HeaderID: id, "ce-type": typ, "ce-specversion": "1.0"}, Data: []byte(data)}
}

func spec(stream, durable string) events.SubscriptionSpec {
	return events.SubscriptionSpec{Durable: durable, Streams: []string{stream}, MaxDeliver: 3, AckWait: AckWait,
		Backoff: []time.Duration{Backoff}}
}

func setup(t T, f Factory) (events.Transport, string, context.Context) {
	t.Helper()
	tr, stream := f(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	must(t, tr.EnsureStream(ctx, events.Stream{Name: stream, Partitions: 4}))
	return tr, stream, ctx
}

// recorder collects deliveries.
type recorder struct {
	mu   sync.Mutex
	got  []*events.Delivery
	byID map[string]int
}

func newRecorder() *recorder { return &recorder{byID: map[string]int{}} }

func (r *recorder) add(d *events.Delivery) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, d)
	r.byID[d.ID()]++
}

func (r *recorder) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.got) }

func (r *recorder) distinct() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.byID) }

func eventually(t T, within time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	fail(t, "not within %s: %s", within, what)
}

func subscribe(t T, tr events.Transport, ctx context.Context, s events.SubscriptionSpec, h events.Handler) events.Subscription {
	t.Helper()
	sub, err := tr.Subscribe(ctx, s, h)
	must(t, err)
	t.Cleanup(func() { _ = sub.Stop() })
	return sub
}

// Cases is the suite.
var Cases = []Case{
	{"offline consumer receives what was published meanwhile", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		h := func(_ context.Context, d *events.Delivery) events.Outcome { rec.add(d); return events.Ack }
		sub, err := tr.Subscribe(ctx, spec(stream, "offline"), h)
		must(t, err)
		must(t, sub.Stop())
		for i := 0; i < 3; i++ {
			must(t, tr.Publish(ctx, msg(stream, i, "demo.thing_happened.v1", strconv.Itoa(i))))
		}
		subscribe(t, tr, ctx, spec(stream, "offline"), h)
		eventually(t, 10*time.Second, func() bool { return rec.distinct() == 3 }, "three messages after coming back")
	}},
	{"instances of one durable each message once", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		h := func(_ context.Context, d *events.Delivery) events.Outcome { rec.add(d); return events.Ack }
		subscribe(t, tr, ctx, spec(stream, "compete"), h)
		subscribe(t, tr, ctx, spec(stream, "compete"), h)
		for i := 0; i < 40; i++ {
			must(t, tr.Publish(ctx, msg(stream, i%4, "demo.thing_happened.v1", strconv.Itoa(i))))
		}
		eventually(t, 15*time.Second, func() bool { return rec.distinct() == 40 }, "all 40 handled")
		time.Sleep(AckWait + 500*time.Millisecond)
		if n := rec.count(); n != 40 {
			fail(t, "%d deliveries for 40 messages: each is handled by one instance once", n)
		}
	}},
	{"an acknowledged message is never delivered again", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		subscribe(t, tr, ctx, spec(stream, "acked"), func(_ context.Context, d *events.Delivery) events.Outcome {
			rec.add(d)
			return events.Ack
		})
		must(t, tr.Publish(ctx, msg(stream, 0, "demo.thing_happened.v1", "x")))
		eventually(t, 10*time.Second, func() bool { return rec.count() == 1 }, "delivered")
		time.Sleep(2*AckWait + 500*time.Millisecond)
		if n := rec.count(); n != 1 {
			fail(t, "%d deliveries of an acknowledged message", n)
		}
	}},
	{"a nak is redelivered after the backoff", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		var first atomic.Int64
		subscribe(t, tr, ctx, spec(stream, "nak"), func(_ context.Context, d *events.Delivery) events.Outcome {
			rec.add(d)
			if d.Attempt == 1 {
				first.Store(time.Now().UnixNano())
				return events.Nak
			}
			return events.Ack
		})
		must(t, tr.Publish(ctx, msg(stream, 0, "demo.thing_happened.v1", "x")))
		eventually(t, 10*time.Second, func() bool { return rec.count() == 2 }, "redelivered once")
		rec.mu.Lock()
		second := rec.got[1]
		rec.mu.Unlock()
		if second.Attempt != 2 {
			fail(t, "the redelivery says attempt %d, want 2", second.Attempt)
		}
		time.Sleep(AckWait)
		if n := rec.count(); n != 2 {
			fail(t, "%d deliveries; the second was acknowledged", n)
		}
	}},
	{"an unacknowledged message is redelivered after the ack wait", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		s := spec(stream, "slow")
		s.Concurrency = 2 // the redelivery reaches another handler while the first one hangs
		subscribe(t, tr, ctx, s, func(_ context.Context, d *events.Delivery) events.Outcome {
			rec.add(d)
			if d.Attempt == 1 {
				time.Sleep(AckWait + 700*time.Millisecond) // the handler hangs past the ack wait
			}
			return events.Ack
		})
		must(t, tr.Publish(ctx, msg(stream, 0, "demo.thing_happened.v1", "x")))
		eventually(t, 3*AckWait+5*time.Second, func() bool { return rec.count() >= 2 }, "redelivered after the ack wait")
	}},
	{"in progress keeps a slow message from being redelivered", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		subscribe(t, tr, ctx, spec(stream, "progress"), func(_ context.Context, d *events.Delivery) events.Outcome {
			rec.add(d)
			for i := 0; i < 6; i++ {
				time.Sleep(AckWait / 3)
				_ = d.InProgress()
			}
			return events.Ack
		})
		must(t, tr.Publish(ctx, msg(stream, 0, "demo.thing_happened.v1", "x")))
		eventually(t, 10*time.Second, func() bool { return rec.count() >= 1 }, "delivered")
		time.Sleep(2*AckWait + time.Second)
		if n := rec.count(); n != 1 {
			fail(t, "%d deliveries of a message kept in progress", n)
		}
	}},
	{"max deliver stops the deliveries", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		subscribe(t, tr, ctx, spec(stream, "poison"), func(_ context.Context, d *events.Delivery) events.Outcome {
			rec.add(d)
			return events.Nak
		})
		must(t, tr.Publish(ctx, msg(stream, 0, "demo.thing_happened.v1", "poison")))
		eventually(t, 10*time.Second, func() bool { return rec.count() == 3 }, "three deliveries")
		time.Sleep(AckWait + time.Second)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if len(rec.got) != 3 {
			fail(t, "%d deliveries, the limit is 3", len(rec.got))
		}
		for i, d := range rec.got {
			if d.Attempt != i+1 {
				fail(t, "delivery %d says attempt %d", i+1, d.Attempt)
			}
		}
	}},
	{"term ends the deliveries at once", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		subscribe(t, tr, ctx, spec(stream, "term"), func(_ context.Context, d *events.Delivery) events.Outcome {
			rec.add(d)
			return events.Term
		})
		must(t, tr.Publish(ctx, msg(stream, 0, "demo.thing_happened.v1", "x")))
		eventually(t, 10*time.Second, func() bool { return rec.count() == 1 }, "delivered")
		time.Sleep(AckWait + time.Second)
		if n := rec.count(); n != 1 {
			fail(t, "%d deliveries after term", n)
		}
	}},
	{"creating a durable again keeps its settings", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		h := func(_ context.Context, d *events.Delivery) events.Outcome { rec.add(d); return events.Nak }
		s := spec(stream, "settings")
		sub, err := tr.Subscribe(ctx, s, h)
		must(t, err)
		must(t, sub.Stop())
		s.MaxDeliver = 6
		subscribe(t, tr, ctx, s, h)
		must(t, tr.Publish(ctx, msg(stream, 0, "demo.thing_happened.v1", "x")))
		eventually(t, 10*time.Second, func() bool { return rec.count() >= 3 }, "deliveries")
		time.Sleep(2*AckWait + time.Second)
		if n := rec.count(); n != 3 {
			fail(t, "%d deliveries: the existing durable's limit of 3 was changed", n)
		}
	}},
	{"headers and payload round-trip unchanged", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		got := make(chan *events.Delivery, 1)
		subscribe(t, tr, ctx, spec(stream, "roundtrip"), func(_ context.Context, d *events.Delivery) events.Outcome {
			got <- d
			return events.Ack
		})
		m := msg(stream, 2, "demo.thing_happened.v1", `{"name":"仓库 1","bytes":"\u0000ÿ"}`)
		m.Headers["ce-besubjects"] = `["acc:01","org:7"]`
		m.Headers["ce-traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		m.Headers["ce-belegalentity"] = "法人 A"
		must(t, tr.Publish(ctx, m))
		select {
		case d := <-got:
			if d.Subject != m.Subject || string(d.Data) != string(m.Data) {
				fail(t, "subject or payload changed: %q %q", d.Subject, d.Data)
			}
			for k, v := range m.Headers {
				if d.Headers[k] != v {
					fail(t, "header %s: %q, want %q", k, d.Headers[k], v)
				}
			}
		case <-time.After(10 * time.Second):
			fail(t, "not delivered")
		}
	}},
	{"a message over the limit fails to publish", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		big := msg(stream, 0, "demo.thing_happened.v1", "")
		big.Data = make([]byte, tr.MaxMessage()+1)
		if err := tr.Publish(ctx, big); err == nil {
			fail(t, "a message over %d bytes was accepted", tr.MaxMessage())
		}
	}},
	{"the same message ID is delivered once", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		subscribe(t, tr, ctx, spec(stream, "dedup"), func(_ context.Context, d *events.Delivery) events.Outcome {
			rec.add(d)
			return events.Ack
		})
		m := msg(stream, 0, "demo.thing_happened.v1", "x")
		must(t, tr.Publish(ctx, m))
		must(t, tr.Publish(ctx, m)) // the forwarder's ack was lost and it published again
		eventually(t, 10*time.Second, func() bool { return rec.count() >= 1 }, "delivered")
		time.Sleep(AckWait)
		if n := rec.count(); n != 1 {
			fail(t, "%d deliveries of one message ID", n)
		}
	}},
	{"two durables each receive a copy", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		a, b := newRecorder(), newRecorder()
		subscribe(t, tr, ctx, spec(stream, "member_a"), func(_ context.Context, d *events.Delivery) events.Outcome { a.add(d); return events.Ack })
		subscribe(t, tr, ctx, spec(stream, "member_b"), func(_ context.Context, d *events.Delivery) events.Outcome { b.add(d); return events.Ack })
		for i := 0; i < 5; i++ {
			must(t, tr.Publish(ctx, msg(stream, i%4, "demo.thing_happened.v1", strconv.Itoa(i))))
		}
		eventually(t, 10*time.Second, func() bool { return a.distinct() == 5 && b.distinct() == 5 }, "five each")
	}},
	{"filters select subjects", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		rec := newRecorder()
		s := spec(stream, "filtered")
		s.Filters = []string{"be." + stream + ".*.demo.wanted.v1"}
		subscribe(t, tr, ctx, s, func(_ context.Context, d *events.Delivery) events.Outcome { rec.add(d); return events.Ack })
		must(t, tr.Publish(ctx, msg(stream, 1, "demo.unwanted.v1", "no")))
		must(t, tr.Publish(ctx, msg(stream, 2, "demo.wanted.v1", "yes")))
		eventually(t, 10*time.Second, func() bool { return rec.count() == 1 }, "the wanted one")
		time.Sleep(500 * time.Millisecond)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if len(rec.got) != 1 || string(rec.got[0].Data) != "yes" {
			fail(t, "%d deliveries", len(rec.got))
		}
	}},
	{"notifications are neither stored nor redelivered", func(t T, f Factory) {
		tr, stream, ctx := setup(t, f)
		subject := "be.notify." + stream
		must(t, tr.Notify(ctx, subject, []byte("before")))
		var mu sync.Mutex
		var got []string
		l, err := tr.Listen(ctx, subject, func(b []byte) { mu.Lock(); got = append(got, string(b)); mu.Unlock() })
		must(t, err)
		t.Cleanup(func() { _ = l.Stop() })
		time.Sleep(300 * time.Millisecond)
		must(t, tr.Notify(ctx, subject, []byte("while")))
		eventually(t, 5*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) >= 1 }, "received")
		time.Sleep(AckWait)
		mu.Lock()
		defer mu.Unlock()
		if fmt.Sprint(got) != "[while]" {
			fail(t, "received %v; only the notification sent while listening, once", got)
		}
	}},
}
