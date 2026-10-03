package events

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/stretchr/testify/require"
)

// opLog records, in order, what the fakes did (cursor commits, acks, ...).
type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) add(op string) { l.mu.Lock(); l.ops = append(l.ops, op); l.mu.Unlock() }
func (l *opLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ops...)
}

// fakeSettle is a delivery's settlement, recorded in the op log.
type fakeSettle struct {
	log       *opLog
	kaEvery   time.Duration
	kaStopped bool
}

func (f *fakeSettle) Ack(ctx context.Context) error { f.log.add("ack"); return nil }
func (f *fakeSettle) NakWithDelay(d time.Duration) error {
	f.log.add("nak " + d.String())
	return nil
}
func (f *fakeSettle) Term() error { f.log.add("term"); return nil }
func (f *fakeSettle) KeepAlive(ctx context.Context, every time.Duration) func() {
	f.kaEvery = every
	f.log.add("keepalive")
	return func() { f.kaStopped = true; f.log.add("keepalive-stop") }
}

// fakeCursors is besdk_event_cursor in memory; apply commits only when fn succeeds.
type fakeCursors struct {
	mu  sync.Mutex
	v   map[cursorKey]int64
	log *opLog
}

func (f *fakeCursors) apply(ctx context.Context, k cursorKey, ev envelope.Event, fn func(context.Context, *pg.Tx) error) (bool, error) {
	f.mu.Lock()
	cur, ok := f.v[k]
	f.mu.Unlock()
	if ok && cur >= ev.Version {
		return false, nil
	}
	if err := fn(ctx, nil); err != nil {
		return false, err
	}
	f.mu.Lock()
	f.v[k] = ev.Version
	f.mu.Unlock()
	f.log.add(fmt.Sprintf("commit v%d", ev.Version))
	return true, nil
}

func (f *fakeCursors) current(ctx context.Context, k cursorKey) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.v[k]
	return v, ok, nil
}

func (f *fakeCursors) advance(ctx context.Context, k cursorKey, ev envelope.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.v[k] < ev.Version {
		f.v[k] = ev.Version
	}
	f.log.add(fmt.Sprintf("advance v%d", ev.Version))
	return nil
}

// consumerRecorder collects the consumer metrics.
type consumerRecorder struct {
	mu      sync.Mutex
	handled map[string]int // "subject result"
	dlq     int
	lag     []float64
}

func (r *consumerRecorder) metrics() ConsumerMetrics {
	r.handled = map[string]int{}
	return ConsumerMetrics{
		Handled:      func(s, res string) { r.mu.Lock(); r.handled[s+" "+res]++; r.mu.Unlock() },
		DeadLettered: func(string) { r.mu.Lock(); r.dlq++; r.mu.Unlock() },
		Lag:          func(_ string, s float64) { r.mu.Lock(); r.lag = append(r.lag, s); r.mu.Unlock() },
	}
}

const (
	subSubject = "conformance.widget.reverted.v1"
	subDurable = "conformance_peer__conformance__widget__reverted__v1"
)

type consumerFixture struct {
	c    *Consumer
	pub  *fakePublisher
	cur  *fakeCursors
	log  *opLog
	rec  *consumerRecorder
	logs *syncBuffer
}

func newConsumerFixture(t *testing.T, sub Subscription) *consumerFixture {
	t.Helper()
	sub.Subject, sub.AggregateType, sub.TransactionDocument = subSubject, "conformance.widget.widget", true
	if sub.Backoff == nil {
		sub.Backoff = []time.Duration{200 * time.Millisecond, 500 * time.Millisecond, time.Second}
	}
	if sub.MaxDeliver == 0 {
		sub.MaxDeliver = 3
	}
	l, logs := testLogger()
	f := &consumerFixture{pub: &fakePublisher{}, log: &opLog{}, rec: &consumerRecorder{}, logs: logs}
	f.cur = &fakeCursors{v: map[cursorKey]int64{}, log: f.log}
	f.c = &Consumer{ComponentID: "conformance/peer", Subscription: sub, Publisher: f.pub, Logger: l,
		Metrics: f.rec.metrics()}
	return f
}

func (f *consumerFixture) runner(t *testing.T) *consumerRunner {
	t.Helper()
	r, err := f.c.newRunner(f.cur)
	require.NoError(t, err)
	return r
}

// deliver runs one delivery through the runner and returns its settlement.
func (f *consumerFixture) deliver(t *testing.T, in Inbound) *fakeSettle {
	t.Helper()
	s := &fakeSettle{log: f.log}
	in.Settle = s
	f.runner(t).handle(context.Background(), in)
	return s
}

// inbound builds a valid delivery of version v of aggregate w-1.
func inbound(t *testing.T, v int64, delivery uint64) Inbound {
	t.Helper()
	id := envelope.NewID()
	payload, _ := json.Marshal(map[string]any{"widget_id": "w-1", "legal_entity_id": "LE01", "number": "N1",
		"status": "DRAFT", "reason": "RESERVATION_EXPIRED", "version": v})
	h, err := envelope.Headers(envelope.Producer{ComponentID: "conformance/widget", Version: "2.0.3",
		EventsFile: "widget.events.json"}, envelope.Row{ID: id.String(), Subject: subSubject,
		AggregateType: "conformance.widget.widget", AggregateID: "w-1", AggregateVersion: v,
		OccurredAt: time.Now().Add(-2 * time.Second), Payload: payload}, true)
	require.NoError(t, err)
	return Inbound{Subject: subSubject, Header: h, Data: payload, NumDelivered: delivery, StreamSeq: 40 + uint64(v)}
}
