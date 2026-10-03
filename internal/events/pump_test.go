package events

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/stretchr/testify/require"
)

const widgetSchema = "conformance/widget@2.0.3/contracts/events/widget.events.json#conformance.widget.reverted.v1"

func pendingRow(t *testing.T, version int64) claimedRow {
	id := envelope.NewID()
	return claimedRow{outboxRow: outboxRow{id: id.String(), createdAt: envelope.IDTime(id),
		subject: "conformance.widget.reverted.v1", aggregateType: "conformance.widget.widget", aggregateID: "w-1",
		aggregateVersion: version, occurredAt: fixedNow,
		traceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		headers:     []byte(`{"ce-dataschema":"` + widgetSchema + `","ce-legalentity":"LE01"}`),
		payload:     []byte(`{"legal_entity_id":"LE01","version":` + fmt.Sprint(version) + `}`)}}
}

type pumpRecorder struct {
	mu        sync.Mutex
	published map[string]int
	pending   int64
	oldest    float64
}

func (r *pumpRecorder) metrics() PumpMetrics {
	r.published = map[string]int{}
	return PumpMetrics{
		Published: func(s string) { r.mu.Lock(); r.published[s]++; r.mu.Unlock() },
		Pending:   func(n int64) { r.mu.Lock(); r.pending = n; r.mu.Unlock() },
		OldestAge: func(s float64) { r.mu.Lock(); r.oldest = s; r.mu.Unlock() },
	}
}

func newTestPump(pub Publisher) (*Pump, *syncBuffer, *pumpRecorder) {
	log, buf := testLogger()
	rec := &pumpRecorder{}
	return &Pump{Bus: pub, ComponentID: "conformance/widget", Logger: log, Metrics: rec.metrics()}, buf, rec
}

func TestPumpRoundPublishesWithEnvelopeHeaders(t *testing.T) {
	r1, r2 := pendingRow(t, 1), pendingRow(t, 2)
	ob := newFakeOutbox(r1, r2)
	pub := &fakePublisher{}
	p, _, rec := newTestPump(pub)
	n, err := p.newRunner(ob).round(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, n)
	msgs := pub.messages()
	require.Len(t, msgs, 2)
	m := msgs[0]
	require.Equal(t, r1.id, m.ID)
	require.Equal(t, "conformance.widget.reverted.v1", m.Subject)
	require.Equal(t, r1.payload, m.Data)
	require.Equal(t, map[string]string{
		"ce-specversion": "1.0", "ce-id": r1.id, "ce-source": "conformance/widget",
		"ce-type": "conformance.widget.reverted.v1", "ce-time": "2026-10-03T08:30:00.123456Z",
		"ce-subject": "w-1", "content-type": "application/json", "ce-dataschema": widgetSchema,
		"ce-aggregatetype": "conformance.widget.widget", "ce-aggregateversion": "1", "ce-hopcount": "0",
		"ce-legalentity": "LE01", "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}, m.Header)
	require.Equal(t, "PUBLISHED", ob.statusOf(r1.id))
	require.Equal(t, "PUBLISHED", ob.statusOf(r2.id))
	require.Equal(t, 2, rec.published["conformance.widget.reverted.v1"])
}

func TestPumpRoundCarriesCausation(t *testing.T) {
	r := pendingRow(t, 1)
	r.causationID = "01920000-0000-7000-8000-000000000001"
	r.hopCount = 4
	pub := &fakePublisher{}
	p, _, _ := newTestPump(pub)
	_, err := p.newRunner(newFakeOutbox(r)).round(context.Background())
	require.NoError(t, err)
	h := pub.messages()[0].Header
	require.Equal(t, "01920000-0000-7000-8000-000000000001", h["ce-causationid"])
	require.Equal(t, "4", h["ce-hopcount"])
}

func TestPumpFailedPublishGoesBackWithBackoff(t *testing.T) {
	r1, r2 := pendingRow(t, 1), pendingRow(t, 2)
	ob := newFakeOutbox(r1, r2)
	pub := &fakePublisher{fail: func(m jetstream.Message) error {
		if m.ID == r2.id {
			return errors.New("nats: maximum payload exceeded " + strings.Repeat("x", 500))
		}
		return nil
	}}
	p, buf, _ := newTestPump(pub)
	run := p.newRunner(ob)
	_, err := run.round(context.Background())
	require.NoError(t, err)
	require.Equal(t, "PUBLISHED", ob.statusOf(r1.id))
	f, ok := ob.failure(r2.id)
	require.True(t, ok)
	require.Equal(t, time.Second, f.delay)
	require.LessOrEqual(t, len(f.err), maxLastError)
	require.Contains(t, f.err, "maximum payload")
	require.Contains(t, buf.String(), r2.id)

	ob.reopen()
	_, err = run.round(context.Background())
	require.NoError(t, err)
	f, _ = ob.failure(r2.id)
	require.Equal(t, 2*time.Second, f.delay)
}

func TestPumpBusOutageLogsOncePerOutage(t *testing.T) {
	rows := []claimedRow{pendingRow(t, 1), pendingRow(t, 2), pendingRow(t, 3)}
	ob := newFakeOutbox(rows...)
	down := fmt.Errorf("jetstream: not connected: %w", jetstream.ErrUnavailable)
	pub := &fakePublisher{fail: func(jetstream.Message) error { return down }}
	p, buf, _ := newTestPump(pub)
	run := p.newRunner(ob)
	for i := 0; i < 3; i++ {
		_, err := run.round(context.Background())
		require.NoError(t, err)
		ob.reopen()
	}
	require.Equal(t, 1, strings.Count(buf.String(), "event bus unavailable"))
	for _, r := range rows {
		require.Equal(t, "PENDING", ob.statusOf(r.id))
		require.NotContains(t, buf.String(), r.id) // not logged per row
	}
	pub.setFail(nil)
	_, err := run.round(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(buf.String(), "event bus available again"))
	for _, r := range rows {
		require.Equal(t, "PUBLISHED", ob.statusOf(r.id))
	}
	pub.setFail(func(jetstream.Message) error { return down })
	r4 := pendingRow(t, 4)
	ob2 := newFakeOutbox(r4)
	run.ob = ob2
	_, _ = run.round(context.Background())
	require.Equal(t, 2, strings.Count(buf.String(), "event bus unavailable"))
}

func TestPumpRowWithoutDataSchemaIsKeptAndFailed(t *testing.T) {
	r := pendingRow(t, 1)
	r.headers = []byte(`{}`)
	ob := newFakeOutbox(r)
	pub := &fakePublisher{}
	p, buf, _ := newTestPump(pub)
	_, err := p.newRunner(ob).round(context.Background())
	require.NoError(t, err)
	require.Empty(t, pub.messages())
	f, ok := ob.failure(r.id)
	require.True(t, ok)
	require.Contains(t, f.err, "ce-dataschema")
	require.Contains(t, buf.String(), "level=ERROR")
}

func TestPumpBackoff(t *testing.T) {
	want := []time.Duration{time.Second, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, time.Minute, time.Minute}
	for attempts, w := range want {
		require.Equal(t, w, pumpBackoff(attempts), "attempts %d", attempts)
	}
	require.Equal(t, time.Minute, pumpBackoff(1000))
}

func TestPumpPollingAdapts(t *testing.T) {
	require.Equal(t, time.Duration(0), nextWait(2*time.Second, BatchSize))
	require.Equal(t, BusyPoll, nextWait(2*time.Second, 3))
	require.Equal(t, 400*time.Millisecond, nextWait(BusyPoll, 0))
	require.Equal(t, BusyPoll, nextWait(0, 0))
	require.Equal(t, IdlePoll, nextWait(1600*time.Millisecond, 0))
	require.Equal(t, IdlePoll, nextWait(IdlePoll, 0))
}

func TestPumpRunFinishesTheBatchOnShutdown(t *testing.T) {
	r := pendingRow(t, 1)
	ob := newFakeOutbox(r)
	pub := &fakePublisher{block: make(chan struct{})}
	p, _, _ := newTestPump(pub)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.newRunner(ob).run(ctx) }()
	require.True(t, waitFor(func() bool { return ob.statusOf(r.id) == "SENDING" }, time.Second))
	cancel()
	time.Sleep(20 * time.Millisecond)
	close(pub.block)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
	require.Equal(t, "PUBLISHED", ob.statusOf(r.id))
	require.Len(t, pub.messages(), 1)
}

func TestPumpRunRefreshesGaugesAndSurvivesClaimErrors(t *testing.T) {
	ob := newFakeOutbox()
	ob.claimErr = errors.New("db down")
	p, buf, rec := newTestPump(&fakePublisher{})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	require.NoError(t, p.newRunner(ob).run(ctx))
	require.GreaterOrEqual(t, ob.statsSeen, 1)
	rec.mu.Lock()
	require.Equal(t, 1.5, rec.oldest)
	rec.mu.Unlock()
	require.Contains(t, buf.String(), "db down")
}

func TestPumpRunRequiresItsParts(t *testing.T) {
	require.Error(t, (&Pump{}).Run(context.Background()))
}
