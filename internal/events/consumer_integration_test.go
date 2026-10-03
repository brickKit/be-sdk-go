package events

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/stretchr/testify/require"
)

const consumerID = "conformance/peer"

func (e *env) subscribe(t *testing.T, sub Subscription, rec *consumerRecorder) func() {
	t.Helper()
	sub.Subject, sub.AggregateType = e.subject, e.seg+".widget"
	_, err := EnsureTopology(within(t, 10*time.Second), e.bus, consumerID, nil, []string{e.subject})
	require.NoError(t, err)
	log, _ := testLogger()
	c := &Consumer{Bus: JetStream{Bus: e.bus}, Store: e.store, ComponentID: consumerID, Subscription: sub,
		Publisher: e.bus, Logger: log, Metrics: rec.metrics()}
	return runFor(t, c.Run)
}

func projectApply(calls *atomic.Int32) func(context.Context, *pg.Tx, envelope.Event) error {
	return func(ctx context.Context, tx *pg.Tx, ev envelope.Event) error {
		calls.Add(1)
		var p struct{ Status string }
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return Permanent(err)
		}
		status := p.Status
		_, err := tx.ExecContext(ctx, `INSERT INTO projection (aggregate_id, version, status) VALUES ($1, $2, $3)
			ON CONFLICT (aggregate_id) DO UPDATE SET version = EXCLUDED.version, status = EXCLUDED.status`,
			ev.AggregateID, ev.Version, status)
		return err
	}
}

func TestIntegrationPublishConsumeApplyAndSkipStale(t *testing.T) {
	e := newEnv(t)
	_, err := EnsureTopology(within(t, 10*time.Second), e.bus, producerID, []string{e.subject}, nil)
	require.NoError(t, err)
	var calls atomic.Int32
	rec := &consumerRecorder{}
	e.subscribe(t, Subscription{Apply: projectApply(&calls)}, rec)
	runFor(t, (&Pump{Store: e.store, Bus: e.bus, ComponentID: producerID}).Run)

	e.write(t, false, e.event("w-1", 1, "DRAFT"))
	e.write(t, false, e.event("w-1", 2, "APPROVED"))
	var version int64
	var status string
	require.True(t, waitFor(func() bool {
		var n int
		e.scan(t, `SELECT count(*) FROM SCHEMA.besdk_event_cursor WHERE version = 2`, &n)
		return n == 1
	}, 15*time.Second))
	e.scan(t, `SELECT version, status FROM SCHEMA.projection WHERE aggregate_id = 'w-1'`, &version, &status)
	require.Equal(t, int64(2), version)
	require.Equal(t, "APPROVED", status)
	var consumer, aggType, eventID string
	e.scan(t, `SELECT consumer, aggregate_type, event_id FROM SCHEMA.besdk_event_cursor`, &consumer, &aggType, &eventID)
	require.Equal(t, "", consumer)
	require.Equal(t, e.seg+".widget", aggType)
	before := calls.Load()

	// A shuffled redelivery of an older version (republished with a new id) is skipped.
	e.write(t, false, e.event("w-1", 1, "DRAFT"))
	require.True(t, waitFor(func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.handled[e.subject+" skipped"] >= 1
	}, 15*time.Second))
	require.Equal(t, before, calls.Load(), "Apply never called for a stale version")
	e.scan(t, `SELECT version, status FROM SCHEMA.projection WHERE aggregate_id = 'w-1'`, &version, &status)
	require.Equal(t, int64(2), version)
	require.Equal(t, "APPROVED", status)
}

func TestIntegrationHandlerFailingMaxDeliverEndsInTheDeadLetters(t *testing.T) {
	e := newEnv(t)
	_, err := EnsureTopology(within(t, 10*time.Second), e.bus, producerID, []string{e.subject}, nil)
	require.NoError(t, err)
	var calls atomic.Int32
	rec := &consumerRecorder{}
	e.subscribe(t, Subscription{MaxDeliver: 3, Backoff: []time.Duration{200 * time.Millisecond},
		Apply: func(context.Context, *pg.Tx, envelope.Event) error { calls.Add(1); return errors.New("always fails") }}, rec)
	runFor(t, (&Pump{Store: e.store, Bus: e.bus, ComponentID: producerID}).Run)
	ids := e.write(t, false, e.event("w-9", 1, "DRAFT"))

	durable, dlqSubject, err := envelope.Durable(consumerID, e.subject)
	require.NoError(t, err)
	dlq, err := e.js.Stream(within(t, 5*time.Second), envelope.DLQStream)
	require.NoError(t, err)
	require.True(t, waitFor(func() bool {
		_, err := dlq.GetLastMsgForSubject(context.Background(), dlqSubject)
		return err == nil
	}, 15*time.Second))
	m, err := dlq.GetLastMsgForSubject(context.Background(), dlqSubject)
	require.NoError(t, err)
	require.Equal(t, envelope.ReasonMaxDeliver, m.Header.Get("be-dlq-reason"))
	require.Equal(t, durable, m.Header.Get("be-dlq-consumer"))
	require.Equal(t, "4", m.Header.Get("be-dlq-delivery"))
	require.Equal(t, ids[0], m.Header.Get("ce-id"))
	require.Equal(t, e.subject, m.Header.Get("ce-type"))
	require.Equal(t, "dlq:"+durable+":1", m.Header.Get("Nats-Msg-Id"))
	require.Equal(t, int32(3), calls.Load(), "three deliveries ran the handler; the fourth was dead-lettered on receipt")

	time.Sleep(2 * time.Second)
	require.Equal(t, int32(3), calls.Load(), "terminated: not delivered again")
	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, 1, rec.dlq)
	require.Equal(t, 3, rec.handled[e.subject+" nak"])
}
