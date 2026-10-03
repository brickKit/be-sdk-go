package events

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/stretchr/testify/require"
)

func TestIntegrationOutboxCommittedRowsArePublishedOnce(t *testing.T) {
	e := newEnv(t)
	_, err := EnsureTopology(within(t, 10*time.Second), e.bus, producerID, []string{e.subject}, nil)
	require.NoError(t, err)
	ids := e.write(t, false, e.event("w-1", 1, "DRAFT"), e.event("w-2", 1, "DRAFT"))
	e.write(t, true, e.event("w-3", 1, "DRAFT"))
	var n int
	e.scan(t, `SELECT count(*) FROM SCHEMA.besdk_outbox`, &n)
	require.Equal(t, 2, n, "the rolled-back transaction left no row")

	stop := runFor(t, (&Pump{Store: e.store, Bus: e.bus, ComponentID: producerID}).Run)
	require.True(t, waitFor(func() bool { return len(e.streamMsgs(t)) == 2 }, 10*time.Second))
	var published int
	require.True(t, waitFor(func() bool {
		e.scan(t, `SELECT count(*) FROM SCHEMA.besdk_outbox WHERE status = 'PUBLISHED' AND published_at IS NOT NULL
			AND claimed_until IS NULL`, &published)
		return published == 2
	}, 5*time.Second))
	stop()

	msgs := e.streamMsgs(t)
	require.Len(t, msgs, 2)
	for i, m := range msgs {
		h := m.Header
		require.Equal(t, ids[i], h.Get("Nats-Msg-Id"))
		require.Equal(t, ids[i], h.Get("ce-id"))
		require.Equal(t, "1.0", h.Get("ce-specversion"))
		require.Equal(t, producerID, h.Get("ce-source"))
		require.Equal(t, e.subject, h.Get("ce-type"))
		require.Equal(t, e.seg+".widget", h.Get("ce-aggregatetype"))
		require.Equal(t, "1", h.Get("ce-aggregateversion"))
		require.Equal(t, "0", h.Get("ce-hopcount"))
		require.Equal(t, "application/json", h.Get("content-type"))
		require.Equal(t, producerID+"@2.0.3/contracts/events/widget.events.json#"+e.subject, h.Get("ce-dataschema"))
		require.Empty(t, h.Get("ce-causationid"))
		require.JSONEq(t, `{"status":"DRAFT"}`, string(m.Data))
	}
	require.Equal(t, "w-1", msgs[0].Header.Get("ce-subject"))
}

func TestIntegrationOutboxSurvivesABusOutage(t *testing.T) {
	e := newEnv(t)
	_, err := EnsureTopology(within(t, 10*time.Second), e.bus, producerID, []string{e.subject}, nil)
	require.NoError(t, err)
	ids := e.write(t, false, e.event("w-1", 1, "DRAFT"))

	down, err := jetstream.Connect(jetstream.Options{URL: "nats://127.0.0.1:1", Name: "test/down"})
	require.NoError(t, err)
	t.Cleanup(down.Close)
	var pending atomic.Int64
	pending.Store(-1)
	log, logs := testLogger()
	stop := runFor(t, (&Pump{Store: e.store, Bus: down, ComponentID: producerID, Logger: log,
		Metrics: PumpMetrics{Pending: func(n int64) { pending.Store(n) }}}).Run)
	var attempts int
	require.True(t, waitFor(func() bool {
		e.scan(t, `SELECT attempts FROM SCHEMA.besdk_outbox`, &attempts)
		return attempts >= 2
	}, 10*time.Second))
	stop()
	var status, lastError string
	var later bool
	e.scan(t, `SELECT status, last_error, next_attempt_at > now() - interval '1 second' FROM SCHEMA.besdk_outbox`,
		&status, &lastError, &later)
	require.Equal(t, "PENDING", status)
	require.NotEmpty(t, lastError)
	require.True(t, later, "next_attempt_at moved forward")
	require.Equal(t, int64(1), pending.Load())
	require.Contains(t, logs.String(), "event bus unavailable")
	require.Empty(t, e.streamMsgs(t))

	pub := &countingPublisher{Publisher: e.bus}
	stop = runFor(t, (&Pump{Store: e.store, Bus: pub, ComponentID: producerID}).Run)
	require.True(t, waitFor(func() bool { return pub.count() == 1 }, 10*time.Second))
	// A claim that expired after publishing (crash before marking) publishes the row again:
	// the duplicate window drops the copy.
	e.exec(t, `UPDATE SCHEMA.besdk_outbox SET status = 'PENDING', next_attempt_at = now()`)
	require.True(t, waitFor(func() bool { return pub.count() == 2 }, 10*time.Second))
	stop()
	msgs := e.streamMsgs(t)
	require.Len(t, msgs, 1, "no loss, no duplicate")
	require.Equal(t, ids[0], msgs[0].Header.Get("ce-id"))
}

func TestIntegrationTwoPumpsPublishEachRowOnce(t *testing.T) {
	e := newEnv(t)
	_, err := EnsureTopology(within(t, 10*time.Second), e.bus, producerID, []string{e.subject}, nil)
	require.NoError(t, err)
	const rows = 600
	for b := 0; b < rows/100; b++ {
		evs := make([]Outgoing, 100)
		for i := range evs {
			evs[i] = e.event("w", int64(b*100+i+1), "DRAFT")
		}
		e.write(t, false, evs...)
	}
	p1, p2 := &countingPublisher{Publisher: e.bus}, &countingPublisher{Publisher: e.bus}
	runFor(t, (&Pump{Store: e.store, Bus: p1, ComponentID: producerID}).Run)
	runFor(t, (&Pump{Store: newStore(t, e.id), Bus: p2, ComponentID: producerID}).Run)
	var published int
	require.True(t, waitFor(func() bool {
		e.scan(t, `SELECT count(*) FROM SCHEMA.besdk_outbox WHERE status = 'PUBLISHED'`, &published)
		return published == rows
	}, 20*time.Second))
	require.Equal(t, rows, p1.count()+p2.count(), "each row published exactly once")
	require.Positive(t, p1.count())
	require.Positive(t, p2.count())
	require.Len(t, e.streamMsgs(t), rows)
}

func TestIntegrationWriteRefusesAnUndeclaredSubject(t *testing.T) {
	e := newEnv(t)
	ev := e.event("w-1", 1, "DRAFT")
	ev.Subject = e.seg + ".widget.other.v1"
	err := e.store.Run(within(t, 5*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		_, err := Write(ctx, tx, e.producer(), ev)
		return err
	})
	requireInternal(t, err, "not declared")
}
