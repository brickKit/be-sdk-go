//go:build integration

package pgqueue

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/transporttest"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var migrateOnce sync.Once

// busURL is the test bus: schema be_bus_test in the test database, as the superuser of BE_SDK_TEST_PG_ADMIN.
func busURL(t transporttest.T) string {
	u, err := url.Parse(os.Getenv("BE_SDK_TEST_PG_ADMIN"))
	if err != nil || u.Host == "" {
		t.Errorf("BE_SDK_TEST_PG_ADMIN is not set: the queue tests need PostgreSQL")
		t.FailNow()
	}
	u.Scheme, u.Path = "pgqueue", "/"+pgtest.Database
	q := u.Query()
	q.Set("schema", "be_bus_test")
	u.RawQuery = q.Encode()
	return u.String()
}

func factory(t transporttest.T) (events.Transport, string) {
	bus := busURL(t)
	var merr error
	migrateOnce.Do(func() {
		tb := t.(testing.TB)
		pgtest.EnsureDatabase(tb, pgtest.Database)
		pgtest.Exec(tb, pgtest.Admin(tb, pgtest.Database), "CREATE SCHEMA IF NOT EXISTS be_bus_test")
		merr = Migrate(context.Background(), bus, slog.New(slog.NewTextHandler(io.Discard, nil)))
	})
	if merr != nil {
		t.Errorf("%v", merr)
		t.FailNow()
	}
	tr, err := Connect(context.Background(), bus, "be-sdk-go-test", 8)
	if err != nil {
		t.Errorf("%v", err)
		t.FailNow()
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr, "sdktest.s" + ids.New().String()[24:]
}

func TestPostgresQueuePassesTheSuite(t *testing.T) {
	factory(t) // migrate before the parallel cases start
	transporttest.Run(t, factory)
}

func TestAnInFlightRowWithASmallerSequenceIsNotSkipped(t *testing.T) { // P6.23, CK-BUS-08
	tr, stream := factory(t)
	q := tr.(*Transport)
	ctx := context.Background()
	got := make(chan string, 4)
	sub, err := q.Subscribe(ctx, events.SubscriptionSpec{Durable: events.DurableName("test/"+stream, "barrier"),
		Streams: []string{stream}}, func(_ context.Context, d *events.Delivery) events.Outcome {
		got <- string(d.Data)
		return events.Ack
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()
	insert := "INSERT INTO " + q.t("be_bus_message") + " (msg_id, subject, headers, data) VALUES ($1, $2, '{}', $3)"
	subject := "be." + stream + ".0.demo.x.v1"

	early, err := q.pool.Begin(ctx) // takes the smaller transaction ID first
	if err != nil {
		t.Fatal(err)
	}
	if _, err := early.Exec(ctx, "SELECT pg_current_xact_id()"); err != nil {
		t.Fatal(err)
	}
	late, err := q.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := late.Exec(ctx, insert, ids.New().String(), subject, []byte("smaller sequence, still in flight")); err != nil {
		t.Fatal(err)
	}
	if _, err := early.Exec(ctx, insert, ids.New().String(), subject, []byte("larger sequence, committed first")); err != nil {
		t.Fatal(err)
	}
	if err := early.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first := <-got
	if first != "larger sequence, committed first" {
		t.Fatalf("got %q first", first)
	}
	if err := late.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case second := <-got:
		if second != "smaller sequence, still in flight" {
			t.Fatalf("got %q", second)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the row that committed later with a smaller sequence was skipped")
	}
}

func TestPurgeDropsMessagesPastTheRetention(t *testing.T) { // P6.27, CK-BUS-09
	tr, stream := factory(t)
	ctx := context.Background()
	require.NoError(t, tr.EnsureStream(ctx, events.Stream{Name: stream, Partitions: 1}))
	// a subscription that has read nothing yet: before P6.27 it held every message back
	sub, err := tr.Subscribe(ctx, events.SubscriptionSpec{Durable: "purge_" + stream[9:], Streams: []string{stream}},
		func(context.Context, *events.Delivery) events.Outcome { return events.Nak })
	require.NoError(t, err)
	require.NoError(t, sub.Stop())
	subject := "be." + stream + ".0.x.v1"
	for range 2 {
		require.NoError(t, tr.Publish(ctx, events.Message{Subject: subject,
			Headers: map[string]string{events.HeaderID: ids.New().String()}, Data: []byte("{}")}))
	}
	admin := pgtest.Admin(t, pgtest.Database)
	var oldest, newest string
	require.NoError(t, admin.QueryRow(ctx, `SELECT min(msg_id), max(msg_id) FROM be_bus_test.be_bus_message
		WHERE subject = $1`, subject).Scan(&oldest, &newest))
	pgtest.Exec(t, admin, "UPDATE be_bus_test.be_bus_message SET published_at = now() - interval '8 days' WHERE msg_id = '"+oldest+"'")
	n, err := tr.(*Transport).Purge(ctx, events.TransportRetention)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n, "the subscription never read it, yet past the retention it goes")
	var left []string
	rows, err := admin.Query(ctx, "SELECT msg_id FROM be_bus_test.be_bus_message WHERE subject = $1", subject)
	require.NoError(t, err)
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		left = append(left, id)
	}
	assert.Equal(t, []string{newest}, left)
}

// backlog gives a new subscription n due deliveries of real messages, written directly (publishing 20 000 one by
// one would take most of the test).
func backlog(t *testing.T, n int) events.SubscriptionSpec {
	ctx := context.Background()
	s := events.SubscriptionSpec{Durable: "backlog_" + ids.New().String()[24:], MaxDeliver: 3, AckWait: 30 * time.Second}.Defaults()
	admin := pgtest.Admin(t, pgtest.Database)
	pgtest.Exec(t, admin, `INSERT INTO be_bus_test.be_bus_subscription (durable, streams, filters, max_deliver, ack_wait_ms,
		backoff_ms) VALUES ('`+s.Durable+`', '{x}', '{}', 3, 30000, '{1000}')`)
	_, err := admin.Exec(ctx, `WITH m AS (INSERT INTO be_bus_test.be_bus_message (subject, headers, data)
			SELECT 'be.x.0.t.v1', '{}'::jsonb, '\x7b7d'::bytea FROM generate_series(1, $2) RETURNING seq)
		INSERT INTO be_bus_test.be_bus_delivery (durable, seq) SELECT $1, seq FROM m`, s.Durable, n)
	require.NoError(t, err)
	t.Cleanup(func() { forget(t, s.Durable, "be.x.0.t.v1") })
	return s
}

// forget removes what a test wrote straight into the shared test bus: every later subscription would read past it.
func forget(t *testing.T, durable, subject string) {
	admin := pgtest.Admin(t, pgtest.Database)
	pgtest.Exec(t, admin, "DELETE FROM be_bus_test.be_bus_subscription WHERE durable = '"+durable+"'")
	pgtest.Exec(t, admin, "DELETE FROM be_bus_test.be_bus_message WHERE subject = '"+subject+"'")
}

func TestASubscriptionCrossesOtherStreamsWithoutWaiting(t *testing.T) { // P6.23
	// the queue is shared by every stream: a subscription reads past every message to keep its position, at most a
	// batch per round; waiting a poll after each round that delivered nothing capped that at batch/poll a second
	tr, stream := factory(t)
	ctx := context.Background()
	admin := pgtest.Admin(t, pgtest.Database)
	other := "be.other" + stream[9:] + ".0.t.v1"
	_, err := admin.Exec(ctx, `INSERT INTO be_bus_test.be_bus_message (subject, headers, data)
		SELECT $1, '{}'::jsonb, '\x7b7d'::bytea FROM generate_series(1, 20000)`, other)
	require.NoError(t, err)
	durable := "cross_" + stream[9:]
	t.Cleanup(func() { forget(t, durable, other) })
	require.NoError(t, tr.EnsureStream(ctx, events.Stream{Name: stream, Partitions: 1}))
	got := make(chan struct{}, 1)
	started := time.Now()
	sub, err := tr.Subscribe(ctx, events.SubscriptionSpec{Durable: durable, Streams: []string{stream}},
		func(context.Context, *events.Delivery) events.Outcome { got <- struct{}{}; return events.Ack })
	require.NoError(t, err)
	defer sub.Stop()
	require.NoError(t, tr.Publish(ctx, events.Message{Subject: "be." + stream + ".0.x.v1",
		Headers: map[string]string{events.HeaderID: ids.New().String()}, Data: []byte("{}")}))
	select {
	case <-got:
		assert.Less(t, time.Since(started), 2*time.Second, "past 20 000 messages of another stream")
	case <-time.After(10 * time.Second):
		t.Fatal("not delivered within 10 s: the subscription crawls past the other stream's messages")
	}
}

func medianClaim(t *testing.T, tr *Transport, s events.SubscriptionSpec) time.Duration {
	var took []time.Duration
	for range 15 {
		started := time.Now()
		d, _, err := tr.claim(context.Background(), s)
		took = append(took, time.Since(started))
		require.NoError(t, err)
		require.NotNil(t, d)
	}
	slices.Sort(took)
	return took[len(took)/2]
}

func TestABacklogDoesNotSlowEachClaim(t *testing.T) { // P6.23
	// every claim used to scan the subscription's whole backlog first: the more behind, the slower each message,
	// so a subscriber that once fell behind could not catch up
	tr, _ := factory(t)
	small := medianClaim(t, tr.(*Transport), backlog(t, 20))
	large := medianClaim(t, tr.(*Transport), backlog(t, 20000))
	assert.Less(t, large, 3*small+2*time.Millisecond, "claims with 20 000 waiting (%v) against 20 waiting (%v)", large, small)
}

func TestAClaimWhoseMessageWasPurgedIsDropped(t *testing.T) { // P6.27
	tr, _ := factory(t)
	s := backlog(t, 2)
	admin := pgtest.Admin(t, pgtest.Database)
	var first int64
	require.NoError(t, admin.QueryRow(context.Background(), `SELECT min(seq) FROM be_bus_test.be_bus_delivery
		WHERE durable = $1`, s.Durable).Scan(&first))
	pgtest.Exec(t, admin, "DELETE FROM be_bus_test.be_bus_message WHERE seq = "+strconv.FormatInt(first, 10))
	var got *events.Delivery
	for range 3 { // the orphan is dropped on the way; the real message follows
		d, _, err := tr.(*Transport).claim(context.Background(), s)
		require.NoError(t, err)
		if d != nil {
			got = d
			break
		}
	}
	require.NotNil(t, got, "the delivery with a message comes through")
	var orphans int
	require.NoError(t, admin.QueryRow(context.Background(), `SELECT count(*) FROM be_bus_test.be_bus_delivery
		WHERE durable = $1 AND seq = $2`, s.Durable, first).Scan(&orphans))
	assert.Zero(t, orphans, "the delivery of the purged message is gone")
}

func TestAPublishWakesAnIdleReaderWithoutWaitingForThePoll(t *testing.T) { // P6.30
	tr, stream := factory(t)
	tr.(*Transport).poll = time.Hour // only the wake can pick the message up
	ctx := context.Background()
	require.NoError(t, tr.EnsureStream(ctx, events.Stream{Name: stream, Partitions: 1}))
	durable := "wake_" + stream[9:]
	t.Cleanup(func() { forget(t, durable, "be."+stream+".0.x.v1") })
	got := make(chan time.Time, 4)
	sub, err := tr.Subscribe(ctx, events.SubscriptionSpec{Durable: durable, Streams: []string{stream}},
		func(context.Context, *events.Delivery) events.Outcome { got <- time.Now(); return events.Ack })
	require.NoError(t, err)
	defer sub.Stop()
	time.Sleep(500 * time.Millisecond) // listening, and the reader has read past the bus and is idle
	for i := range 2 {
		published := time.Now()
		require.NoError(t, tr.Publish(ctx, events.Message{Subject: "be." + stream + ".0.x.v1",
			Headers: map[string]string{events.HeaderID: ids.New().String()}, Data: []byte("{}")}))
		select {
		case at := <-got:
			assert.Less(t, at.Sub(published), 2*time.Second, "message %d: delivered on the wake", i)
		case <-time.After(10 * time.Second):
			t.Fatalf("message %d was not delivered without a poll", i)
		}
	}
}
