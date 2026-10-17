//go:build integration

package pgqueue

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
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
