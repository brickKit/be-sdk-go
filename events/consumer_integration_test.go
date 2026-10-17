//go:build integration

package events

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func delivery(attempt int, extra map[string]string, data string) *Delivery {
	h := map[string]string{HeaderID: ids.New().String(), HeaderType: "demo.stock.moved.v1", HeaderSubject: "r1",
		HeaderVersion: "1", HeaderSeq: "1", HeaderPartition: "0", HeaderHops: "0", HeaderProjection: "full"}
	for k, v := range extra {
		h[k] = v
	}
	return &Delivery{Message: Message{Subject: "be.demo.stock.0.demo.stock.moved.v1", Headers: h, Data: []byte(data)}, Attempt: attempt}
}

func count(t *testing.T, d *db.DB, sql string) int {
	var n int
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, sql).Scan(&n)
	}))
	return n
}

func TestARedeliveryTakesEffectOnce(t *testing.T) { // P6.25, CK-EV-08
	d, _ := source(t, "evcons1")
	c := &Consumer{Member: "demo/evcons1", DB: d, Log: quietLog(), Sub: Sub{Name: "moves", Streams: []string{"demo.stock"},
		Handle: func(ctx context.Context, tx db.Tx, e Received) error {
			_, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1)", ids.New())
			return err
		}}}
	del := delivery(1, nil, `{}`)
	assert.Equal(t, Ack, c.Handle(context.Background(), del))
	del.Attempt = 2 // the ack was lost; the transport delivers again
	assert.Equal(t, Ack, c.Handle(context.Background(), del))
	assert.Equal(t, 1, count(t, d, "SELECT count(*) FROM items"))
}

func TestTheLastFailureBecomesOneDeadLetter(t *testing.T) { // P6.20
	d, _ := source(t, "evcons2")
	c := &Consumer{Member: "demo/evcons2", DB: d, Log: quietLog(), Sub: Sub{Name: "moves", Streams: []string{"demo.stock"},
		Handle: func(ctx context.Context, tx db.Tx, e Received) error {
			_, _ = tx.Exec(ctx, "INSERT INTO items VALUES ($1)", ids.New()) // rolled back with the failure
			return errors.New("cannot apply")
		}}}
	del := delivery(1, nil, `{"x":1}`)
	assert.Equal(t, Nak, c.Handle(context.Background(), del))
	del.Attempt = 2
	assert.Equal(t, Nak, c.Handle(context.Background(), del))
	del.Attempt = 3
	assert.Equal(t, Term, c.Handle(context.Background(), del))
	assert.Equal(t, Term, c.Handle(context.Background(), del), "a repeated last delivery")
	assert.Equal(t, 1, count(t, d, "SELECT count(*) FROM be_dead_letter WHERE error LIKE '%cannot apply%' AND attempts = 3"))
	assert.Equal(t, 0, count(t, d, "SELECT count(*) FROM items"))
	assert.Equal(t, 0, count(t, d, "SELECT count(*) FROM be_inbox"), "a failed event is not marked as applied")
}

func TestTooManyHopsIsAFailure(t *testing.T) { // P6.15
	d, _ := source(t, "evcons3")
	called := false
	c := &Consumer{Member: "demo/evcons3", DB: d, Log: quietLog(), Sub: Sub{Name: "moves", Streams: []string{"demo.stock"},
		Handle: func(context.Context, db.Tx, Received) error { called = true; return nil }}}
	assert.Equal(t, Nak, c.Handle(context.Background(), delivery(1, map[string]string{HeaderHops: "11"}, `{}`)))
	assert.False(t, called)
}

// objects is an in-memory object store for claim checks.
type objects struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (o *objects) Put(_ context.Context, id string, data []byte) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	uri := "mem://" + id
	o.m[uri] = append([]byte(nil), data...)
	return uri, nil
}

func (o *objects) Get(_ context.Context, uri string) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.m[uri], nil
}

func TestLargePayloadsTravelByClaimCheck(t *testing.T) { // P6.14, CK-EV-06
	d, _ := source(t, "evcons4")
	store := &objects{m: map[string][]byte{}}
	single := Stream{Name: "demo.stock", Partitions: 1}
	p, err := NewProducer("demo/evcons4", []Stream{single}, store)
	require.NoError(t, err)
	big := strings.Repeat("x", MaxPayload+10)
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := p.Emit(ctx, tx, "demo.stock", Event{Type: "demo.stock.moved.v1", RecordID: "r1", Version: 1, Projection: Full,
			Data: map[string]string{"blob": big}})
		return err
	}))
	tr := NewMemory(0)
	f := &Forwarder{DB: d, Producer: p, Transport: tr, Source: "demo/evcons4", Impl: ids.New().String(), Log: quietLog()}
	n, err := f.Once(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)

	var got Received
	c := &Consumer{Member: "demo/evcons4", DB: d, Claims: store, Log: quietLog(), Sub: Sub{Name: "moves", Streams: []string{"demo.stock"},
		Handle: func(_ context.Context, _ db.Tx, e Received) error { got = e; return nil }}}
	done := make(chan Outcome, 2)
	sub, err := tr.Subscribe(context.Background(), c.Spec(), func(ctx context.Context, del *Delivery) Outcome {
		assert.Equal(t, "true", del.Headers[HeaderClaim])
		assert.Less(t, len(del.Data), 1000, "the message carries the reference only")
		out := c.Handle(ctx, del)
		done <- out
		return out
	})
	require.NoError(t, err)
	defer sub.Stop()
	select {
	case out := <-done:
		assert.Equal(t, Ack, out)
	case <-time.After(5 * time.Second):
		t.Fatal("not delivered")
	}
	var payload map[string]string
	require.NoError(t, got.Decode(&payload))
	assert.Equal(t, big, payload["blob"])

	// an object that does not match the reference's size and digest is refused
	uri, _ := store.Put(context.Background(), "other", []byte("tampered"))
	_, err = ResolveClaim(context.Background(), Message{Headers: map[string]string{HeaderClaim: "true", HeaderID: "x"},
		Data: []byte(`{"claim":{"uri":"` + uri + `","size":8,"sha256":"` + strings.Repeat("0", 64) + `"}}`)}, store)
	assert.ErrorContains(t, err, "does not match its size and digest")
}
