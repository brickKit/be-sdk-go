package events

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

const producerID = "conformance/widget"

// projectionMigration is the consumer's own table, written by the Apply handler.
var projectionMigration = fstest.MapFS{
	"1_projection.up.sql": {Data: []byte(`CREATE TABLE projection (aggregate_id text PRIMARY KEY,
		version bigint NOT NULL, status text NOT NULL)`)},
	"1_projection.down.sql": {Data: []byte(`DROP TABLE projection`)},
}

// env is one integration test's world: a fresh schema with the besdk_* tables, a store, the bus,
// a random first subject segment and a contract on it.
type env struct {
	id       testpg.Identity
	super    *sql.DB // the superuser, for assertions only
	store    *pg.Store
	bus      *jetstream.Bus
	js       natsjs.JetStream
	seg      string
	subject  string // <seg>.widget.changed.v1
	contract *Contract
}

func newEnv(t *testing.T) *env {
	t.Helper()
	url := os.Getenv("TEST_NATS_URL")
	if url == "" || os.Getenv("TEST_PG16_DSN") == "" {
		t.Skip("TEST_PG16_DSN / TEST_NATS_URL not set")
	}
	e := &env{id: testpg.New(t)}
	e.super = testpg.Open(t, e.id.SuperDSN)
	e.super.SetMaxOpenConns(2)
	ctx := within(t, 30*time.Second)
	_, err := pg.MigrateUp(ctx, pg.MigrateConfig{Host: e.id.Host, Port: e.id.Port, Database: e.id.Database,
		SSLMode: "disable", Owner: e.id.Owner, OwnerPassword: e.id.OwnerPassword, Schema: e.id.Schema,
		ComponentID: producerID, Component: projectionMigration})
	require.NoError(t, err)
	e.store = newStore(t, e.id)
	e.bus = connectBus(t, url)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	e.js, err = natsjs.New(nc)
	require.NoError(t, err)
	var r [5]byte
	_, _ = rand.Read(r[:])
	e.seg = "t" + hex.EncodeToString(r[:])
	e.subject = e.seg + ".widget.changed.v1"
	e.contract, err = LoadContract(fstest.MapFS{"contracts/events/widget.events.json": {Data: []byte(fmt.Sprintf(
		`{"events":[{"subject":%q,"x-aggregate-type":%q,"payload":{"type":"object","required":["status"],
		"properties":{"status":{"type":"string"}}}}]}`, e.subject, e.seg+".widget"))}})
	require.NoError(t, err)
	stream := "BE_" + strings.ToUpper(e.seg)
	t.Cleanup(func() {
		ctx := context.Background()
		_ = e.js.DeleteStream(ctx, stream)
		if s, err := e.js.Stream(ctx, envelope.DLQStream); err == nil {
			_ = s.Purge(ctx, natsjs.WithPurgeSubject("dlq.*__"+e.seg+"__>"))
		}
	})
	return e
}

func newStore(t *testing.T, id testpg.Identity) *pg.Store {
	t.Helper()
	p, err := pg.OpenPool(pg.PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User,
		Password: func() string { return id.Password }, SSLMode: "disable", MaxConns: 8})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return pg.NewStore(p, pg.StoreConfig{ComponentID: producerID, Role: id.User, Schema: id.Schema, Budget: 8})
}

func connectBus(t *testing.T, url string) *jetstream.Bus {
	t.Helper()
	b, err := jetstream.Connect(jetstream.Options{URL: url, Name: "test/events"})
	require.NoError(t, err)
	t.Cleanup(b.Close)
	require.True(t, waitFor(b.Connected, 5*time.Second), "bus not connected")
	return b
}

func within(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func (e *env) producer() *Producer {
	return &Producer{ComponentID: producerID, Version: "2.0.3", Contract: e.contract, Publishes: []string{e.subject}}
}

// write adds events in one business transaction; rollback makes the transaction fail afterwards.
func (e *env) write(t *testing.T, rollback bool, evs ...Outgoing) []string {
	t.Helper()
	var ids []string
	err := e.store.Run(within(t, 10*time.Second), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		ids = ids[:0]
		for _, ev := range evs {
			id, err := Write(ctx, tx, e.producer(), ev)
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if rollback {
			return fmt.Errorf("business rule failed")
		}
		return nil
	})
	if rollback {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}
	return ids
}

func (e *env) event(aggregate string, version int64, status string) Outgoing {
	return Outgoing{Subject: e.subject, AggregateID: aggregate, Version: version, Payload: map[string]any{"status": status}}
}

// sql runs a query as the superuser on the test schema and scans one row.
func (e *env) scan(t *testing.T, query string, dest ...any) {
	t.Helper()
	q := strings.ReplaceAll(query, "SCHEMA.", `"`+e.id.Schema+`".`)
	require.NoError(t, e.super.QueryRow(q).Scan(dest...))
}

func (e *env) exec(t *testing.T, query string) {
	t.Helper()
	_, err := e.super.Exec(strings.ReplaceAll(query, "SCHEMA.", `"`+e.id.Schema+`".`))
	require.NoError(t, err)
}

// streamMsgs returns every message of the test's stream.
func (e *env) streamMsgs(t *testing.T) []*natsjs.RawStreamMsg {
	t.Helper()
	ctx := within(t, 5*time.Second)
	s, err := e.js.Stream(ctx, "BE_"+strings.ToUpper(e.seg))
	require.NoError(t, err)
	info, err := s.Info(ctx)
	require.NoError(t, err)
	var out []*natsjs.RawStreamMsg
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err == nil {
			out = append(out, m)
		}
	}
	return out
}

// runFor starts fn in the background and returns a function that cancels it and waits.
func runFor(t *testing.T, fn func(ctx context.Context) error) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := fn(ctx); err != nil {
			t.Errorf("run: %v", err)
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); wg.Wait() }) }
	t.Cleanup(stop)
	return stop
}

// countingPublisher counts the messages a pump got stored.
type countingPublisher struct {
	Publisher
	mu sync.Mutex
	n  int
}

func (c *countingPublisher) PublishBatch(ctx context.Context, ms []jetstream.Message) []error {
	errs := c.Publisher.PublishBatch(ctx, ms)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, err := range errs {
		if err == nil {
			c.n++
		}
	}
	return errs
}

func (c *countingPublisher) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}
