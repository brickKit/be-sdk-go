//go:build integration

package events

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var stock = Stream{Name: "demo.stock", Partitions: 2}

// source provisions and migrates a producer schema and returns its handle and an admin connection to it.
func source(t *testing.T, name string) (*db.DB, *pgx.Conn) { return dbtest.Schema(t, name, nil) }

func emit(t *testing.T, d *db.DB, p *Producer, n int) {
	for i := 0; i < n; i++ {
		require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			id := ids.New().String()
			if _, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1)", id); err != nil {
				return err
			}
			_, err := p.Emit(ctx, tx, "demo.stock", Event{Type: "demo.stock.moved.v1", RecordID: id, Version: 1,
				Projection: Full, Data: map[string]string{"id": id}})
			return err
		}))
	}
}

func positions(t *testing.T, admin *pgx.Conn, partition int) []int64 {
	rows, err := admin.Query(context.Background(), "SELECT seq FROM be_outbox WHERE partition = $1 AND seq IS NOT NULL ORDER BY seq", partition)
	require.NoError(t, err)
	seqs, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	require.NoError(t, err)
	return seqs
}

func TestEmitOnlyWithTheBusinessCommit(t *testing.T) { // CK-EV-01
	d, admin := source(t, "evsrc1")
	p, err := NewProducer("demo/evsrc1", []Stream{stock}, nil)
	require.NoError(t, err)
	err = d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := p.Emit(ctx, tx, "demo.stock", Event{Type: "demo.stock.moved.v1", RecordID: "r1", Version: 1, Projection: Full})
		require.NoError(t, err)
		return assert.AnError
	})
	require.ErrorIs(t, err, assert.AnError)
	var n int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_outbox").Scan(&n))
	assert.Zero(t, n)
	emit(t, d, p, 1)
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_outbox").Scan(&n))
	assert.Equal(t, 1, n)
}

func TestTwoNumberersNeverShareAPosition(t *testing.T) { // CK-EV-02, RT09
	d, admin := source(t, "evsrc2")
	p, err := NewProducer("demo/evsrc2", []Stream{stock}, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ { // emitters
		wg.Add(1)
		go func() { defer wg.Done(); emit(t, d, p, 60) }()
	}
	var numberers sync.WaitGroup
	for r := 0; r < 2; r++ { // two replicas numbering both partitions as fast as they can
		numberers.Add(1)
		go func() {
			defer numberers.Done()
			for ctx.Err() == nil {
				for part := 0; part < stock.Partitions; part++ {
					_, _ = Number(ctx, d, stock.Name, part, 7)
				}
			}
		}()
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond)
	cancel()
	numberers.Wait()
	for part := 0; part < stock.Partitions; part++ {
		_, err := Number(context.Background(), d, stock.Name, part, 1000)
		require.NoError(t, err)
	}
	total := 0
	for part := 0; part < stock.Partitions; part++ {
		seqs := positions(t, admin, part)
		total += len(seqs)
		for i, s := range seqs {
			require.Equal(t, int64(i+1), s, "partition %d: positions are 1..n with no gap or repeat", part)
		}
	}
	assert.Equal(t, 240, total)
}

func TestALongTransactionDelaysOnlyItsOwnRows(t *testing.T) { // P6.6
	d, admin := source(t, "evsrc3")
	single := Stream{Name: "demo.stock", Partitions: 1}
	p, err := NewProducer("demo/evsrc3", []Stream{single}, nil)
	require.NoError(t, err)
	inserted := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			if _, err := p.Emit(ctx, tx, "demo.stock", Event{Type: "demo.stock.moved.v1", RecordID: "slow", Version: 1, Projection: Full}); err != nil {
				return err
			}
			close(inserted)
			<-release
			return nil
		}, db.StatementTimeout(30*time.Second))
	}()
	<-inserted
	emit(t, d, p, 2) // inserted later, committed first
	n, err := Number(context.Background(), d, single.Name, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "the open transaction's row is not visible yet")
	close(release)
	require.NoError(t, <-done)
	n, err = Number(context.Background(), d, single.Name, 0, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	var slowSeq int64
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT seq FROM be_outbox WHERE record_id = 'slow'").Scan(&slowSeq))
	assert.Equal(t, int64(3), slowSeq, "numbered after the rows that committed before it, no gap")
	assert.Equal(t, []int64{1, 2, 3}, positions(t, admin, 0))
}

func TestANewEpochRestartsPositionsAndRecordsTheFork(t *testing.T) { // CK-EV-03
	d, admin := source(t, "evsrc4")
	single := Stream{Name: "demo.stock", Partitions: 1}
	p, err := NewProducer("demo/evsrc4", []Stream{single}, nil)
	require.NoError(t, err)
	emit(t, d, p, 3)
	_, err = Number(context.Background(), d, single.Name, 0, 100)
	require.NoError(t, err)
	pgtest.Exec(t, admin, "UPDATE be_recovery_generation SET generation = 2") // the runbook, before unfreezing
	emit(t, d, p, 2)
	_, err = Number(context.Background(), d, single.Name, 0, 100)
	require.NoError(t, err)
	rows, err := admin.Query(context.Background(), "SELECT epoch_generation, seq FROM be_outbox ORDER BY ord")
	require.NoError(t, err)
	type pos struct{ gen, seq int64 }
	got, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (pos, error) {
		var p pos
		return p, r.Scan(&p.gen, &p.seq)
	})
	require.NoError(t, err)
	assert.Equal(t, []pos{{1, 1}, {1, 2}, {1, 3}, {2, 1}, {2, 2}}, got)
	var oldGen, fork, newGen int64
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT old_generation, fork_seq, new_generation FROM be_epoch_history").
		Scan(&oldGen, &fork, &newGen))
	assert.Equal(t, [3]int64{1, 3, 2}, [3]int64{oldGen, fork, newGen})

	pgtest.Exec(t, admin, "DELETE FROM be_recovery_generation")
	emit(t, d, p, 1)
	_, err = Number(context.Background(), d, single.Name, 0, 100)
	require.Error(t, err, "no generation: numbering refused")
}

func TestForwarderPublishesInOrderOnceNumbered(t *testing.T) { // P6.10
	d, admin := source(t, "evsrc5")
	p, err := NewProducer("demo/evsrc5", []Stream{stock}, nil)
	require.NoError(t, err)
	tr := NewMemory(0)
	emit(t, d, p, 20)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f1 := &Forwarder{DB: d, Producer: p, Transport: tr, Source: "demo/evsrc5", Impl: "6f1c2d3e-4a5b-4c6d-8e7f-901234567890", Log: log}
	f2 := &Forwarder{DB: d, Producer: p, Transport: tr, Source: "demo/evsrc5", Impl: "6f1c2d3e-4a5b-4c6d-8e7f-901234567890", Log: log}
	var wg sync.WaitGroup
	for _, f := range []*Forwarder{f1, f2} {
		wg.Add(1)
		go func(f *Forwarder) { defer wg.Done(); _, err := f.Once(context.Background()); assert.NoError(t, err) }(f)
	}
	wg.Wait()
	_, err = f1.Once(context.Background())
	require.NoError(t, err)
	var unpublished int
	require.NoError(t, admin.QueryRow(context.Background(), "SELECT count(*) FROM be_outbox WHERE published_at IS NULL").Scan(&unpublished))
	assert.Zero(t, unpublished)

	got := map[string][]int{}
	var mu sync.Mutex
	sub, err := tr.Subscribe(context.Background(), SubscriptionSpec{Durable: "check", Streams: []string{"demo.stock"}},
		func(_ context.Context, del *Delivery) Outcome {
			mu.Lock()
			defer mu.Unlock()
			got[del.Headers[HeaderPartition]] = append(got[del.Headers[HeaderPartition]], atoi(del.Headers[HeaderSeq]))
			return Ack
		})
	require.NoError(t, err)
	defer sub.Stop()
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(got["0"])+len(got["1"]) == 20 }, 5*time.Second, 20*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for part, seqs := range got {
		assert.True(t, sort.IntsAreSorted(seqs), "partition %s published in position order: %v", part, seqs)
	}
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}

// slowClaims is object storage that takes a while per upload and records how many uploads overlapped.
type slowClaims struct {
	mu        sync.Mutex
	now, peak int
	delay     time.Duration
}

func (s *slowClaims) Put(ctx context.Context, eventID string, _ []byte) (string, error) {
	s.mu.Lock()
	s.now++
	s.peak = max(s.peak, s.now)
	s.mu.Unlock()
	time.Sleep(s.delay)
	s.mu.Lock()
	s.now--
	s.mu.Unlock()
	return "s3://claims/" + eventID, nil
}

func TestClaimUploadsOverlapAndPublishingKeepsItsOrder(t *testing.T) { // P6.10, P6.14
	d, _ := source(t, "evsrc8")
	one := Stream{Name: "demo.stock", Partitions: 1}
	claims := &slowClaims{delay: 40 * time.Millisecond}
	p, err := NewProducer("demo/evsrc8", []Stream{one}, claims)
	require.NoError(t, err)
	const n = 12
	big := make([]byte, MaxPayload+1)
	for i := 0; i < n; i++ {
		require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			id := ids.New().String()
			if _, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1)", id); err != nil {
				return err
			}
			_, err := p.Emit(ctx, tx, "demo.stock", Event{Type: "demo.stock.moved.v1", RecordID: id, Version: 1,
				Projection: Full, Data: map[string]string{"blob": string(big)}})
			return err
		}))
	}
	tr := NewMemory(0)
	f := &Forwarder{DB: d, Producer: p, Transport: tr, Source: "demo/evsrc8", Impl: "6f1c2d3e-4a5b-4c6d-8e7f-901234567890",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	started := time.Now()
	published, err := f.Once(context.Background())
	took := time.Since(started)
	require.NoError(t, err)
	require.Equal(t, n, published)
	assert.Greater(t, claims.peak, 1, "uploads of one batch overlap")
	assert.LessOrEqual(t, claims.peak, ClaimConcurrency)
	assert.Less(t, took, n*claims.delay/2, "one upload at a time caps a partition at 1/delay events a second")

	var seqs []int
	var mu sync.Mutex
	sub, err := tr.Subscribe(context.Background(), SubscriptionSpec{Durable: "check", Streams: []string{"demo.stock"}},
		func(_ context.Context, del *Delivery) Outcome {
			mu.Lock()
			defer mu.Unlock()
			seqs = append(seqs, atoi(del.Headers[HeaderSeq]))
			return Ack
		})
	require.NoError(t, err)
	defer sub.Stop()
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(seqs) == n }, 5*time.Second, 20*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.True(t, sort.IntsAreSorted(seqs), "published in position order: %v", seqs)
}

func TestTheCommitWakesTheForwarder(t *testing.T) { // P6.29
	// at a low rate every event used to wait for the next poll (about 120 ms at 50/s with a 250 ms poll); the commit
	// that wrote it now wakes the member's own forwarder
	d, _ := source(t, "evsrc9")
	one := Stream{Name: "demo.stock", Partitions: 1}
	p, err := NewProducer("demo/evsrc9", []Stream{one}, nil)
	require.NoError(t, err)
	tr := NewMemory(0)
	f := &Forwarder{DB: d, Producer: p, Transport: tr, Source: "demo/evsrc9", Impl: "6f1c2d3e-4a5b-4c6d-8e7f-901234567890",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Poll: 10 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, f.Start(ctx))
	got := make(chan time.Time, 1)
	sub, err := tr.Subscribe(ctx, SubscriptionSpec{Durable: "wake", Streams: []string{"demo.stock"}},
		func(context.Context, *Delivery) Outcome { got <- time.Now(); return Ack })
	require.NoError(t, err)
	defer sub.Stop()
	time.Sleep(200 * time.Millisecond) // the forwarder's first round is over; the next poll is 10 s away
	written := time.Now()
	emit(t, d, p, 1)
	select {
	case at := <-got:
		assert.Less(t, at.Sub(written), time.Second, "published right after the commit")
	case <-time.After(5 * time.Second):
		t.Fatal("not published within 5 s: the forwarder waited for its poll")
	}
}
