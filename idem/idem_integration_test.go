//go:build integration

package idem_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var notes = fstest.MapFS{"0001_notes.up.sql": {Data: []byte("CREATE TABLE notes (id uuid PRIMARY KEY, body text NOT NULL);")}}

const alice, bob = "0190f3a2-7c1e-7b2a-9d4e-00000000a11c", "0190f3a2-7c1e-7b2a-9d4e-000000000b0b"

func create(d *db.DB, r idem.Request, runs *atomic.Int32, hold time.Duration) (idem.Outcome, bool, error) {
	return idem.Do(context.Background(), d, r, func(ctx context.Context, tx db.Tx) (idem.Outcome, error) {
		runs.Add(1)
		id := ids.New()
		if _, err := tx.Exec(ctx, "INSERT INTO notes VALUES ($1, $2)", id, string(r.Body)); err != nil {
			return idem.Outcome{}, err
		}
		time.Sleep(hold)
		res, _ := json.Marshal(map[string]string{"id": id.String()})
		return idem.Outcome{Status: 201, Result: res}, nil
	})
}

func count(t *testing.T, d *db.DB) int {
	var n int
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT count(*) FROM notes").Scan(&n)
	}))
	return n
}

func TestOnceAndReplayed(t *testing.T) { // P4.23, CK-IDEM-02
	d, _ := dbtest.Schema(t, "idem_once", notes)
	var runs atomic.Int32
	r := idem.Request{Principal: alice, Key: "k-1", Operation: "demo.note.create", Target: "POST /api/notes", Body: []byte(`{"t":1}`)}
	first, replayed, err := create(d, r, &runs, 0)
	require.NoError(t, err)
	require.False(t, replayed)
	again, replayed, err := create(d, r, &runs, 0)
	require.NoError(t, err)
	require.True(t, replayed)
	assert.Equal(t, first.Status, again.Status)
	assert.JSONEq(t, string(first.Result), string(again.Result), "the replay returns the stored outcome")
	assert.EqualValues(t, 1, runs.Load())

	other := r
	other.Body = []byte(`{"t":2}`)
	_, _, err = create(d, other, &runs, 0)
	assert.True(t, errs.Is(err, "be", errs.ReasonIdempotencyKeyReused), "%v", err)
	bobs := r
	bobs.Principal = bob
	_, replayed, err = create(d, bobs, &runs, 0)
	require.NoError(t, err)
	assert.False(t, replayed, "another principal's same key is a different claim")
	assert.Equal(t, 2, count(t, d))
}

func TestAFailedCommandLeavesNoClaim(t *testing.T) { // P4.23
	d, _ := dbtest.Schema(t, "idem_fail", notes)
	r := idem.Request{Principal: alice, Key: "k-fail", Operation: "demo.note.create", Target: "POST /api/notes", Body: []byte("x")}
	_, _, err := idem.Do(context.Background(), d, r, func(ctx context.Context, tx db.Tx) (idem.Outcome, error) {
		if _, err := tx.Exec(ctx, "INSERT INTO notes VALUES ($1, 'lost')", ids.New()); err != nil {
			return idem.Outcome{}, err
		}
		return idem.Outcome{}, errors.New("the command failed")
	})
	require.Error(t, err)
	var runs atomic.Int32
	_, replayed, err := create(d, r, &runs, 0)
	require.NoError(t, err)
	assert.False(t, replayed, "the same key runs again after a failure")
	assert.Equal(t, 1, count(t, d), "the failed attempt's write rolled back with its claim")
}

func TestConcurrentUsesRunOnce(t *testing.T) { // CK-IDEM-02, multi-instance primitive
	d, _ := dbtest.Schema(t, "idem_race", notes)
	var runs atomic.Int32
	r := idem.Request{Principal: alice, Key: "k-race", Operation: "demo.note.create", Target: "POST /api/notes", Body: []byte("same")}
	var wg sync.WaitGroup
	results := make([]idem.Outcome, 8)
	replays := make([]bool, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			results[i], replays[i], err = create(d, r, &runs, 300*time.Millisecond)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 1, runs.Load(), "the command ran once")
	assert.Equal(t, 1, count(t, d))
	fresh := 0
	for i := range results {
		assert.JSONEq(t, string(results[0].Result), string(results[i].Result))
		if !replays[i] {
			fresh++
		}
	}
	assert.Equal(t, 1, fresh, "every other use is a replay")
}

func TestInProgressWhenTheFirstUseOutlastsTheLockWait(t *testing.T) { // P4.23
	d, _ := dbtest.Schema(t, "idem_wait", notes)
	var runs atomic.Int32
	r := idem.Request{Principal: alice, Key: "k-slow", Operation: "demo.note.create", Target: "POST /api/notes", Body: []byte("slow")}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, _, err := idem.Do(context.Background(), d, r, func(ctx context.Context, tx db.Tx) (idem.Outcome, error) {
			close(started)
			time.Sleep(3 * time.Second) // longer than the 2-second lock wait
			return idem.Outcome{Status: 201}, nil
		})
		done <- err
	}()
	<-started
	_, _, err := create(d, r, &runs, 0)
	assert.True(t, errs.Is(err, "be", errs.ReasonIdempotencyInProgress), "%v", err)
	require.NoError(t, <-done)
	_, replayed, err := create(d, r, &runs, 0)
	require.NoError(t, err)
	assert.True(t, replayed)
	assert.Zero(t, runs.Load())
}

func TestCleanupRemovesOnlyExpiredRecords(t *testing.T) { // P4.25, CK-IDEM-04
	d, admin := dbtest.Schema(t, "idem_clean", notes)
	var runs atomic.Int32
	for _, k := range []string{"old-1", "old-2", "fresh"} {
		_, _, err := create(d, idem.Request{Principal: alice, Key: k, Operation: "demo.note.create", Target: "POST /api/notes"}, &runs, 0)
		require.NoError(t, err)
	}
	pgtest.Exec(t, admin, "UPDATE be_idempotency SET expires_at = now() - interval '1 second' WHERE key LIKE 'old-%'")
	n, err := idem.Cleanup(context.Background(), d)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	var left []string
	rows, err := admin.Query(context.Background(), "SELECT key FROM be_idempotency")
	require.NoError(t, err)
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		left = append(left, k)
	}
	assert.Equal(t, []string{"fresh"}, left)
}
