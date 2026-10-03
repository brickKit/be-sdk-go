package idem

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type widget struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

const userA = "user:0192f0c4-7b1e-7cc3-9a52-3f1d2e4b5a60"

// once runs Idempotent in its own transaction, counting executions.
func once(t *testing.T, e *env, c Command, now time.Time, runs *int, out widget) (widget, int, bool, error) {
	t.Helper()
	var (
		res      widget
		status   int
		replayed bool
	)
	err := e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
		var err error
		res, status, replayed, err = Idempotent(ctx, tx, c, now, func() (widget, error) {
			*runs++
			return out, nil
		})
		return err
	})
	return res, status, replayed, err
}

func TestFirstUseExecutesAndCompletes(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "Widget A"})
		runs := 0
		res, status, replayed, err := once(t, e, c, t0, &runs, widget{ID: "w1", Name: "Widget A"})
		require.NoError(t, err)
		require.Equal(t, 1, runs)
		require.False(t, replayed)
		require.Equal(t, 201, status)
		require.Equal(t, widget{ID: "w1", Name: "Widget A"}, res)

		var st, result string
		var hash []byte
		var created, expires time.Time
		e.scan(t, `SELECT status, result::text, request_hash, created_at, expires_at FROM SCHEMA.besdk_idempotency
			WHERE caller = $1 AND idempotency_key = $2`, []any{userA, "k1"}, &st, &result, &hash, &created, &expires)
		require.Equal(t, "DONE", st)
		require.JSONEq(t, `{"http":201,"body":{"id":"w1","name":"Widget A"}}`, result)
		require.Equal(t, c.Hash, hash)
		require.True(t, created.Equal(t0), "created_at %s", created)
		require.True(t, expires.Equal(t0.Add(30*24*time.Hour)), "expires_at %s", expires)
	})
}

func TestReplayReturnsStoredResultWithoutExecuting(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "Widget A"})
		runs := 0
		_, _, _, err := once(t, e, c, t0, &runs, widget{ID: "w1", Name: "Widget A"})
		require.NoError(t, err)

		again := c
		again.Status = 200 // the stored status wins on replay
		res, status, replayed, err := once(t, e, again, t0.Add(time.Hour), &runs, widget{ID: "w2"})
		require.NoError(t, err)
		require.Equal(t, 1, runs, "a replay must not execute")
		require.True(t, replayed)
		require.Equal(t, 201, status)
		require.Equal(t, widget{ID: "w1", Name: "Widget A"}, res)
	})
}

func TestMismatchOnCommandTargetOrBody(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.approve", "w1", map[string]any{"note": "ok"})
		runs := 0
		_, _, _, err := once(t, e, c, t0, &runs, widget{ID: "w1"})
		require.NoError(t, err)
		for name, other := range map[string]Command{
			"command": cmd(t, userA, "k1", "conformance.widget.reject", "w1", map[string]any{"note": "ok"}),
			"target":  cmd(t, userA, "k1", "conformance.widget.approve", "w2", map[string]any{"note": "ok"}),
			"body":    cmd(t, userA, "k1", "conformance.widget.approve", "w1", map[string]any{"note": "no"}),
		} {
			_, _, _, err := once(t, e, other, t0, &runs, widget{})
			require.ErrorIs(t, err, ErrMismatch, name)
		}
		require.Equal(t, 1, runs)
	})
}

func TestTwoStepClaimIsInProgress(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.approve", "w1", map[string]any{"note": "ok"})
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
			p, err := Claim(ctx, tx, c, t0)
			require.False(t, p.Found)
			return err
		}))
		runs := 0
		_, _, _, err := once(t, e, c, t0, &runs, widget{})
		require.ErrorIs(t, err, ErrInProgress)
		require.Equal(t, 0, runs)
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
			p, err := Lookup(ctx, tx, c, t0)
			require.Equal(t, Prior{Found: true, InProgress: true}, p)
			return err
		}))
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error { // GetStatus by key (P13.9)
			row, err := Get(ctx, tx, c.Caller, c.Key, t0)
			require.NoError(t, err)
			require.Equal(t, StatusClaimed, row.Status)
			require.Equal(t, c.Name, row.Command)
			gone, err := Get(ctx, tx, c.Caller, c.Key, t0.Add(TTL))
			require.NoError(t, err)
			require.Nil(t, gone, "expired at expires_at")
			other, err := Get(ctx, tx, SystemNamespace, c.Key, t0)
			require.Nil(t, other, "another namespace")
			return err
		}))

		// step two completes in another transaction; the key then replays
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
			return Complete(ctx, tx, c, Result{HTTP: 200, Body: json.RawMessage(`{"id":"w1","name":"approved"}`)}, t0)
		}))
		res, status, replayed, err := once(t, e, c, t0, &runs, widget{})
		require.NoError(t, err)
		require.True(t, replayed)
		require.Equal(t, 200, status)
		require.Equal(t, widget{ID: "w1", Name: "approved"}, res)
		require.Equal(t, 0, runs)
	})
}

func TestReleaseThenRetryExecutes(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.approve", "w1", map[string]any{"note": "ok"})
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error { _, err := Claim(ctx, tx, c, t0); return err }))
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error { return Release(ctx, tx, c) }))
		require.Equal(t, 0, e.count(t))
		runs := 0
		_, _, replayed, err := once(t, e, c, t0, &runs, widget{ID: "w1"})
		require.NoError(t, err)
		require.False(t, replayed)
		require.Equal(t, 1, runs)
		// Release never removes a completed key
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error { return Release(ctx, tx, c) }))
		require.Equal(t, 1, e.count(t))
	})
}

func TestFailedOneStepLeavesNoClaim(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "A"})
		boom := errors.New("business rule failed")
		err := e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
			_, _, _, err := Idempotent(ctx, tx, c, t0, func() (widget, error) { return widget{}, boom })
			return err
		})
		require.ErrorIs(t, err, boom)
		require.Equal(t, 0, e.count(t))
		runs := 0
		_, _, replayed, err := once(t, e, c, t0, &runs, widget{ID: "w1"})
		require.NoError(t, err)
		require.False(t, replayed)
		require.Equal(t, 1, runs)
	})
}

// A caller that swallows do's error and commits must not leave a claim that blocks the key for 30 days.
func TestFailedOneStepReleasesEvenIfCommitted(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "A"})
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
			_, _, _, err := Idempotent(ctx, tx, c, t0, func() (widget, error) { return widget{}, errors.New("no") })
			require.Error(t, err)
			return nil
		}))
		require.Equal(t, 0, e.count(t))
	})
}

func TestNamespacesAreIsolated(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		req := map[string]any{"name": "Widget A"}
		runs := 0
		_, _, _, err := once(t, e, cmd(t, userA, "k1", "conformance.widget.create", "", req), t0, &runs, widget{ID: "a"})
		require.NoError(t, err)
		for i, ns := range []string{"user:0192f0c4-7b1e-7cc3-9a52-3f1d2e4b5a61", "svc:erp/sales", SystemNamespace} {
			c := cmd(t, ns, "k1", "conformance.widget.create", "", req)
			require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
				p, err := Lookup(ctx, tx, c, t0)
				require.Equal(t, Prior{}, p, ns)
				return err
			}))
			res, _, replayed, err := once(t, e, c, t0, &runs, widget{ID: ns})
			require.NoError(t, err)
			require.False(t, replayed, ns)
			require.Equal(t, ns, res.ID)
			require.Equal(t, i+2, runs)
		}
		res, _, replayed, err := once(t, e, cmd(t, userA, "k1", "conformance.widget.create", "", req), t0, &runs, widget{})
		require.NoError(t, err)
		require.True(t, replayed)
		require.Equal(t, "a", res.ID)
		require.Equal(t, 4, e.count(t))
	})
}

func TestExpiredKeyIsANewCommand(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		runs := 0
		_, _, _, err := once(t, e, cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "A"}),
			t0, &runs, widget{ID: "old"})
		require.NoError(t, err)
		later := t0.Add(TTL) // half-open: at expires_at the key is new
		c := cmd(t, userA, "k1", "conformance.widget.rename", "w9", map[string]any{"name": "B"})
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
			p, err := Lookup(ctx, tx, c, later)
			require.Equal(t, Prior{}, p)
			return err
		}))
		res, _, replayed, err := once(t, e, c, later, &runs, widget{ID: "new"})
		require.NoError(t, err)
		require.False(t, replayed)
		require.Equal(t, "new", res.ID)
		var command, target string
		var created time.Time
		e.scan(t, `SELECT command, target, created_at FROM SCHEMA.besdk_idempotency`, nil, &command, &target, &created)
		require.Equal(t, []string{"conformance.widget.rename", "w9"}, []string{command, target})
		require.True(t, created.Equal(later))
	})
}

func TestConcurrentSameKeyExecutesOnce(t *testing.T) { // CP-IDEM-07
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "A"})
		race(t, e, c, t0)
	})
}

// Two requests that both find the same expired row: the row lock makes exactly one take it over.
func TestConcurrentTakeoverOfExpiredKeyExecutesOnce(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		runs := 0
		_, _, _, err := once(t, e, cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "old"}),
			t0, &runs, widget{ID: "old"})
		require.NoError(t, err)
		race(t, e, cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "A"}), t0.Add(TTL))
	})
}

// race runs the same command in 20 goroutines, each in its own transaction, and requires exactly
// one execution; the others replay or see the claim in progress.
func race(t *testing.T, e *env, c Command, now time.Time) {
	t.Helper()
	const n = 20
	type out struct {
		replayed bool
		err      error
	}
	results := make(chan out, n)
	executed := make(chan struct{}, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			<-start
			var o out
			o.err = e.store.Run(context.Background(), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
				var err error
				_, _, o.replayed, err = Idempotent(ctx, tx, c, now, func() (widget, error) {
					executed <- struct{}{}
					time.Sleep(50 * time.Millisecond)
					return widget{ID: "w1"}, nil
				})
				return err
			})
			results <- o
		}()
	}
	close(start)
	replays, inProgress := 0, 0
	for i := 0; i < n; i++ {
		o := <-results
		switch {
		case o.err == nil && o.replayed:
			replays++
		case errors.Is(o.err, ErrInProgress):
			inProgress++
		case o.err != nil:
			t.Fatalf("unexpected error: %v", o.err)
		}
	}
	require.Equal(t, 1, len(executed), "exactly one execution")
	require.Equal(t, n-1, replays+inProgress)
	require.Equal(t, 1, e.count(t))
}

func TestDeleteExpiredInBatches(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		for i, created := range []time.Time{t0.Add(-40 * 24 * time.Hour), t0.Add(-31 * 24 * time.Hour), t0.Add(-TTL),
			t0.Add(-TTL), t0.Add(-35 * 24 * time.Hour), t0.Add(-TTL + time.Microsecond), t0} {
			e.exec(t, `INSERT INTO SCHEMA.besdk_idempotency (caller, idempotency_key, command, request_hash, status, created_at, expires_at)
				VALUES ('system', $1, 'c', '\x00', 'CLAIMED', $2, $3)`, string(rune('a'+i)), created, ExpiresAt(created))
		}
		var counts []int64
		for i := 0; i < 3; i++ {
			require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
				n, err := DeleteExpired(ctx, tx, t0, 3)
				counts = append(counts, n)
				return err
			}))
		}
		require.Equal(t, []int64{3, 2, 0}, counts)
		require.Equal(t, 2, e.count(t))
	})
}

func TestCompleteWithoutClaimFails(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "A"})
		err := e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
			return Complete(ctx, tx, c, Result{HTTP: 201, Body: json.RawMessage(`{}`)}, t0)
		})
		require.Error(t, err)
	})
}

// A gRPC command's response message is stored as protojson and replayed into a fresh message.
func TestProtoResultReplays(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, "svc:erp/sales", "k1", "conformance.v1.WidgetService/TouchWidget", "w1", map[string]any{})
		c.Status = 0
		call := func() (*wrapperspb.StringValue, int, bool) {
			var (
				res      *wrapperspb.StringValue
				status   int
				replayed bool
			)
			require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
				var err error
				res, status, replayed, err = Idempotent(ctx, tx, c, t0, func() (*wrapperspb.StringValue, error) {
					return wrapperspb.String("touched"), nil
				})
				return err
			}))
			return res, status, replayed
		}
		first, status, replayed := call()
		require.False(t, replayed)
		require.Equal(t, 200, status)
		again, status, replayed := call()
		require.True(t, replayed)
		require.Equal(t, 200, status)
		require.Equal(t, first.GetValue(), again.GetValue())
	})
}

// No key: the command runs without idempotency and nothing is stored.
func TestNoKeyRunsWithoutIdempotency(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		c := cmd(t, userA, "", "conformance.widget.create", "", map[string]any{"name": "A"})
		runs := 0
		for i := 0; i < 2; i++ {
			_, status, replayed, err := once(t, e, c, t0, &runs, widget{ID: "w"})
			require.NoError(t, err)
			require.False(t, replayed)
			require.Equal(t, 201, status)
		}
		require.Equal(t, 2, runs)
		require.Equal(t, 0, e.count(t))
	})
}
