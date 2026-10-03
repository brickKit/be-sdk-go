package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type order struct{ ID, State string }

// Reconciler: "a" is done at once, "b" never finishes, "c" always fails; with three attempts and a
// 50 ms backoff, b and c are given up after three handled passes each.
func TestReconcilerHappyPathBackoffAndGiveUp(t *testing.T) {
	e := newEnv(t)
	e.exec(t, `INSERT INTO SCHEMA.orders (id, state) VALUES ('a', 'PENDING'), ('b', 'PENDING'), ('c', 'PENDING')`)
	var mu sync.Mutex
	handled := map[string]int{}
	rec := NewReconciler(ReconcilerSpec[order]{
		Name: "widget.orders", Every: time.Hour, Timeout: 5 * time.Second, MaxAttempts: 3,
		Backoff: []time.Duration{50 * time.Millisecond},
		Candidates: func(ctx context.Context, tx *pg.Tx, limit int) ([]order, error) {
			rows, err := tx.QueryContext(ctx, `SELECT id, state FROM orders WHERE state = 'PENDING' ORDER BY id LIMIT $1`, limit)
			if err != nil {
				return nil, err
			}
			defer func() { _ = rows.Close() }()
			var out []order
			for rows.Next() {
				var o order
				if err := rows.Scan(&o.ID, &o.State); err != nil {
					return nil, err
				}
				out = append(out, o)
			}
			return out, rows.Err()
		},
		ID: func(o order) string { return o.ID },
		Handle: func(ctx context.Context, o order) (Outcome, error) {
			mu.Lock()
			handled[o.ID]++
			mu.Unlock()
			switch o.ID {
			case "a":
				return Outcome{Done: true, State: "CONFIRMED"}, nil
			case "b":
				return Outcome{State: "STILL_PENDING"}, nil
			}
			return Outcome{}, errors.New("downstream unavailable")
		},
		Apply: func(ctx context.Context, tx *pg.Tx, o order, out Outcome) error {
			if !out.Done {
				return nil
			}
			_, err := tx.ExecContext(ctx, `UPDATE orders SET state = $2 WHERE id = $1 AND state = 'PENDING'`, o.ID, out.State)
			return err
		},
		GiveUp: func(ctx context.Context, tx *pg.Tx, o order) error {
			_, err := tx.ExecContext(ctx, `UPDATE orders SET state = 'SUSPENDED' WHERE id = $1 AND state = 'PENDING'`, o.ID)
			return err
		},
	})
	reg := prometheus.NewRegistry()
	eng := e.engine(t, "i1", Declarations{Reconcilers: []*Reconciler{rec}}, func(c *Config) { c.Registerer = reg })
	for range 12 {
		eng.RunOnce(within(t, 5*time.Second), "widget.orders")
		time.Sleep(70 * time.Millisecond)
	}
	states := map[string]string{}
	for _, id := range []string{"a", "b", "c"} {
		var s string
		e.scan(t, `SELECT state FROM SCHEMA.orders WHERE id = '`+id+`'`, &s)
		states[id] = s
	}
	require.Equal(t, map[string]string{"a": "CONFIRMED", "b": "SUSPENDED", "c": "SUSPENDED"}, states)
	mu.Lock()
	require.Equal(t, map[string]int{"a": 1, "b": 3, "c": 3}, handled, "three attempts, then give up")
	mu.Unlock()
	require.Equal(t, 0, e.count(t, `SELECT count(*) FROM SCHEMA.besdk_reconcile`), "terminal items leave no row")
	require.Equal(t, 2.0, counterValue(t, eng.m.recGiveups.WithLabelValues("widget.orders")))
	require.Equal(t, RunNoop, eng.RunOnce(within(t, 5*time.Second), "widget.orders").Result)
}

// The reconcile claim respects the backoff: an item is not handled again before next_at.
func TestReconcilerBackoffSkipsItem(t *testing.T) {
	e := newEnv(t)
	e.exec(t, `INSERT INTO SCHEMA.orders (id, state) VALUES ('b', 'PENDING')`)
	var calls int
	rec := NewReconciler(ReconcilerSpec[string]{
		Name: "widget.orders", Every: time.Hour, Timeout: 5 * time.Second, MaxAttempts: 10,
		Backoff:    []time.Duration{time.Hour},
		Candidates: func(context.Context, *pg.Tx, int) ([]string, error) { return []string{"b"}, nil },
		ID:         func(s string) string { return s },
		Handle:     func(context.Context, string) (Outcome, error) { calls++; return Outcome{State: "PENDING"}, nil },
		GiveUp:     func(context.Context, *pg.Tx, string) error { return nil },
	})
	eng := e.engine(t, "i1", Declarations{Reconcilers: []*Reconciler{rec}})
	eng.RunOnce(within(t, 5*time.Second), "widget.orders")
	eng.RunOnce(within(t, 5*time.Second), "widget.orders")
	require.Equal(t, 1, calls)
	var attempts int
	var backingOff bool
	e.scan(t, `SELECT attempts, next_at > now() + interval '50 minutes' FROM SCHEMA.besdk_reconcile WHERE item_id = 'b'`,
		&attempts, &backingOff)
	require.Equal(t, 1, attempts)
	require.True(t, backingOff)
}
