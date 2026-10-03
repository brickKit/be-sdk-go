package idem

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/stretchr/testify/require"
)

// pausing pauses after Claim's read of the existing row, so a test can interleave another
// transaction exactly there.
type pausing struct {
	Querier
	paused chan<- struct{}
	resume <-chan struct{}
}

func (p pausing) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	r := p.Querier.QueryRowContext(ctx, query, args...)
	if strings.HasPrefix(query, "SELECT command") {
		close(p.paused)
		select {
		case <-p.resume:
		case <-time.After(time.Second):
		}
	}
	return r
}

// B reads the expired row and stops; A then tries the same key. Without the row lock both would
// take the row over and both execute; with it A waits for B and replays B's result.
func TestExpiredTakeoverHoldsTheRowLock(t *testing.T) {
	majors(t, func(t *testing.T, e *env) {
		runs := 0
		_, _, _, err := once(t, e, cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "old"}),
			t0, &runs, widget{ID: "old"})
		require.NoError(t, err)
		c := cmd(t, userA, "k1", "conformance.widget.create", "", map[string]any{"name": "A"})
		later := t0.Add(TTL)
		var executed atomic.Int32
		do := func() (widget, error) {
			executed.Add(1)
			time.Sleep(50 * time.Millisecond)
			return widget{ID: "new"}, nil
		}
		paused, resume := make(chan struct{}), make(chan struct{})
		bDone := make(chan error, 1)
		go func() {
			bDone <- e.store.Run(context.Background(), pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
				_, _, _, err := Idempotent(ctx, pausing{Querier: tx, paused: paused, resume: resume}, c, later, do)
				return err
			})
		}()
		<-paused
		var replayed bool
		require.NoError(t, e.tx(t, func(ctx context.Context, tx *pg.Tx) error {
			p, err := Claim(ctx, tx, c, later)
			if err == nil && !p.Found {
				close(resume) // A holds the claim: let B go on
				_, err = do()
				if err == nil {
					err = Complete(ctx, tx, c, Result{HTTP: 201, Body: []byte(`{"id":"new"}`)}, later)
				}
			}
			replayed = p.Found
			return err
		}))
		require.NoError(t, <-bDone)
		require.Equal(t, int32(1), executed.Load(), "exactly one execution")
		require.True(t, replayed, "A replays B's result")
	})
}
