//go:build integration

package reserve_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/reserve"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A participant of its own: items with stock on hand and reserved, a branch's line (one item, a quantity, what was
// consumed of it), and the events it publishes (one row a change, written in the change's transaction, in its order by the clock).
var tables = fstest.MapFS{"0001_demo.up.sql": {Data: []byte(`
CREATE TABLE item (id text PRIMARY KEY, on_hand numeric NOT NULL, reserved numeric NOT NULL DEFAULT 0,
	CHECK (reserved >= 0 AND reserved <= on_hand));
CREATE TABLE line (branch_id uuid PRIMARY KEY, item text NOT NULL REFERENCES item (id), quantity numeric NOT NULL,
	consumed numeric NOT NULL DEFAULT 0 CHECK (consumed <= quantity));
CREATE TABLE published (at timestamptz NOT NULL DEFAULT clock_timestamp(), branch_id uuid NOT NULL, state text NOT NULL,
	version bigint NOT NULL);
`)}}

type demo struct {
	d    *db.DB
	kind reserve.Kind
}

func newDemo(t testing.TB, name string) *demo {
	d, _ := dbtest.Schema(t, name, tables)
	k := reserve.Kind{Name: "demo.stock", Period: 5 * time.Minute,
		Lock: func(ctx context.Context, tx db.Tx, id string) error { // the item's row, before the branch (P16.11)
			_, err := tx.Exec(ctx, `SELECT 1 FROM item WHERE id = (SELECT item FROM line WHERE branch_id = $1) FOR UPDATE`, id)
			return err
		},
		GiveBack: func(ctx context.Context, tx db.Tx, b reserve.Branch) error {
			_, err := tx.Exec(ctx, `UPDATE item SET reserved = reserved - (l.quantity - l.consumed) FROM line l
				WHERE l.branch_id = $1 AND item.id = l.item`, b.ID)
			return err
		},
		Changed: func(ctx context.Context, tx db.Tx, b reserve.Branch) error {
			_, err := tx.Exec(ctx, `INSERT INTO published (branch_id, state, version) VALUES ($1, $2, $3)`, b.ID,
				string(b.State), b.Version)
			return err
		}}
	return &demo{d: d, kind: k}
}

func (x *demo) stock(t testing.TB, item string, onHand int) {
	require.NoError(t, x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO item (id, on_hand) VALUES ($1, $2)`, item, onHand)
		return err
	}))
}

func (x *demo) hold(t testing.TB, id, item string, qty int) reserve.Answer {
	t.Helper()
	var a reserve.Answer
	require.NoError(t, x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		var err error
		a, err = reserve.Hold(ctx, tx, x.kind, id, item+"x"+string(rune('0'+qty)), func(ctx context.Context, tx db.Tx) (bool, string, error) {
			var enough bool
			if err := tx.QueryRow(ctx, `SELECT on_hand - reserved >= $2 FROM item WHERE id = $1 FOR UPDATE`, item, qty).
				Scan(&enough); err != nil {
				return false, "", err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO line (branch_id, item, quantity) VALUES ($1, $2, $3)`, id, item, qty); err != nil {
				return false, "", err
			}
			if !enough {
				return false, "insufficient", nil
			}
			_, err := tx.Exec(ctx, `UPDATE item SET reserved = reserved + $2 WHERE id = $1`, item, qty)
			return err == nil, "", err
		})
		return err
	}))
	return a
}

func (x *demo) do(t testing.TB, f func(ctx context.Context, tx db.Tx) (reserve.Answer, error)) reserve.Answer {
	t.Helper()
	var a reserve.Answer
	require.NoError(t, x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		var err error
		a, err = f(ctx, tx)
		return err
	}))
	return a
}

func (x *demo) confirm(t testing.TB, id string) reserve.Answer {
	return x.do(t, func(ctx context.Context, tx db.Tx) (reserve.Answer, error) {
		return reserve.Confirm(ctx, tx, x.kind, id)
	})
}

func (x *demo) cancel(t testing.TB, id string) reserve.Answer {
	return x.do(t, func(ctx context.Context, tx db.Tx) (reserve.Answer, error) {
		return reserve.Cancel(ctx, tx, x.kind, id)
	})
}

func (x *demo) query(t testing.TB, id string) reserve.Answer {
	return x.do(t, func(ctx context.Context, tx db.Tx) (reserve.Answer, error) { return reserve.Query(ctx, tx, x.kind, id) })
}

func (x *demo) consume(t testing.TB, id, consumption string, qty int) reserve.Answer {
	return x.do(t, func(ctx context.Context, tx db.Tx) (reserve.Answer, error) {
		return reserve.Consume(ctx, tx, x.kind, id, consumption, consumption+"/"+string(rune('0'+qty)),
			func(ctx context.Context, tx db.Tx, b reserve.Branch) (bool, bool, error) {
				var rest int
				if err := tx.QueryRow(ctx, `SELECT (quantity - consumed)::int FROM line WHERE branch_id = $1`, b.ID).Scan(&rest); err != nil {
					return false, false, err
				}
				if qty > rest {
					return false, true, nil
				}
				if _, err := tx.Exec(ctx, `UPDATE line SET consumed = consumed + $2 WHERE branch_id = $1`, b.ID, qty); err != nil {
					return false, false, err
				}
				_, err := tx.Exec(ctx, `UPDATE item SET on_hand = on_hand - $2, reserved = reserved - $2
					FROM line l WHERE l.branch_id = $1 AND item.id = l.item`, b.ID, qty)
				return qty < rest, false, err
			})
	})
}

func (x *demo) available(t testing.TB, item string) (onHand, reserved int) {
	require.NoError(t, x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `SELECT on_hand::int, reserved::int FROM item WHERE id = $1`, item).Scan(&onHand, &reserved)
	}))
	return
}

func (x *demo) events(t testing.TB, id string) []string {
	var out []string
	require.NoError(t, x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `SELECT state || '@' || version FROM published WHERE branch_id = $1 ORDER BY at, version`, id)
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	}))
	return out
}

// TestABranchsLife: hold, the same hold again, other content, confirm twice, consume within, beyond and exactly, a
// consumption ID again with the same and other content, and the ended branch's answers; an event a change, none for
// what changes nothing (P16.2-P16.7).
func TestABranchsLife(t *testing.T) {
	x := newDemo(t, "reserve_life")
	x.stock(t, "tea", 10)
	id := ids.New().String()

	a := x.hold(t, id, "tea", 4)
	require.Equal(t, reserve.Held, a.Branch.State)
	require.NotNil(t, a.Branch.HoldExpiresAt)
	assert.WithinDuration(t, time.Now().Add(5*time.Minute), *a.Branch.HoldExpiresAt, time.Minute)
	same := x.hold(t, id, "tea", 4)
	assert.Equal(t, reserve.Held, same.Branch.State, "the same hold again: as it is")
	assert.True(t, same.Repeated)
	assert.True(t, x.hold(t, id, "tea", 5).Conflict, "other content")
	_, reserved := x.available(t, "tea")
	assert.Equal(t, 4, reserved, "taken once")

	assert.Equal(t, reserve.NotCommitted, x.consume(t, id, "c0", 1).Refused, "a held branch is not consumed")
	assert.Equal(t, reserve.Committed, x.confirm(t, id).Branch.State)
	assert.NotNil(t, x.query(t, id).Branch.ConfirmedAt)
	assert.Equal(t, reserve.Committed, x.confirm(t, id).Branch.State, "confirming again")

	c1 := x.consume(t, id, "c1", 1)
	assert.Equal(t, reserve.Committed, c1.Branch.State, "something is left")
	assert.True(t, x.consume(t, id, "c1", 2).Conflict, "a consumption ID with other content")
	again := x.consume(t, id, "c1", 1)
	assert.True(t, again.Repeated)
	assert.Equal(t, c1.Branch.Version, again.Branch.Version, "the same consumption again: nothing more")
	assert.Equal(t, reserve.BeyondRemaining, x.consume(t, id, "c2", 4).Refused)
	done := x.consume(t, id, "c2", 3)
	assert.Equal(t, reserve.Consumed, done.Branch.State)
	onHand, reserved := x.available(t, "tea")
	assert.Equal(t, [2]int{6, 0}, [2]int{onHand, reserved})

	assert.Equal(t, reserve.Consumed, x.confirm(t, id).Branch.State)
	assert.Equal(t, reserve.Consumed, x.cancel(t, id).Branch.State)
	assert.Equal(t, reserve.NotCommitted, x.consume(t, id, "c3", 1).Refused)
	assert.Equal(t, []string{"held@1", "committed@2", "committed@3", "consumed@4"}, x.events(t, id))
}

// TestTombstonesAndTheEnded: a cancel before the hold leaves a tombstone the late hold answers with; a query leaves
// none; a cancel gives back what was not consumed; ended branches answer as they are (P16.3-P16.6).
func TestTombstonesAndTheEnded(t *testing.T) {
	x := newDemo(t, "reserve_tomb")
	x.stock(t, "rice", 5)
	late := ids.New().String()
	assert.Equal(t, reserve.Cancelled, x.cancel(t, late).Branch.State)
	assert.Equal(t, reserve.Cancelled, x.hold(t, late, "rice", 1).Branch.State, "RS04")
	assert.Equal(t, reserve.Cancelled, x.hold(t, late, "rice", 2).Branch.State, "whatever the content")
	assert.Equal(t, reserve.Cancelled, x.confirm(t, late).Branch.State)
	assert.Equal(t, []string{"cancelled@1"}, x.events(t, late))

	fresh := ids.New().String()
	assert.Equal(t, reserve.Absent, x.query(t, fresh).Branch.State)
	assert.Equal(t, reserve.Absent, x.confirm(t, fresh).Branch.State)
	assert.Equal(t, reserve.NoBranch, x.consume(t, fresh, "n1", 1).Refused)
	assert.Equal(t, reserve.Held, x.hold(t, fresh, "rice", 2).Branch.State, "no tombstone was left")
	x.confirm(t, fresh)
	x.consume(t, fresh, "n2", 1)
	assert.Equal(t, reserve.Released, x.cancel(t, fresh).Branch.State)
	onHand, reserved := x.available(t, "rice")
	assert.Equal(t, [2]int{4, 0}, [2]int{onHand, reserved}, "the unconsumed 1 came back")
	assert.Equal(t, reserve.Released, x.confirm(t, fresh).Branch.State, "not revived")
	assert.Equal(t, reserve.Released, x.cancel(t, fresh).Branch.State)

	short := ids.New().String()
	r := x.hold(t, short, "rice", 9)
	assert.Equal(t, reserve.Rejected, r.Branch.State)
	assert.Equal(t, "insufficient", r.Branch.Reason)
	assert.Nil(t, r.Branch.HoldExpiresAt)
	assert.Equal(t, reserve.Rejected, x.cancel(t, short).Branch.State)
}

// TestExpiry: a hold past its period expires at its confirm, given back, and by the job; the job leaves the others
// (P16.4, P16.8).
func TestExpiry(t *testing.T) {
	x := newDemo(t, "reserve_expiry")
	x.stock(t, "salt", 10)
	past := func(id string) {
		require.NoError(t, x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE be_reserve_branch SET hold_expires_at = now() - interval '1 second' WHERE id = $1`, id)
			return err
		}))
	}
	atConfirm, byJob, kept, committed := ids.New().String(), ids.New().String(), ids.New().String(), ids.New().String()
	for _, id := range []string{atConfirm, byJob, kept, committed} {
		x.hold(t, id, "salt", 2)
	}
	x.confirm(t, committed)
	past(atConfirm)
	past(byJob)
	assert.Equal(t, reserve.Expired, x.confirm(t, atConfirm).Branch.State)
	require.NoError(t, reserve.ExpireDue(context.Background(), x.d, x.kind))
	assert.Equal(t, reserve.Expired, x.query(t, byJob).Branch.State)
	assert.Equal(t, reserve.Held, x.query(t, kept).Branch.State)
	assert.Equal(t, reserve.Committed, x.query(t, committed).Branch.State, "a committed branch never expires (RS10)")
	_, reserved := x.available(t, "salt")
	assert.Equal(t, 4, reserved, "the two expired gave back")
}

// TestNoOversellAndNoDeadlock: holds racing for the last items take exactly what there is (RS01); consumptions and
// cancels of the same branches at once wait in the lock order, never on each other (P16.11).
func TestNoOversellAndNoDeadlock(t *testing.T) {
	x := newDemo(t, "reserve_race")
	x.stock(t, "pen", 5)
	var wg sync.WaitGroup
	var mu sync.Mutex
	held := 0
	var heldIDs []string
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := ids.New().String()
			if a := x.hold(t, id, "pen", 1); a.Branch.State == reserve.Held {
				mu.Lock()
				held++
				heldIDs = append(heldIDs, id)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, 5, held)
	for _, id := range heldIDs {
		x.confirm(t, id)
	}
	errs := make(chan error, 2*len(heldIDs))
	for i, id := range heldIDs {
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs <- x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
				_, err := reserve.Cancel(ctx, tx, x.kind, id)
				return err
			})
		}()
		go func() {
			defer wg.Done()
			errs <- x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
				if err := x.kind.Lock(ctx, tx, id); err != nil { // a consumer locks the resource first, as the rule says
					return err
				}
				_, err := reserve.Consume(ctx, tx, x.kind, id, "race-"+string(rune('a'+i)), "1",
					func(ctx context.Context, tx db.Tx, b reserve.Branch) (bool, bool, error) {
						_, err := tx.Exec(ctx, `UPDATE line SET consumed = consumed + 1 WHERE branch_id = $1`, b.ID)
						if err == nil {
							_, err = tx.Exec(ctx, `UPDATE item SET on_hand = on_hand - 1, reserved = reserved - 1 WHERE id = 'pen'`)
						}
						return false, false, err
					})
				return err
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err, "no deadlock, no failure")
	}
	onHand, reserved := x.available(t, "pen")
	assert.Equal(t, 0, reserved, "each branch was consumed or released, nothing left reserved")
	assert.LessOrEqual(t, onHand, 5)
}

// TestAnotherKindsBranch: a branch ID stored under another kind is not this participation's.
func TestAnotherKindsBranch(t *testing.T) {
	x := newDemo(t, "reserve_kind")
	x.stock(t, "ink", 3)
	id := ids.New().String()
	x.hold(t, id, "ink", 1)
	other := x.kind
	other.Name = "demo.other"
	err := x.d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := reserve.Query(ctx, tx, other, id)
		return err
	})
	assert.True(t, errors.Is(err, reserve.ErrKind), err)
}

// BenchmarkHoldAndConfirm: a hold and its confirm, each in its own transaction, against the demo participant: what
// the runtime adds on the protocol's hot path (be-protocol chapter 16; a measurement, not a gate).
func BenchmarkHoldAndConfirm(b *testing.B) {
	x := newDemo(b, "reserve_bench")
	x.stock(b, "bench", 1<<30)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := ids.New().String()
		x.hold(b, id, "bench", 1)
		x.confirm(b, id)
	}
}
