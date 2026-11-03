//go:build integration

package events

import (
	"context"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ReadAfter (P8.28's Ended reads the identity member's outbox this way): rows after a cursor in position order,
// pages, the end, and every case where the reader may have missed rows answering reset.
func TestReadAfterPagesByPositionAndResetsWhenRowsMayBeMissed(t *testing.T) {
	ctx := context.Background()
	d, admin := source(t, "evreadafter")
	single := Stream{Name: "demo.stock", Partitions: 1}
	p, err := NewProducer("demo/evreadafter", []Stream{single}, nil)
	require.NoError(t, err)
	read := func(after *Cursor, limit int) ([]Row, Cursor, bool) {
		rows, next, reset, err := ReadAfter(ctx, d, "impl-a", "demo.stock", 0, after, limit)
		require.NoError(t, err)
		return rows, next, reset
	}

	rows, start, reset := read(nil, 10)
	assert.True(t, reset, "no cursor: start from the end")
	assert.Empty(t, rows)
	assert.EqualValues(t, 0, start.Seq)

	emit(t, d, p, 3)
	_, err = Number(ctx, d, "demo.stock", 0, 100)
	require.NoError(t, err)
	rows, next, reset := read(&start, 2)
	require.False(t, reset)
	require.Len(t, rows, 2, "a page of the limit")
	assert.EqualValues(t, []int64{1, 2}, []int64{rows[0].Seq, rows[1].Seq})
	assert.EqualValues(t, 2, next.Seq)
	rows, next, reset = read(&next, 2)
	require.False(t, reset)
	require.Len(t, rows, 1)
	assert.EqualValues(t, 3, rows[0].Seq)
	rows, atEnd, reset := read(&next, 2)
	assert.False(t, reset, "at the end: nothing new, no reset")
	assert.Empty(t, rows)
	assert.Equal(t, next, atEnd)

	other := next
	other.Impl = "impl-b"
	_, _, reset = read(&other, 2)
	assert.True(t, reset, "another implementation instance")
	ahead := next
	ahead.Seq = 9
	_, end, reset := read(&ahead, 2)
	assert.True(t, reset, "past the end: the database went back")
	assert.EqualValues(t, 3, end.Seq)

	emit(t, d, p, 2)
	_, err = Number(ctx, d, "demo.stock", 0, 100)
	require.NoError(t, err)
	pgtest.Exec(t, admin, "DELETE FROM be_outbox WHERE seq = 4") // purged
	_, end, reset = read(&next, 2)
	assert.True(t, reset, "the row after the cursor was purged")
	assert.EqualValues(t, 5, end.Seq)

	pgtest.Exec(t, admin, "UPDATE be_recovery_generation SET generation = generation + 1")
	_, end, reset = read(&atEnd, 2)
	assert.True(t, reset, "another epoch")
	assert.NotEqual(t, atEnd.Epoch, end.Epoch)
	assert.EqualValues(t, 0, end.Seq, "nothing numbered in the new epoch")
	emit(t, d, p, 5)
	_, err = Number(ctx, d, "demo.stock", 0, 100)
	require.NoError(t, err)
	_, end, reset = read(&atEnd, 2)
	assert.True(t, reset, "a cursor of the old epoch is not a position in the new one, even where that has rows")
	assert.EqualValues(t, 5, end.Seq)
}
