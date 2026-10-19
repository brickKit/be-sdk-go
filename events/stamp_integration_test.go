//go:build integration

package events

import (
	"context"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadStampDescribesTheReadsSnapshot(t *testing.T) { // protocol 02, S3
	d, admin := source(t, "evstamp")
	p, err := NewProducer("demo/evstamp", []Stream{stock}, nil)
	require.NoError(t, err)
	stamp := func() Stamp {
		var st Stamp
		require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
			st, err = ReadStamp(ctx, tx, "impl-1", stock)
			return err
		}, db.Snapshot()))
		return st
	}
	st := stamp()
	assert.Equal(t, "impl-1", st.Instance)
	assert.Equal(t, []int64{0, 0}, st.Positions, "nothing numbered yet")
	assert.Positive(t, st.Epoch.Generation)

	emit(t, d, p, 6)
	for part := 0; part < stock.Partitions; part++ {
		_, err := Number(context.Background(), d, stock.Name, part, 100)
		require.NoError(t, err)
	}
	st = stamp()
	for part := 0; part < stock.Partitions; part++ {
		got := positions(t, admin, part)
		want := int64(0)
		if len(got) > 0 {
			want = got[len(got)-1]
		}
		assert.Equal(t, want, st.Positions[part], "partition %d: the highest numbered position", part)
	}
	assert.Equal(t, int64(6), st.Positions[0]+st.Positions[1])

	// inside one snapshot transaction, positions numbered after it began are not seen
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		before, err := ReadStamp(ctx, tx, "impl-1", stock)
		if err != nil {
			return err
		}
		emit(t, d, p, 4)
		for part := 0; part < stock.Partitions; part++ {
			if _, err := Number(context.Background(), d, stock.Name, part, 100); err != nil {
				return err
			}
		}
		after, err := ReadStamp(ctx, tx, "impl-1", stock)
		assert.Equal(t, before.Positions, after.Positions, "one snapshot")
		return err
	}, db.Snapshot()))
	assert.Equal(t, int64(10), func() int64 { s := stamp(); return s.Positions[0] + s.Positions[1] }())
}
