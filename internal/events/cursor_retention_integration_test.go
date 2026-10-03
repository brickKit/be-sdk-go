package events

import (
	"context"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/stretchr/testify/require"
)

// P12.15: cursor rows unseen for 30 days are deleted, in bounded batches; fresh ones stay.
func TestIntegrationDeleteStaleCursors(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, err := e.super.Exec(`INSERT INTO ` + e.id.Schema + `.besdk_event_cursor (consumer, aggregate_type, aggregate_id, version, event_id, seen_at)
	  SELECT '', 'a.b.c', 'old-' || g, 1, 'e', now() - interval '31 days' FROM generate_series(1, 5) g
	  UNION ALL SELECT '', 'a.b.c', 'new', 1, 'e', now() - interval '29 days'`)
	require.NoError(t, err)
	before := time.Now().Add(-CursorRetention)
	var total int64
	for {
		var n int64
		require.NoError(t, e.store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
			var err error
			n, err = DeleteStaleCursors(ctx, tx, before, 2)
			return err
		}))
		total += n
		if n < 2 {
			break
		}
	}
	require.EqualValues(t, 5, total)
	var left int
	require.NoError(t, e.super.QueryRow(`SELECT count(*) FROM `+e.id.Schema+`.besdk_event_cursor`).Scan(&left))
	require.Equal(t, 1, left)
}
