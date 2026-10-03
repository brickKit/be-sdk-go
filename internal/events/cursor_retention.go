package events

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CursorRetention is how long a cursor row unseen stays (P12.15): 30 days.
const CursorRetention = 30 * 24 * time.Hour

const deleteStaleCursors = `DELETE FROM besdk_event_cursor WHERE (consumer, aggregate_type, aggregate_id) IN (
SELECT consumer, aggregate_type, aggregate_id FROM besdk_event_cursor WHERE seen_at < $1
 ORDER BY seen_at LIMIT $2 FOR UPDATE SKIP LOCKED)`

// execer is what DeleteStaleCursors needs; *pg.Tx satisfies it.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// DeleteStaleCursors deletes at most limit cursor rows last seen before before (P12.15), skipping rows
// another transaction holds, and returns how many it deleted. The runtime's be.cleanup job calls it,
// one transaction per batch, until it returns fewer than limit.
func DeleteStaleCursors(ctx context.Context, tx execer, before time.Time, limit int) (int64, error) {
	res, err := tx.ExecContext(ctx, deleteStaleCursors, before, limit)
	if err != nil {
		return 0, fmt.Errorf("events: delete stale cursors: %w", err)
	}
	return res.RowsAffected()
}
