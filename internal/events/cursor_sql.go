package events

import (
	"context"
	"database/sql"
	"errors"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
)

// upsertCursor is the normative statement of ddl/03-event-cursor.sql (P12.6): no row returned
// means a duplicate or an older version.
const upsertCursor = `INSERT INTO besdk_event_cursor (consumer, aggregate_type, aggregate_id, version, event_id)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (consumer, aggregate_type, aggregate_id) DO UPDATE
   SET version = EXCLUDED.version, event_id = EXCLUDED.event_id, seen_at = now()
 WHERE besdk_event_cursor.version < EXCLUDED.version
RETURNING 1`

const readCursor = `SELECT version FROM besdk_event_cursor
 WHERE consumer = $1 AND aggregate_type = $2 AND aggregate_id = $3`

// pgCursors is besdk_event_cursor through the member's Store.
type pgCursors struct{ store *pg.Store }

func upsert(ctx context.Context, tx *pg.Tx, k cursorKey, ev envelope.Event) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, upsertCursor, k.consumer, k.aggregateType, k.aggregateID, ev.Version, ev.ID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (c pgCursors) apply(ctx context.Context, k cursorKey, ev envelope.Event, fn func(context.Context, *pg.Tx) error) (bool, error) {
	var applied bool
	err := c.store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		ok, err := upsert(ctx, tx, k, ev)
		applied = ok
		if err != nil || !ok {
			return err
		}
		return fn(ctx, tx)
	})
	return applied && err == nil, err
}

func (c pgCursors) current(ctx context.Context, k cursorKey) (int64, bool, error) {
	var v int64
	err := c.store.Run(ctx, pg.TxOptions{ReadOnly: true}, func(ctx context.Context, tx *pg.Tx) error {
		return tx.QueryRowContext(ctx, readCursor, k.consumer, k.aggregateType, k.aggregateID).Scan(&v)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return v, err == nil, err
}

func (c pgCursors) advance(ctx context.Context, k cursorKey, ev envelope.Event) error {
	return c.store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		_, err := upsert(ctx, tx, k, ev) // no row: a newer version got there first, fine
		return err
	})
}
