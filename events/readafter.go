package events

import (
	"context"
	"errors"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// Cursor is a reader's position on one partition of a stream in the outbox: the implementation instance
// (IMPLEMENTATION_INSTANCE), the epoch and the last sequence read.
type Cursor struct {
	Impl  string
	Epoch Epoch
	Seq   int64
}

// ReadAfter reads at most limit numbered rows of one partition after the cursor, in position order, for a reader
// that keeps its own cursor instead of subscribing (the identity member's SessionService/Ended, P8.28). Positions
// are numbered after commit under the partition's lock (P6.5), so reading by position never skips a row that
// committed later. reset is true, with no rows and next at the current end, when the cursor is nil, of another
// instance or epoch, or when the row after it is not there (purged, or the database went back past the cursor): the
// reader may have missed rows and starts again from what it knows.
func ReadAfter(ctx context.Context, d *db.DB, impl, stream string, partition int, after *Cursor, limit int) (rows []Row,
	next Cursor, reset bool, err error) {
	err = d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var current Epoch
		if err := tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").
			Scan(&current.Generation, &current.Timeline); err != nil {
			return errors.Join(ErrNoEpoch, err)
		}
		var counter int64
		var gen, tl *int64
		err := tx.QueryRow(ctx, `SELECT last_seq, epoch_generation, epoch_timeline FROM be_outbox_partition
			WHERE stream = $1 AND partition = $2`, stream, partition).Scan(&counter, &gen, &tl)
		if err != nil && !isNoRows(err) {
			return err
		}
		if gen == nil || *gen != current.Generation || *tl != current.Timeline {
			counter = 0 // nothing numbered in this epoch yet
		}
		end := Cursor{Impl: impl, Epoch: current, Seq: counter}
		if after == nil || after.Impl != impl || after.Epoch != current {
			next, reset = end, true
			return nil
		}
		if after.Seq == counter {
			next = end
			return nil
		}
		var one int
		err = tx.QueryRow(ctx, `SELECT 1 FROM be_outbox WHERE stream = $1 AND partition = $2 AND epoch_generation = $3
			AND epoch_timeline = $4 AND seq = $5`, stream, partition, current.Generation, current.Timeline,
			after.Seq+1).Scan(&one)
		if isNoRows(err) {
			next, reset = end, true // purged, or past the end
			return nil
		}
		if err != nil {
			return err
		}
		rows, err = rowsBetween(ctx, tx, stream, partition, current, after.Seq, counter, limit)
		if err != nil {
			return err
		}
		next = Cursor{Impl: impl, Epoch: current, Seq: after.Seq}
		if len(rows) > 0 {
			next.Seq = rows[len(rows)-1].Seq
		}
		return nil
	})
	return rows, next, reset, err
}
