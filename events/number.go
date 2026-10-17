package events

import (
	"context"
	"errors"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// Limits of the numbering transaction (P6.5).
const (
	NumberLockWait = 2 * time.Second
	NumberTxLimit  = 5 * time.Second
)

// ErrNoEpoch is returned when the epoch cannot be read (a standby, or no recovery generation): numbering is refused.
var ErrNoEpoch = errors.New("events: the database epoch cannot be read; numbering refused")

// Number gives the visible, unnumbered rows of one partition their publish positions (P6.5-P6.8): lock the counter,
// read the epoch, start a new epoch with a history entry when it changed, then number in insertion order with a fresh
// statement snapshot. It returns how many rows were numbered.
func Number(ctx context.Context, d *db.DB, stream string, partition, batch int) (int, error) {
	var n int
	err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		n = 0
		if _, err := tx.Exec(ctx, `INSERT INTO be_outbox_partition (stream, partition) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, stream, partition); err != nil {
			return err
		}
		var gen, tl *int64
		var last int64
		if err := tx.QueryRow(ctx, `SELECT epoch_generation, epoch_timeline, last_seq FROM be_outbox_partition
			WHERE stream = $1 AND partition = $2 FOR UPDATE`, stream, partition).Scan(&gen, &tl, &last); err != nil {
			return err
		}
		var now Epoch
		if err := tx.QueryRow(ctx, `SELECT generation, timeline FROM be_current_db_epoch()`).
			Scan(&now.Generation, &now.Timeline); err != nil {
			return errors.Join(ErrNoEpoch, err)
		}
		if gen == nil || *gen != now.Generation || *tl != now.Timeline {
			if gen != nil {
				if _, err := tx.Exec(ctx, `INSERT INTO be_epoch_history (stream, partition, old_generation, old_timeline,
					fork_seq, new_generation, new_timeline) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
					stream, partition, *gen, *tl, last, now.Generation, now.Timeline); err != nil {
					return err
				}
			}
			last = 0
		}
		// a new statement, so a new snapshot taken after the lock was granted (READ COMMITTED)
		tag, err := tx.Exec(ctx, `WITH picked AS (
				SELECT ord, row_number() OVER (ORDER BY ord) AS rn FROM (
					SELECT ord FROM be_outbox WHERE stream = $1 AND partition = $2 AND seq IS NULL ORDER BY ord LIMIT $3
				) p
			)
			UPDATE be_outbox o SET seq = $4 + picked.rn, epoch_generation = $5, epoch_timeline = $6
			FROM picked WHERE o.ord = picked.ord`, stream, partition, batch, last, now.Generation, now.Timeline)
		if err != nil {
			return err
		}
		n = int(tag.RowsAffected())
		_, err = tx.Exec(ctx, `UPDATE be_outbox_partition SET last_seq = $3, epoch_generation = $4, epoch_timeline = $5
			WHERE stream = $1 AND partition = $2`, stream, partition, last+int64(n), now.Generation, now.Timeline)
		return err
	}, db.LockTimeout(NumberLockWait), db.TransactionTimeout(NumberTxLimit))
	return n, err
}
