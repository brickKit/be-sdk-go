package db

import "context"

// Batch bounds one delete of DeleteInBatches, so a cleanup never holds many row locks or outlasts the statement
// timeout however large the backlog.
const Batch = 1000

// DeleteInBatches runs a delete statement whose last parameter is the batch size (Batch), each batch in its own
// background transaction, until a batch deletes fewer rows; it returns the total.
func DeleteInBatches(ctx context.Context, d *DB, sql string, args ...any) (int64, error) {
	var total int64
	for {
		var n int64
		if err := d.BackgroundTx(ctx, func(ctx context.Context, tx Tx) error {
			tag, err := tx.Exec(ctx, sql, append(args, Batch)...)
			n = tag.RowsAffected()
			return err
		}); err != nil {
			return total, err
		}
		total += n
		if n < Batch {
			return total, nil
		}
	}
}
