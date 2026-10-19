package events

import (
	"context"
	"errors"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// Stamp is what a source declares with every read it answers (protocol 02, S3 and step 3): its implementation
// instance, the database epoch and, per partition of the stream, the highest numbered publish position. Read it in
// the same db.Snapshot() transaction as the data, so the positions describe exactly the snapshot the data came from.
type Stamp struct {
	Instance  string
	Epoch     Epoch
	Positions []int64 // index = partition; 0 for a partition that has numbered nothing yet
}

// ReadStamp reads the stamp of a stream inside tx. It fails where numbering would (ErrNoEpoch on a standby or
// without a recovery generation): an answer whose epoch cannot be stated must not be given as a primary's.
func ReadStamp(ctx context.Context, tx db.Tx, instance string, s Stream) (Stamp, error) {
	st := Stamp{Instance: instance, Positions: make([]int64, s.Partitions)}
	if err := tx.QueryRow(ctx, `SELECT generation, timeline FROM be_current_db_epoch()`).
		Scan(&st.Epoch.Generation, &st.Epoch.Timeline); err != nil {
		return Stamp{}, errors.Join(ErrNoEpoch, err)
	}
	rows, err := tx.Query(ctx, `SELECT partition, last_seq, epoch_generation, epoch_timeline FROM be_outbox_partition
		WHERE stream = $1`, s.Name)
	if err != nil {
		return Stamp{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var p int
		var last int64
		var gen, tl *int64
		if err := rows.Scan(&p, &last, &gen, &tl); err != nil {
			return Stamp{}, err
		}
		// a counter of an older epoch numbers nothing in this one yet: the next numbering starts a new epoch at 1
		if p >= 0 && p < s.Partitions && gen != nil && *gen == st.Epoch.Generation && *tl == st.Epoch.Timeline {
			st.Positions[p] = last
		}
	}
	return st, rows.Err()
}
