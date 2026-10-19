package audit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// Transfer parameters (protocol 03).
const (
	Batch         = 500
	TransferEvery = 5 * time.Second
)

// Wire is one record as it is sent to the audit component: the canonical bytes stored with the row (P8.21).
type Wire struct {
	ID     string
	Seq    int64
	Epoch  string // <generation>.<timeline>
	Digest string
	Record []byte
}

// Receipt is the audit component's answer for one record.
type Receipt struct {
	ID     string
	Status string // received; anything else (conflict, invalid, unknown) stops the record
	Epoch  string // the audit database's epoch
	Seq    int64  // its storage sequence
	Chain  string // the source chain digest
}

// Sender delivers records to the audit component, idempotent by ID.
type Sender interface {
	Send(ctx context.Context, records []Wire) ([]Receipt, error)
}

var errNoEpoch = errors.New("audit: the database epoch cannot be read; numbering refused")

// Number gives committed rows their production sequence under the counter's lock, in insertion order, with the
// database epoch (P8.20; the same algorithm as the outbox, P6.5).
func (a *Auditor) Number(ctx context.Context) (int, error) {
	var n int
	err := a.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var gen, tl *int64
		var last int64
		if err := tx.QueryRow(ctx, "SELECT epoch_generation, epoch_timeline, last_seq FROM be_audit_counter FOR UPDATE").
			Scan(&gen, &tl, &last); err != nil {
			return err
		}
		var g, t int64
		if err := tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").Scan(&g, &t); err != nil {
			return errors.Join(errNoEpoch, err)
		}
		if gen == nil || *gen != g || *tl != t {
			last = 0
		}
		tag, err := tx.Exec(ctx, `WITH picked AS (
				SELECT ord, row_number() OVER (ORDER BY ord) AS rn FROM (
					SELECT ord FROM be_audit_log WHERE seq IS NULL ORDER BY ord LIMIT $1) p)
			UPDATE be_audit_log l SET seq = $2 + picked.rn, epoch_generation = $3, epoch_timeline = $4
			FROM picked WHERE l.ord = picked.ord`, Batch, last, g, t)
		if err != nil {
			return err
		}
		n = int(tag.RowsAffected())
		_, err = tx.Exec(ctx, "UPDATE be_audit_counter SET last_seq = $1, epoch_generation = $2, epoch_timeline = $3",
			last+int64(n), g, t)
		return err
	}, db.LockTimeout(2*time.Second), db.TransactionTimeout(5*time.Second))
	return n, err
}

// Transfer sends one batch of numbered pending rows and stores the receipts (P8.21); it returns how many were
// settled. A conflict stops that record only; nothing is retried automatically after a conflict.
func (a *Auditor) Transfer(ctx context.Context, s Sender) (int, error) {
	var batch []Wire
	err := a.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		batch = batch[:0]
		rows, err := tx.Query(ctx, `SELECT id::text, seq, epoch_generation, epoch_timeline, digest, record
			FROM be_audit_log WHERE transfer_state = 'pending' AND seq IS NOT NULL AND record IS NOT NULL
			ORDER BY ord LIMIT $1`, Batch)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var w Wire
			var g, t int64
			if err := rows.Scan(&w.ID, &w.Seq, &g, &t, &w.Digest, &w.Record); err != nil {
				return err
			}
			w.Epoch = strconv.FormatInt(g, 10) + "." + strconv.FormatInt(t, 10)
			batch = append(batch, w)
		}
		return rows.Err()
	})
	if err != nil || len(batch) == 0 {
		return 0, err
	}
	receipts, err := s.Send(ctx, batch)
	if err != nil {
		return 0, fmt.Errorf("sending audit records: %w", err) // they stay pending and are sent again (idempotent by ID)
	}
	err = a.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		for _, r := range receipts {
			state := "conflict" // conflict, invalid or a status this SDK does not know: never taken as received (P8.21)
			if r.Status == "received" {
				state = "received"
			}
			if _, err := tx.Exec(ctx, `UPDATE be_audit_log SET transfer_state = $2, receipt_epoch = $3, receipt_seq = $4,
				receipt_chain = $5, received_at = now() WHERE id = $1 AND transfer_state = 'pending'`,
				r.ID, state, r.Epoch, r.Seq, r.Chain); err != nil {
				return err
			}
		}
		return nil
	})
	return len(receipts), err
}

// Backlog counts the rows not yet received and refreshes the capacity check (P8.23).
func (a *Auditor) Backlog(ctx context.Context) (int64, error) {
	var n int64
	err := a.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT count(*) FROM be_audit_log WHERE transfer_state = 'pending'").Scan(&n)
	})
	if err == nil {
		a.backlog.Store(n)
	}
	return n, err
}

// Watermark is the audit component's backed-up watermark: the highest storage sequence in a backup, with its epoch.
// Ancestors maps each older epoch on the current history to its fork point.
type Watermark struct {
	Epoch     string
	Seq       int64
	Ancestors map[string]int64
	// BackedUpInEpoch says at least one backup completed in Epoch (needed to delete records of older epochs).
	BackedUpInEpoch bool
}

// Purge deletes received rows whose receipts the watermark covers and that are older than the minimum retention
// (P8.22). Receipts on a lost branch or past an ancestor's fork point go back to pending to be sent again.
func (a *Auditor) Purge(ctx context.Context, w Watermark) (deleted, resent int64, err error) {
	err = a.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM be_audit_log WHERE transfer_state = 'received' AND received_at < now() - make_interval(secs => $3)
			AND receipt_epoch = $1 AND receipt_seq <= $2`, w.Epoch, w.Seq, MinRetention.Seconds())
		if err != nil {
			return err
		}
		deleted = tag.RowsAffected()
		known := []string{w.Epoch}
		for epoch, fork := range w.Ancestors {
			known = append(known, epoch)
			if w.BackedUpInEpoch {
				t, err := tx.Exec(ctx, `DELETE FROM be_audit_log WHERE transfer_state = 'received'
					AND received_at < now() - make_interval(secs => $3) AND receipt_epoch = $1 AND receipt_seq <= $2`,
					epoch, fork, MinRetention.Seconds())
				if err != nil {
					return err
				}
				deleted += t.RowsAffected()
			}
			t, err := tx.Exec(ctx, `UPDATE be_audit_log SET transfer_state = 'pending', receipt_epoch = NULL, receipt_seq = NULL,
				receipt_chain = NULL, received_at = NULL WHERE transfer_state = 'received' AND receipt_epoch = $1 AND receipt_seq > $2`,
				epoch, fork)
			if err != nil {
				return err
			}
			resent += t.RowsAffected()
		}
		// receipts of an epoch that is neither current nor an ancestor are on a lost branch: send them again
		t, err := tx.Exec(ctx, `UPDATE be_audit_log SET transfer_state = 'pending', receipt_epoch = NULL, receipt_seq = NULL,
			receipt_chain = NULL, received_at = NULL WHERE transfer_state = 'received' AND NOT (receipt_epoch = ANY($1))
			AND receipt_epoch IS NOT NULL
			AND NOT ((split_part(receipt_epoch, '.', 1)::bigint, split_part(receipt_epoch, '.', 2)::bigint) >
				(split_part($2, '.', 1)::bigint, split_part($2, '.', 2)::bigint))`, known, w.Epoch)
		if err != nil {
			return err
		}
		resent += t.RowsAffected()
		return nil
	})
	return deleted, resent, err
}
