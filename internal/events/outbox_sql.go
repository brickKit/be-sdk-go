package events

import (
	"context"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// claimOutbox is the normative claim of ddl/02-outbox.sql (P12.1, P10.9), returning the columns
// the pump needs.
const claimOutbox = `UPDATE besdk_outbox SET status = 'SENDING', claimed_until = now() + interval '30 seconds',
                        attempts = attempts + 1
 WHERE (id, created_at) IN (
       SELECT id, created_at FROM besdk_outbox
        WHERE (status = 'PENDING' AND next_attempt_at <= now())
           OR (status = 'SENDING' AND claimed_until < now())
        ORDER BY created_at, id
        LIMIT 256
        FOR UPDATE SKIP LOCKED)
RETURNING id::text, created_at, subject, aggregate_type, aggregate_id, aggregate_version, occurred_at,
          traceparent, causation_id, hop_count, headers::text, payload::text, attempts`

const markPublished = `UPDATE besdk_outbox SET status = 'PUBLISHED', published_at = now(), claimed_until = NULL
 WHERE (id, created_at) IN (SELECT * FROM unnest($1::uuid[], $2::timestamptz[]))`

// markFailed only touches rows still SENDING: a row another pump re-claimed and published stays
// PUBLISHED.
const markFailed = `UPDATE besdk_outbox o SET status = 'PENDING',
       next_attempt_at = now() + f.delay_ms * interval '1 millisecond', claimed_until = NULL, last_error = f.err
  FROM unnest($1::uuid[], $2::timestamptz[], $3::bigint[], $4::text[]) AS f(id, created_at, delay_ms, err)
 WHERE o.id = f.id AND o.created_at = f.created_at AND o.status = 'SENDING'`

const outboxStats = `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8
  FROM besdk_outbox WHERE status <> 'PUBLISHED'`

// pgOutbox is besdk_outbox through the member's Store: every call is one short transaction.
type pgOutbox struct{ store *pg.Store }

func (o pgOutbox) claim(ctx context.Context) ([]claimedRow, error) {
	var rows []claimedRow
	err := o.store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		rows = rows[:0]
		res, err := tx.QueryContext(ctx, claimOutbox)
		if err != nil {
			return err
		}
		defer func() { _ = res.Close() }()
		for res.Next() {
			var r claimedRow
			var headers, payload string
			if err := res.Scan(&r.id, &r.createdAt, &r.subject, &r.aggregateType, &r.aggregateID,
				&r.aggregateVersion, &r.occurredAt, &r.traceParent, &r.causationID, &r.hopCount,
				&headers, &payload, &r.attempts); err != nil {
				return err
			}
			r.headers, r.payload = []byte(headers), []byte(payload)
			r.createdAt, r.occurredAt = r.createdAt.UTC(), r.occurredAt.UTC()
			rows = append(rows, r)
		}
		return res.Err()
	})
	return rows, err
}

func (o pgOutbox) markPublished(ctx context.Context, rows []claimedRow) error {
	if len(rows) == 0 {
		return nil
	}
	ids, created := make([]string, len(rows)), make([]time.Time, len(rows))
	for i, r := range rows {
		ids[i], created[i] = r.id, r.createdAt
	}
	return o.store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, markPublished, ids, created)
		return err
	})
}

func (o pgOutbox) markFailed(ctx context.Context, rows []failedRow) error {
	if len(rows) == 0 {
		return nil
	}
	ids, created := make([]string, len(rows)), make([]time.Time, len(rows))
	delays, errs := make([]int64, len(rows)), make([]string, len(rows))
	for i, r := range rows {
		ids[i], created[i], delays[i], errs[i] = r.id, r.createdAt, r.delay.Milliseconds(), r.err
	}
	return o.store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
		_, err := tx.ExecContext(ctx, markFailed, ids, created, delays, errs)
		return err
	})
}

func (o pgOutbox) stats(ctx context.Context) (int64, float64, error) {
	var n int64
	var age float64
	err := o.store.Run(ctx, pg.TxOptions{ReadOnly: true}, func(ctx context.Context, tx *pg.Tx) error {
		return tx.QueryRowContext(ctx, outboxStats).Scan(&n, &age)
	})
	return n, age, err
}
