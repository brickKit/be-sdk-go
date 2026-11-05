package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/jackc/pgx/v5"
)

// Series is a document number series (protocol 14, "document numbers"): numbers count per legal entity, per series
// code and per period (empty when the series never restarts).
type Series struct {
	LegalEntity, Code, Period string
}

func (s Series) key() string { return strings.Join([]string{s.LegalEntity, s.Code, s.Period}, "/") }

// NextNumber takes the next number of a continuous series in the issuing transaction: the series row stays locked
// until the transaction ends, and a rollback returns the number, so no gap is left. Take it after the idempotency
// claim, never for drafts.
func NextNumber(ctx context.Context, tx Tx, s Series) (int64, error) { return next(ctx, tx, s, true) }

// NextGappedNumber takes a number of a series that allows gaps, in a transaction of its own: the row lock is held
// only for that moment, and a number is never reused.
func (d *DB) NextGappedNumber(ctx context.Context, s Series) (n int64, err error) {
	err = d.Tx(ctx, func(ctx context.Context, tx Tx) error {
		n, err = next(ctx, tx, s, false)
		return err
	})
	return n, err
}

// RegisterImported records imported historical numbers: new numbers continue after last (protocol 16).
func RegisterImported(ctx context.Context, tx Tx, s Series, gapless bool, last int64) error {
	var stored bool
	err := tx.QueryRow(ctx, `INSERT INTO be_doc_series (series, last_number, gapless) VALUES ($1, $2, $3)
		ON CONFLICT (series) DO UPDATE SET last_number = greatest(be_doc_series.last_number, excluded.last_number)
		RETURNING gapless`, s.key(), last, gapless).Scan(&stored)
	if err != nil {
		return err
	}
	return sameKind(s, gapless, stored)
}

func next(ctx context.Context, tx Tx, s Series, gapless bool) (int64, error) {
	var n int64
	var stored bool
	if err := tx.QueryRow(ctx, `INSERT INTO be_doc_series (series, last_number, gapless) VALUES ($1, 1, $2)
		ON CONFLICT (series) DO UPDATE SET last_number = be_doc_series.last_number + 1
		RETURNING last_number, gapless`, s.key(), gapless).Scan(&n, &stored); err != nil {
		return 0, err
	}
	return n, sameKind(s, gapless, stored) // the error rolls the transaction back, increment included
}

func sameKind(s Series, want, stored bool) error {
	if want == stored {
		return nil
	}
	return errs.New(errs.ReasonInternal, "").Wrap(fmt.Errorf("series %s is declared gapless=%t but used as gapless=%t", s.key(), stored, want))
}

// UsedMax is a continuous series and the largest number it has used (be-protocol P14.5).
type UsedMax struct {
	Series
	Max int64
}

// ContinuousMaxima lists every continuous series with its used maximum, ordered by key. The key is split on its
// first two slashes: a legal entity or a series code with a slash is not told apart.
func ContinuousMaxima(ctx context.Context, tx Tx) ([]UsedMax, error) {
	rows, err := tx.Query(ctx, "SELECT series, last_number FROM be_doc_series WHERE gapless ORDER BY series")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UsedMax
	for rows.Next() {
		var key string
		var u UsedMax
		if err := rows.Scan(&key, &u.Max); err != nil {
			return nil, err
		}
		parts := strings.SplitN(key, "/", 3)
		for len(parts) < 3 {
			parts = append(parts, "")
		}
		u.Series = Series{LegalEntity: parts[0], Code: parts[1], Period: parts[2]}
		out = append(out, u)
	}
	return out, rows.Err()
}

// RaiseContinuous raises a continuous series' used maximum to to (P14.5, P5.20: never lowered; a series not there is
// created) and answers the maximum before, so the owner voids the numbers between. A series that allows gaps answers
// REQUEST_INVALID.
func RaiseContinuous(ctx context.Context, tx Tx, s Series, to int64) (before int64, err error) {
	var gapless bool
	err = tx.QueryRow(ctx, "SELECT last_number, gapless FROM be_doc_series WHERE series = $1 FOR UPDATE", s.key()).Scan(&before, &gapless)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		before, gapless = 0, true
	case err != nil:
		return 0, err
	}
	if !gapless {
		return 0, errs.New(errs.ReasonRequestInvalid, "series "+s.key()+" allows gaps: it is not raised")
	}
	if to <= before {
		return before, nil
	}
	_, err = tx.Exec(ctx, `INSERT INTO be_doc_series (series, last_number, gapless) VALUES ($1, $2, true)
		ON CONFLICT (series) DO UPDATE SET last_number = excluded.last_number`, s.key(), to)
	return before, err
}
