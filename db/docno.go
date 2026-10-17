package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/errs"
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
