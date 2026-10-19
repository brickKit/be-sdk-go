package list

import (
	"context"
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/jackc/pgx/v5"
)

// Page runs one compiled page in its own transaction: SELECT cols FROM table AS the spec's alias WHERE q.Where ORDER
// BY q.OrderBy, one row more than the page to know whether another follows. scan reads a row; keys gives a row's
// sort values (as text, in q.SortFields order) and its ID for the next cursor, which is empty on the last page.
// Every field of the rows must be readable by the caller wherever a row is listed (ordinary fields): a list with
// restricted fields selects q.Hits and masks per row instead.
func Page[T any](ctx context.Context, d *db.DB, q *Query, table, cols string, scan func(pgx.Row) (T, error),
	keys func(T) ([]string, string)) ([]T, string, error) {
	out := []T{}
	var more bool
	err := d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		out, more = out[:0], false
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT %s FROM %s AS %s WHERE %s ORDER BY %s LIMIT %d", cols, table,
			pgx.Identifier{q.spec.Alias}.Sanitize(), q.Where, q.OrderBy, q.Limit+1), q.Args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if len(out) == q.Limit {
				more = true
				break
			}
			v, err := scan(rows)
			if err != nil {
				return err
			}
			out = append(out, v)
		}
		return rows.Err()
	})
	if err != nil || !more {
		return out, "", err
	}
	k, id := keys(out[len(out)-1])
	return out, q.Next(k, id), nil
}
