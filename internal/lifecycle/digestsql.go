package lifecycle

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// quoteIdent quotes an SQL identifier.
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// columnsOf lists a table's live columns in declared order.
func columnsOf(ctx context.Context, q Querier, table string) ([]string, error) {
	return strings1(ctx, q, `SELECT a.attname FROM pg_attribute a
	 WHERE a.attrelid = to_regclass(format('%I.%I', current_schema(), $1::text)) AND a.attnum > 0 AND NOT a.attisdropped
	 ORDER BY a.attnum`, table)
}

// primaryKeyOf lists a table's primary-key columns in key order; empty when it has none.
func primaryKeyOf(ctx context.Context, q Querier, table string) ([]string, error) {
	return strings1(ctx, q, `SELECT a.attname FROM pg_index x
	 CROSS JOIN LATERAL generate_series(0, x.indnkeyatts - 1) AS k(i)
	 JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = x.indkey[k.i]
	 WHERE x.indrelid = to_regclass(format('%I.%I', current_schema(), $1::text)) AND x.indisprimary
	 ORDER BY k.i`, table)
}

func strings1(ctx context.Context, q Querier, query string, args ...any) ([]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// canonicalSettings fix PostgreSQL's text output for the digest, whatever the server's defaults; they
// are set with set_config(…, true) and restored before the transaction goes on.
var canonicalSettings = [][2]string{{"DateStyle", "ISO, MDY"}, {"IntervalStyle", "postgres"},
	{"TimeZone", "UTC"}, {"extra_float_digits", "1"}, {"bytea_output", "hex"}}

// unitDigest is a sealed unit's facts.
type unitDigest struct {
	Rows         int64
	MinID, MaxID sql.NullString // the first primary-key column of the first and last row
	Digest       []byte
}

// digestPartition computes the canonical unit digest of one partition of parent (P16): columns in the
// parent's declared order, rows in primary-key order (all columns when there is no key), each value
// in its text output form through format('%s', …), which calls the type's output function.
func digestPartition(ctx context.Context, q Querier, parent, part string) (unitDigest, error) {
	var out unitDigest
	cols, err := columnsOf(ctx, q, parent)
	if err != nil {
		return out, err
	}
	pk, err := primaryKeyOf(ctx, q, parent)
	if err != nil {
		return out, err
	}
	if len(pk) == 0 {
		pk = cols
	}
	restore, err := canonicalOutput(ctx, q)
	if err != nil {
		return out, err
	}
	exprs := make([]string, len(cols))
	for i, c := range cols {
		exprs[i] = fmt.Sprintf("CASE WHEN %[1]s IS NULL THEN NULL ELSE format('%%s', %[1]s) END", quoteIdent(c))
	}
	pkText := make([]string, len(pk))
	for i, c := range pk {
		pkText[i] = quoteIdent(c)
	}
	query := fmt.Sprintf(`SELECT %s::text, %s FROM %s ORDER BY %s`, quoteIdent(pk[0]), strings.Join(exprs, ", "),
		quoteIdent(part), strings.Join(pkText, ", "))
	if err := scanDigest(ctx, q, query, len(cols), &out); err != nil {
		return out, err
	}
	return out, restore(ctx)
}

func scanDigest(ctx context.Context, q Querier, query string, n int, out *unitDigest) error {
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	d := NewDigest()
	vals := make([]sql.NullString, n)
	dest := make([]any, n+1)
	var key sql.NullString
	dest[0] = &key
	for i := range vals {
		dest[i+1] = &vals[i]
	}
	fields := make([]*string, n)
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		for i := range vals {
			fields[i] = nil
			if vals[i].Valid {
				fields[i] = &vals[i].String
			}
		}
		d.Row(fields)
		if !out.MinID.Valid {
			out.MinID = key
		}
		out.MaxID = key
	}
	if err := rows.Err(); err != nil {
		return err
	}
	out.Rows, out.Digest = int64(d.Rows()), d.Sum()
	return nil
}

// canonicalOutput sets canonicalSettings for the rest of the transaction and returns a function that
// puts the previous values back.
func canonicalOutput(ctx context.Context, q Querier) (func(context.Context) error, error) {
	prev := make([]string, len(canonicalSettings))
	for i, s := range canonicalSettings {
		if err := q.QueryRowContext(ctx, `SELECT current_setting($1)`, s[0]).Scan(&prev[i]); err != nil {
			return nil, err
		}
		if _, err := q.ExecContext(ctx, `SELECT set_config($1, $2, true)`, s[0], s[1]); err != nil {
			return nil, err
		}
	}
	return func(ctx context.Context) error {
		for i, s := range canonicalSettings {
			if _, err := q.ExecContext(ctx, `SELECT set_config($1, $2, true)`, s[0], prev[i]); err != nil {
				return err
			}
		}
		return nil
	}, nil
}
