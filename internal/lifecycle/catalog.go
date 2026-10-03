package lifecycle

import (
	"context"
	"database/sql"
	"time"
)

// Querier is what the lifecycle SQL needs: *sql.Tx (the owner's migration transaction) and the
// runtime's *pg.Tx both satisfy it.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// partition is one attached partition as the catalogue describes it: a RANGE partition has From and
// To, a LIST partition has Value, a DEFAULT partition has neither.
type partition struct {
	Name     string
	From, To sql.NullTime
	Value    sql.NullString
}

// listPartitionsSQL reads every partition of a parent in the current schema with its bounds, parsed by
// PostgreSQL itself from pg_get_expr (the bound literals are cast back to timestamptz, so the session's
// DateStyle and TimeZone do not matter).
const listPartitionsSQL = `SELECT c.relname, b.r[1]::timestamptz, b.r[2]::timestamptz, b.l[1]
  FROM pg_inherits i
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_class p ON p.oid = i.inhparent
  JOIN pg_namespace n ON n.oid = p.relnamespace
  CROSS JOIN LATERAL (SELECT regexp_match(pg_get_expr(c.relpartbound, c.oid), '^FOR VALUES FROM \(''([^'']*)''\) TO \(''([^'']*)''\)$') AS r,
                             regexp_match(pg_get_expr(c.relpartbound, c.oid), '^FOR VALUES IN \(''((?:[^'']|'''')*)''\)$') AS l) b
 WHERE n.nspname = current_schema() AND p.relname = $1
 ORDER BY 2 NULLS LAST, 1`

func listPartitions(ctx context.Context, q Querier, parent string) ([]partition, error) {
	rows, err := q.QueryContext(ctx, listPartitionsSQL, parent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []partition
	for rows.Next() {
		var p partition
		if err := rows.Scan(&p.Name, &p.From, &p.To, &p.Value); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// match finds the partition with exactly [from, to) and reports whether another one overlaps it.
func match(parts []partition, from, to time.Time) (exact *partition, overlap bool) {
	for i := range parts {
		p := &parts[i]
		if !p.From.Valid || !p.To.Valid {
			continue
		}
		if p.From.Time.Equal(from) && p.To.Time.Equal(to) {
			return p, false
		}
		if p.From.Time.Before(to) && from.Before(p.To.Time) {
			overlap = true
		}
	}
	return nil, overlap
}
