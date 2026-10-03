package pg

import (
	"context"
	"database/sql"
	"strings"
)

// Tx is one open transaction of a Store. Its four methods have *sql.Tx's signatures (sqlc's DBTX),
// and each prefixes the SQL with `/* be:<PG_SCHEMA> */ ` so a statement prepared for one member is
// never reused for another on a shared connection (P10.2, r1-04b). The ctx passed in must be the one
// Run handed to fn.
type Tx struct {
	tx     *sql.Tx
	schema string
	prefix string
}

func newTx(tx *sql.Tx, schema string) *Tx {
	return &Tx{tx: tx, schema: schema, prefix: "/* be:" + commentSafe(schema) + " */ "}
}

// Schema is the member's PG_SCHEMA.
func (t *Tx) Schema() string { return t.schema }

// ExecContext runs a statement (P10.2 prefix).
func (t *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, t.prefix+query, args...)
}

// QueryContext runs a query (P10.2 prefix).
func (t *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, t.prefix+query, args...)
}

// QueryRowContext runs a query expected to return at most one row (P10.2 prefix).
func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, t.prefix+query, args...)
}

// PrepareContext prepares a statement for this transaction (P10.2 prefix).
func (t *Tx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return t.tx.PrepareContext(ctx, t.prefix+query)
}

// commentSafe keeps a schema name from closing the prefix comment early.
func commentSafe(s string) string { return strings.ReplaceAll(s, "*/", "*_/") }
