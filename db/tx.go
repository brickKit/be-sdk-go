package db

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

// ForegroundWait is how long a foreground transaction waits for a connection before SERVICE_BUSY (protocol 17).
const ForegroundWait = 2 * time.Second

// maxAttempts bounds the retries of serialization failures and deadlocks (the whole transaction is redone).
const maxAttempts = 3

// DB is one member's handle on its database: a shared or own pool, the member's role and schema, its timeouts and
// its connection budget. Built by the launcher or the shell; modules get it from the runtime.
type DB struct {
	member   string
	pool     *Pool
	settings Settings
	prefix   string
	fg, bg   chan struct{}
}

// NewDB binds a member to a pool.
func NewDB(member string, pool *Pool, s Settings) *DB {
	return &DB{member: member, pool: pool, settings: s,
		// one prefix per member: the same SQL of two members never shares a cached statement (protocol 14)
		prefix: "/* " + member + " */ ",
		fg:     make(chan struct{}, max(s.Foreground, 1)), bg: make(chan struct{}, max(s.Background, 1))}
}

type txKey struct{}

// InTx reports whether ctx belongs to an open transaction; the call clients refuse network calls then (NETWORK_IN_TX).
func InTx(ctx context.Context) bool { return ctx.Value(txKey{}) != nil }

// Tx is the transaction a function runs in. Every statement carries the member's prefix.
type Tx struct {
	tx     pgx.Tx
	prefix string
}

// Exec runs a statement.
func (t Tx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.tx.Exec(ctx, t.prefix+sql, args...)
}

// Query runs a query.
func (t Tx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.tx.Query(ctx, t.prefix+sql, args...)
}

// QueryRow runs a query returning at most one row.
func (t Tx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, t.prefix+sql, args...)
}

// TxOption adjusts one transaction.
type TxOption func(*txOptions)

type txOptions struct {
	serializable bool
	statement    time.Duration
}

// Serializable runs the transaction at SERIALIZABLE (declared per transaction, protocol 14).
func Serializable() TxOption { return func(o *txOptions) { o.serializable = true } }

// StatementTimeout overrides the statement timeout of this transaction (reports, background work).
func StatementTimeout(d time.Duration) TxOption { return func(o *txOptions) { o.statement = d } }

// Tx runs fn in a foreground transaction (a user request).
func (d *DB) Tx(ctx context.Context, fn func(context.Context, Tx) error, opts ...TxOption) error {
	return d.run(ctx, d.fg, true, fn, opts)
}

// BackgroundTx runs fn in a background transaction (work items, consumers); it waits for its budget instead of
// failing fast.
func (d *DB) BackgroundTx(ctx context.Context, fn func(context.Context, Tx) error, opts ...TxOption) error {
	return d.run(ctx, d.bg, false, fn, opts)
}

func (d *DB) run(ctx context.Context, budget chan struct{}, foreground bool, fn func(context.Context, Tx) error, opts []TxOption) error {
	if InTx(ctx) {
		return errs.New(errs.ReasonInternal, "").Wrap(errors.New("NESTED_TX: a transaction was opened inside another"))
	}
	o := txOptions{statement: d.settings.Timeouts.Statement}
	for _, f := range opts {
		f(&o)
	}
	if err := acquire(ctx, budget, foreground); err != nil {
		return err
	}
	defer func() { <-budget }()

	var last error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := d.once(ctx, fn, o)
		if err == nil {
			return nil
		}
		last = err
		if !retryable(err) || attempt == maxAttempts {
			break
		}
		time.Sleep(time.Duration(rand.Int64N(int64(20*time.Millisecond) << attempt)))
	}
	return mapError(ctx, last)
}

func acquire(ctx context.Context, budget chan struct{}, foreground bool) error {
	if !foreground {
		select {
		case budget <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t := time.NewTimer(ForegroundWait)
	defer t.Stop()
	select {
	case budget <- struct{}{}:
		return nil
	case <-t.C:
		return errs.New(errs.ReasonServiceBusy, "no database connection became free in time").
			With("resource", "db_pool").WithRetryAfter(time.Second)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *DB) once(ctx context.Context, fn func(context.Context, Tx) error, o txOptions) (err error) {
	iso := pgx.ReadCommitted
	if o.serializable {
		iso = pgx.Serializable
	}
	tx, err := d.pool.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: iso})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	// Everything LOCAL: it ends with the transaction and never stays on the pooled connection (protocol 14, 0-12).
	if _, err = tx.Exec(ctx, d.prefix+`SELECT set_config('role', $1, true), set_config('search_path', $2, true),
		set_config('statement_timeout', $3, true), set_config('lock_timeout', $4, true),
		set_config('idle_in_transaction_session_timeout', $5, true), set_config('application_name', $6, true)`,
		d.settings.User, pgx.Identifier{d.settings.Schema}.Sanitize(), ms(o.statement), ms(d.settings.Timeouts.Lock),
		ms(d.settings.Timeouts.IdleInTx), d.member); err != nil {
		return err
	}
	if err = fn(context.WithValue(ctx, txKey{}, true), Tx{tx: tx, prefix: d.prefix}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func ms(d time.Duration) string { return fmt.Sprintf("%dms", d.Milliseconds()) }

func retryable(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && (pg.Code == "40001" || pg.Code == "40P01")
}

// mapError turns database errors into the protocol's reasons; errors the module returned pass through unchanged.
func mapError(ctx context.Context, err error) error {
	var e *errs.Error
	if errors.As(err, &e) {
		return err
	}
	if ctx.Err() != nil {
		return errs.From(ctx.Err()).Wrap(err)
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "40001", "40P01":
			return errs.New(errs.ReasonTxConflict, "a concurrent change conflicted; try again").Wrap(err)
		case "55P03":
			return errs.New(errs.ReasonLockTimeout, "waiting for a lock took too long").Wrap(err)
		case "57014":
			return errs.New(errs.ReasonStatementTimeout, "the database took too long").Wrap(err)
		case "53300", "57P01", "57P02", "57P03":
			return errs.New(errs.ReasonDependencyUnavailable, "the database is unavailable").With("dependency", "db").Wrap(err)
		}
		return errs.From(err)
	}
	if pgconn.SafeToRetry(err) || errors.Is(err, puddle.ErrClosedPool) {
		return errs.New(errs.ReasonDependencyUnavailable, "the database is unavailable").With("dependency", "db").Wrap(err)
	}
	var netErr *pgconn.ConnectError
	if errors.As(err, &netErr) {
		return errs.New(errs.ReasonDependencyUnavailable, "the database is unavailable").With("dependency", "db").Wrap(err)
	}
	return errs.From(err)
}
