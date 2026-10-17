package db

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/brickKit/be-sdk-go/v3/clock"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
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
	lock         time.Duration
	idle         time.Duration
	total        time.Duration
	native       bool // the server has transaction_timeout (PostgreSQL 17+)
}

// Serializable runs the transaction at SERIALIZABLE (declared per transaction, protocol 14).
func Serializable() TxOption { return func(o *txOptions) { o.serializable = true } }

// StatementTimeout overrides the statement timeout of this transaction (reports, background work).
func StatementTimeout(d time.Duration) TxOption { return func(o *txOptions) { o.statement = d } }

// LockTimeout overrides the lock wait of this transaction.
func LockTimeout(d time.Duration) TxOption { return func(o *txOptions) { o.lock = d } }

// TransactionTimeout bounds the whole transaction: transaction_timeout on PostgreSQL 17 and later; earlier versions
// get the statement and idle-in-transaction timeouts capped at d instead (protocol P6.5).
func TransactionTimeout(d time.Duration) TxOption { return func(o *txOptions) { o.total = d } }

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
	o := txOptions{statement: d.settings.Timeouts.Statement, lock: d.settings.Timeouts.Lock, idle: d.settings.Timeouts.IdleInTx}
	for _, f := range opts {
		f(&o)
	}
	if o.total > 0 {
		// whole-transaction limit: native from PostgreSQL 17, otherwise the statement and idle timeouts capped at it
		if o.native = d.pool.serverVersion(ctx) >= 170000; !o.native {
			o.statement, o.idle = min(o.statement, o.total), min(o.idle, o.total)
		}
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
		d.settings.User, pgx.Identifier{d.settings.Schema}.Sanitize(), ms(o.statement), ms(o.lock), ms(o.idle),
		d.member); err != nil {
		return err
	}
	if o.total > 0 && o.native {
		if _, err = tx.Exec(ctx, d.prefix+"SELECT set_config('transaction_timeout', $1, true)", ms(o.total)); err != nil {
			return err
		}
	}
	if err = fn(context.WithValue(ctx, txKey{}, true), Tx{tx: tx, prefix: d.prefix}); err != nil {
		return err
	}
	if cutoff, ok := reqctx.Cutoff(ctx); ok {
		// P8.12: the database ends the transaction if COMMIT does not follow the check within C_commit; then the
		// check itself, on the suspend-aware clock
		if _, err = tx.Exec(ctx, d.prefix+"SELECT set_config('idle_in_transaction_session_timeout', $1, true)",
			ms(CommitWindow)); err != nil {
			return err
		}
		if clock.Boot() >= cutoff {
			err = errs.New(errs.ReasonAuthorizationViewExpired, "the authorization expired before the change could be committed")
			return err
		}
	}
	return tx.Commit(ctx)
}

// CommitWindow is C_commit: the longest time from the pre-commit cutoff check to COMMIT.
const CommitWindow = 2 * time.Second

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
