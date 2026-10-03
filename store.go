package besdk

import (
	"context"
	"errors"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// ErrNoDatabase: the component declares no PG_SCHEMA, so it has no store.
var ErrNoDatabase = errors.New("besdk: the component declares no database (PG_SCHEMA)")

// Isolation is a transaction's isolation level (P10.4).
type Isolation int

// Isolation levels; READ COMMITTED is the default.
const (
	ReadCommitted Isolation = iota
	RepeatableRead
	Serializable
)

// TxOptions shapes one transaction (P10.3, P10.4); zero values take the protocol defaults.
type TxOptions struct {
	Isolation        Isolation
	ReadOnly         bool
	StatementTimeout time.Duration // 0 = 5 s; capped by the remaining deadline
	LockTimeout      time.Duration // 0 = 2 s
	IdleTimeout      time.Duration // 0 = 30 s
	MaxAttempts      int           // 0 = 3; only serialization failures and deadlocks re-run the body
}

// Store is the component's database handle, bound to its runtime role, schema and connection budget
// (P10.1). It never uses PG_OWNER_USER (P10.12).
type Store struct {
	s  *pg.Store
	rt *Runtime
}

// Tx is one transaction. It implements sqlc's DBTX (ExecContext, QueryContext, QueryRowContext,
// PrepareContext), so `db.New(tx)` works, and carries the transactional actions: Publish (outbox),
// Lock / TryLock (advisory locks, P10.8).
type Tx struct {
	*pg.Tx
	rt *Runtime
}

// DBIdentity is the store's runtime identity.
type DBIdentity struct{ Role, Schema string }

// Store returns the component's store; ErrNoDatabase when it declares no database.
func (rt *Runtime) Store() (*Store, error) {
	if rt.deps.store == nil {
		return nil, ErrNoDatabase
	}
	return &Store{s: rt.deps.store, rt: rt}, nil
}

// Tx runs fn in one READ COMMITTED transaction. fn may run more than once (P10.4): it touches only
// tx, never the network (P8.4). A transaction inside a transaction is refused (NESTED_TX, P10.6).
func (s *Store) Tx(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	return s.TxWith(ctx, TxOptions{}, fn)
}

// TxWith runs fn in one transaction with the given options.
func (s *Store) TxWith(ctx context.Context, o TxOptions, fn func(ctx context.Context, tx *Tx) error) error {
	po := pg.TxOptions{Isolation: pg.Isolation(o.Isolation), ReadOnly: o.ReadOnly, StatementTimeout: o.StatementTimeout,
		LockTimeout: o.LockTimeout, IdleTimeout: o.IdleTimeout, MaxAttempts: o.MaxAttempts}
	return s.run(ctx, po, fn)
}

// ReadSnapshot runs fn in a REPEATABLE READ READ ONLY transaction, statements up to 30 s (P10.3).
func (s *Store) ReadSnapshot(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	return s.run(ctx, pg.ReadSnapshot(), fn)
}

func (s *Store) run(ctx context.Context, o pg.TxOptions, fn func(ctx context.Context, tx *Tx) error) error {
	return s.s.Run(ctx, o, func(ctx context.Context, t *pg.Tx) error { return fn(ctx, &Tx{Tx: t, rt: s.rt}) })
}

// Identity is the store's runtime identity, read only.
func (s *Store) Identity() DBIdentity { return DBIdentity{Role: s.s.Role(), Schema: s.s.Schema()} }

// IsUniqueViolation reports SQLSTATE 23505 anywhere in err; the component maps it to its own reason.
func IsUniqueViolation(err error) bool { return pg.IsUniqueViolation(err) }

// IsLockTimeout reports SQLSTATE 55P03 anywhere in err.
func IsLockTimeout(err error) bool { return pg.IsLockTimeout(err) }

// SQLState returns the SQLSTATE in err, "" when none.
func SQLState(err error) string { return pg.SQLState(err) }
