package pg

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"golang.org/x/sync/semaphore"
)

// DefaultAcquireTimeout is PG_POOL_ACQUIRE_TIMEOUT's default (P10.5).
const DefaultAcquireTimeout = 5 * time.Second

// Hooks report a store's events to the root's metrics; every hook is optional.
type Hooks struct {
	OnTxRetry  func(sqlstate string) // be_tx_retries_total{sqlstate}: 40001 or 40P01 (P10.4, P18.3)
	OnPoolWait func(time.Duration)   // be_db_pool_wait_seconds (P18)
	OnInUse    func(delta int)       // be_db_pool_in_use, +1 on acquire and −1 on release
}

// StoreConfig binds a store to one member's runtime identity and budget (P10.1, P10.5).
type StoreConfig struct {
	ComponentID    string        // set as application_name in every transaction (P10.2)
	Role           string        // PG_USER, the role every transaction switches to
	Schema         string        // PG_SCHEMA, the only entry of search_path
	Budget         int           // the member's PG_POOL_MAX: concurrent connections; 0 = 10
	AcquireTimeout time.Duration // PG_POOL_ACQUIRE_TIMEOUT; 0 = 5 s
	Hooks          Hooks
}

// Store runs one member's transactions on a (possibly shared) pool. It never uses PG_OWNER_USER
// (P10.12).
type Store struct {
	pool   *Pool
	cfg    StoreConfig
	budget *semaphore.Weighted
}

// NewStore binds a member to the pool (P10.5): at most Budget of its units of work hold a connection
// at once, whatever the other members do.
func NewStore(p *Pool, c StoreConfig) *Store {
	c.Budget = orDefault(c.Budget, DefaultPoolMax)
	c.AcquireTimeout = orDefaultDuration(c.AcquireTimeout, DefaultAcquireTimeout)
	return &Store{pool: p, cfg: c, budget: semaphore.NewWeighted(int64(c.Budget))}
}

// Schema is the store's PG_SCHEMA.
func (s *Store) Schema() string { return s.cfg.Schema }

// Role is the store's PG_USER.
func (s *Store) Role() string { return s.cfg.Role }

// Run runs fn in one transaction (P10.2–P10.6): refuses a nested transaction (NESTED_TX) and a spent
// deadline; takes one connection within min(AcquireTimeout, remaining) (DB_POOL_EXHAUSTED); opens the
// transaction with the protocol's SET LOCAL batch; commits; and re-runs fn on 40001 / 40P01 up to
// MaxAttempts. A *problem.Error from fn passes through unchanged; any other failure is classified by
// SQLSTATE with the original error as Cause.
func (s *Store) Run(ctx context.Context, o TxOptions, fn func(ctx context.Context, tx *Tx) error) error {
	if InTx(ctx) {
		return problem.Abort(problem.Be("NESTED_TX", nil))
	}
	if err := ctx.Err(); err != nil {
		return problem.From(err)
	}
	conn, release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	version, err := serverVersionOf(conn)
	if err != nil {
		return err
	}
	for attempt := 1; ; attempt++ {
		err := s.attempt(ctx, conn, o, version, fn)
		if err == nil {
			return nil
		}
		d := classifyAttempt(err, attempt, o.maxAttempts(), ctx.Err())
		if !d.retry {
			return d.err
		}
		if s.cfg.Hooks.OnTxRetry != nil {
			s.cfg.Hooks.OnTxRetry(d.reason)
		}
		if werr := sleepCtx(ctx, jitter(d.base)); werr != nil {
			return problem.From(werr)
		}
	}
}

// acquire takes a budget slot, then one physical connection, waiting at most
// min(AcquireTimeout, remaining deadline) for both together (P10.5).
func (s *Store) acquire(ctx context.Context) (*sql.Conn, func(), error) {
	start := time.Now()
	wctx, cancel := context.WithTimeout(ctx, s.cfg.AcquireTimeout)
	defer cancel()
	waited := func() {
		if s.cfg.Hooks.OnPoolWait != nil {
			s.cfg.Hooks.OnPoolWait(time.Since(start))
		}
	}
	if err := s.budget.Acquire(wctx, 1); err != nil {
		waited()
		return nil, nil, s.acquireError(ctx, err)
	}
	conn, err := s.pool.db.Conn(wctx)
	waited()
	if err != nil {
		s.budget.Release(1)
		return nil, nil, s.acquireError(ctx, err)
	}
	s.inUse(1)
	return conn, func() {
		_ = conn.Close()
		s.budget.Release(1)
		s.inUse(-1)
	}, nil
}

// acquireError: the caller's own cancellation stays CANCELLED, 53300 is DB_TOO_MANY_CONNECTIONS, a
// connect that failed or timed out is DEPENDENCY_UNAVAILABLE {dependency: db} (stage-B ruling), and a
// wait for a free connection that ran out of time is DB_POOL_EXHAUSTED (P10.5).
func (s *Store) acquireError(ctx context.Context, err error) error {
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return problem.From(ctx.Err())
	case SQLState(err) == "53300":
		return problem.Wrap(err, "DB_TOO_MANY_CONNECTIONS", nil)
	case IsConnectionFailure(err):
		return problem.DBUnavailable(err)
	case errors.Is(err, context.DeadlineExceeded):
		return problem.Wrap(err, "DB_POOL_EXHAUSTED", nil)
	}
	return connectError(ctx, err)
}

func (s *Store) inUse(delta int) {
	if s.cfg.Hooks.OnInUse != nil {
		s.cfg.Hooks.OnInUse(delta)
	}
}

// attempt is one run of fn: BEGIN, the SET LOCAL batch, fn, COMMIT; rolled back on any failure.
func (s *Store) attempt(ctx context.Context, conn *sql.Conn, o TxOptions, version int, fn func(context.Context, *Tx) error) (err error) {
	stx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: isolationLevel(o.Isolation), ReadOnly: o.ReadOnly})
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = stx.Rollback()
		}
	}()
	remaining, hasDeadline := remainingOf(ctx)
	sess, err := o.resolve(remaining, hasDeadline, version)
	if err != nil {
		return err
	}
	sess.Role, sess.Schema, sess.AppName = s.cfg.Role, s.cfg.Schema, s.cfg.ComponentID
	tx := newTx(stx, s.cfg.Schema)
	if _, err := tx.ExecContext(ctx, setLocalSQL(sess)); err != nil {
		return err
	}
	if err := fn(markInTx(ctx), tx); err != nil {
		return err
	}
	committed = true
	return stx.Commit()
}

func isolationLevel(i Isolation) sql.IsolationLevel {
	switch i {
	case RepeatableRead:
		return sql.LevelRepeatableRead
	case Serializable:
		return sql.LevelSerializable
	}
	return sql.LevelReadCommitted
}

// remainingOf is the time left before ctx's deadline.
func remainingOf(ctx context.Context) (time.Duration, bool) {
	dl, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(dl), true
}

// sleepCtx waits d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
