package pg

import (
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// Isolation is a transaction's isolation level (P10.4).
type Isolation int

// The isolation levels a transaction may ask for; ReadCommitted is the default (P10.4).
const (
	ReadCommitted Isolation = iota
	RepeatableRead
	Serializable
)

// Defaults and ceilings of the per-transaction settings (P10.3, P10.4).
const (
	DefaultStatementTimeout  = 5 * time.Second  // statement_timeout ceiling of a read-write transaction
	SnapshotStatementTimeout = 30 * time.Second // statement_timeout ceiling of a read snapshot
	DefaultLockTimeout       = 2 * time.Second  // lock_timeout
	DefaultIdleTimeout       = 30 * time.Second // idle_in_transaction_session_timeout
	DefaultMaxAttempts       = problem.MaxTxAttempts
)

// TxOptions shapes one transaction (P10.3, P10.4). Zero values take the protocol defaults; a timeout
// above its protocol ceiling is lowered to the ceiling, and every statement timeout is further capped
// by the remaining deadline.
type TxOptions struct {
	Isolation        Isolation
	ReadOnly         bool
	StatementTimeout time.Duration // 0 = 5 s (a read snapshot: 30 s)
	LockTimeout      time.Duration // 0 = 2 s
	IdleTimeout      time.Duration // 0 = 30 s
	MaxAttempts      int           // 0 = 3; only 40001 / 40P01 re-run the body
}

// ReadSnapshot is REPEATABLE READ READ ONLY with statement_timeout up to 30 s (P10.3). It is a
// function, not a variable, so no member can change it for the others.
func ReadSnapshot() TxOptions {
	return TxOptions{Isolation: RepeatableRead, ReadOnly: true, StatementTimeout: SnapshotStatementTimeout}
}

// session is what the SET LOCAL batch of one transaction carries (P10.2, P10.3).
type session struct {
	Role, Schema, AppName string
	Statement             time.Duration
	Lock                  time.Duration
	Idle                  time.Duration
	Transaction           time.Duration // 0 = not sent (PostgreSQL < 17, or no deadline)
}

func (o TxOptions) isSnapshot() bool { return o.Isolation == RepeatableRead && o.ReadOnly }

func (o TxOptions) maxAttempts() int {
	if o.MaxAttempts > 0 {
		return o.MaxAttempts
	}
	return DefaultMaxAttempts
}

// resolve computes the timeouts of one transaction from the options, the remaining deadline (when
// hasDeadline) and the server version (P10.3). Less than one millisecond left is
// DEADLINE_BUDGET_EXHAUSTED: a zero statement_timeout would disable the limit.
func (o TxOptions) resolve(remaining time.Duration, hasDeadline bool, serverVersion int) (session, error) {
	if hasDeadline && remaining < time.Millisecond {
		return session{}, problem.Be("DEADLINE_BUDGET_EXHAUSTED", nil)
	}
	ceiling := DefaultStatementTimeout
	if o.isSnapshot() {
		ceiling = SnapshotStatementTimeout
	}
	s := session{
		Statement: capAt(o.StatementTimeout, ceiling),
		Lock:      capAt(o.LockTimeout, DefaultLockTimeout),
		Idle:      capAt(o.IdleTimeout, DefaultIdleTimeout),
	}
	if hasDeadline {
		s.Statement = min(s.Statement, remaining)
		if serverVersion >= 170000 {
			s.Transaction = remaining
		}
	}
	return s, nil
}

// capAt returns v, or ceiling when v is zero, negative or above it.
func capAt(v, ceiling time.Duration) time.Duration {
	if v <= 0 || v > ceiling {
		return ceiling
	}
	return v
}
