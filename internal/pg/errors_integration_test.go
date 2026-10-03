package pg

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

const raise40001 = `DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'conflict'; END $$`

// 40001 re-runs the whole body with a fresh transaction, then becomes TX_CONFLICT (P10.4).
func TestRunRetriesSerializationFailureThenTxConflict(t *testing.T) {
	id := testpg.New(t)
	asOwner(t, id, `CREATE TABLE note (n int)`)
	p, _ := standalone(t, id, 1)
	var retries []string
	s := NewStore(p, StoreConfig{ComponentID: "c/x", Role: id.User, Schema: id.Schema,
		Hooks: Hooks{OnTxRetry: func(r string) { retries = append(retries, r) }}})
	attempts := 0
	err := s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		attempts++
		if _, err := tx.ExecContext(ctx, `INSERT INTO note VALUES (1)`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, raise40001)
		return err
	})
	require.True(t, problem.Is(err, problem.DomainBe, "TX_CONFLICT"), "%v", err)
	require.Equal(t, "40001", SQLState(err))
	require.Equal(t, 3, attempts)
	require.Equal(t, []string{"40001", "40001"}, retries)

	attempts = 0
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		attempts++
		if _, err := tx.ExecContext(ctx, `INSERT INTO note VALUES (2)`); err != nil {
			return err
		}
		if attempts == 1 {
			_, err := tx.ExecContext(ctx, raise40001)
			return err
		}
		return nil
	}))
	var rows int
	require.NoError(t, s.Run(within(t, 10e9), ReadSnapshot(), func(ctx context.Context, tx *Tx) error {
		return tx.QueryRowContext(ctx, `SELECT count(*) FROM note`).Scan(&rows)
	}))
	require.Equal(t, 1, rows, "failed attempts rolled back, the successful one committed once")
}

// A lock held elsewhere fails after lock_timeout = 2 s with LOCK_TIMEOUT (P10.3, P10.4).
func TestRunLockTimeout(t *testing.T) {
	id := testpg.New(t)
	asOwner(t, id, `CREATE TABLE held (n int)`)
	_, s := standalone(t, id, 1)
	super := testpg.Open(t, id.SuperDSN)
	holder, err := super.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback() }()
	_, err = holder.Exec(`LOCK TABLE ` + id.Schema + `.held IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)

	start := time.Now()
	err = s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		_, err := tx.ExecContext(ctx, `SELECT * FROM held`)
		return err
	})
	elapsed := time.Since(start)
	require.True(t, problem.Is(err, problem.DomainBe, "LOCK_TIMEOUT"), "%v", err)
	require.True(t, IsLockTimeout(err))
	require.InDelta(t, 2.0, elapsed.Seconds(), 0.8)
}

// statement_timeout cancels a long statement: STATEMENT_TIMEOUT (P10.3, P10.4).
func TestRunStatementTimeout(t *testing.T) {
	id := testpg.New(t)
	_, s := standalone(t, id, 1)
	start := time.Now()
	err := s.Run(within(t, 10e9), TxOptions{StatementTimeout: 200 * time.Millisecond}, func(ctx context.Context, tx *Tx) error {
		_, err := tx.ExecContext(ctx, `SELECT pg_sleep(3)`)
		return err
	})
	require.True(t, problem.Is(err, problem.DomainBe, "STATEMENT_TIMEOUT"), "%v", err)
	require.Less(t, time.Since(start), 2*time.Second)
	// the remaining deadline caps it too
	start = time.Now()
	err = s.Run(within(t, 300*time.Millisecond), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		_, err := tx.ExecContext(ctx, `SELECT pg_sleep(3)`)
		return err
	})
	var pe *problem.Error
	require.ErrorAs(t, err, &pe)
	require.Equal(t, "STATEMENT_TIMEOUT", pe.Reason, "%v", err)
	require.Less(t, time.Since(start), 2*time.Second)
}

// A member's budget of 1 refuses a second concurrent unit of work after AcquireTimeout; another
// member on the same pool is unaffected (P10.5).
func TestRunBudgetExhausted(t *testing.T) {
	id := testpg.New(t)
	p, _ := standalone(t, id, 5)
	var waits atomic.Int64
	var inUse atomic.Int64
	busy := NewStore(p, StoreConfig{ComponentID: "c/busy", Role: id.User, Schema: id.Schema, Budget: 1,
		AcquireTimeout: 300 * time.Millisecond,
		Hooks:          Hooks{OnPoolWait: func(time.Duration) { waits.Add(1) }, OnInUse: func(d int) { inUse.Add(int64(d)) }}})
	other := NewStore(p, StoreConfig{ComponentID: "c/other", Role: id.User, Schema: id.Schema, Budget: 1})

	hold, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- busy.Run(within(t, 10e9), TxOptions{}, func(context.Context, *Tx) error {
			close(hold)
			<-release
			return nil
		})
	}()
	<-hold
	require.Equal(t, int64(1), inUse.Load())
	start := time.Now()
	err := busy.Run(within(t, 10e9), TxOptions{}, func(context.Context, *Tx) error { return nil })
	require.True(t, problem.Is(err, problem.DomainBe, "DB_POOL_EXHAUSTED"), "%v", err)
	require.InDelta(t, 0.3, time.Since(start).Seconds(), 0.25)
	require.NoError(t, other.Run(within(t, 10e9), TxOptions{}, func(context.Context, *Tx) error { return nil }))
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, int64(0), inUse.Load())
	require.Equal(t, int64(2), waits.Load())
}

// The password func is called for every new physical connection, so a rotated secret applies to
// the next new connection without reopening the pool (P2.9).
func TestPasswordRotationAppliesToNewConnections(t *testing.T) {
	id := testpg.New(t)
	var current atomic.Value
	current.Store("wrong")
	var calls atomic.Int64
	p, err := OpenPool(PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User, SSLMode: "disable",
		MaxConns: 1, Password: func() string { calls.Add(1); return current.Load().(string) }})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	s := NewStore(p, StoreConfig{ComponentID: "c/x", Role: id.User, Schema: id.Schema})
	noop := func(context.Context, *Tx) error { return nil }

	require.Error(t, s.Run(within(t, 10e9), TxOptions{}, noop), "wrong password")
	current.Store(id.Password)
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, noop))

	super := testpg.Open(t, id.SuperDSN)
	testpg.Exec(t, super, `ALTER ROLE `+id.User+` PASSWORD 'rotated'`)
	current.Store("rotated")
	before := calls.Load()
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, noop), "the open connection lives on")
	require.Equal(t, before, calls.Load())
	p.db.SetMaxIdleConns(0) // retire the idle connection, as PG_CONN_MAX_LIFETIME would
	p.db.SetMaxIdleConns(1)
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, noop))
	require.Equal(t, before+1, calls.Load(), "one new connection, with the rotated password")
}

// A real serialization failure between two SERIALIZABLE units of work, detected at COMMIT: the loser
// re-runs and wins on its second attempt (P10.4).
func TestRunRetriesRealSerializationConflict(t *testing.T) {
	id := testpg.New(t)
	asOwner(t, id, `CREATE TABLE ledger (n int)`)
	_, s := standalone(t, id, 2)
	bWrote, aDone := make(chan struct{}), make(chan struct{})
	aErr := make(chan error, 1)
	count := func(ctx context.Context, tx *Tx) error {
		var n int
		return tx.QueryRowContext(ctx, `SELECT count(*) FROM ledger`).Scan(&n)
	}
	go func() {
		aErr <- s.Run(within(t, 10e9), TxOptions{Isolation: Serializable}, func(ctx context.Context, tx *Tx) error {
			if err := count(ctx, tx); err != nil {
				return err
			}
			<-bWrote
			_, err := tx.ExecContext(ctx, `INSERT INTO ledger VALUES (1)`)
			return err
		})
		close(aDone)
	}()
	attempts := 0
	err := s.Run(within(t, 10e9), TxOptions{Isolation: Serializable}, func(ctx context.Context, tx *Tx) error {
		attempts++
		if err := count(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger VALUES (2)`); err != nil {
			return err
		}
		if attempts == 1 {
			close(bWrote)
			<-aDone // A commits first; this transaction then fails at COMMIT
		}
		return nil
	})
	require.NoError(t, <-aErr)
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}
