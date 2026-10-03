package pg

import (
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/stretchr/testify/require"
)

func TestSetLocalSQLOrderAndQuoting(t *testing.T) {
	got := setLocalSQL(session{
		Role: `r"x`, Schema: "s_abc", AppName: "erp/o'k",
		Statement: 5 * time.Second, Lock: 2 * time.Second, Idle: 30 * time.Second,
	})
	want := `SET LOCAL ROLE "r""x"; SET LOCAL search_path TO "s_abc"; ` +
		`SET LOCAL application_name = 'erp/o''k'; SET LOCAL statement_timeout = '5000ms'; ` +
		`SET LOCAL lock_timeout = '2000ms'; SET LOCAL idle_in_transaction_session_timeout = '30000ms'`
	require.Equal(t, want, got)
}

func TestSetLocalSQLTransactionTimeout(t *testing.T) {
	got := setLocalSQL(session{Role: "r", Schema: "s", AppName: "a",
		Statement: time.Second, Lock: time.Second, Idle: time.Second, Transaction: 1500 * time.Millisecond})
	require.Contains(t, got, `; SET LOCAL transaction_timeout = '1500ms'`)
	require.True(t, len(got) > 0 && got[len(got)-len(`'1500ms'`):] == `'1500ms'`, "transaction_timeout comes last")
}

func TestResolveDefaults(t *testing.T) {
	s, err := TxOptions{}.resolve(0, false, 160000)
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, s.Statement)
	require.Equal(t, 2*time.Second, s.Lock)
	require.Equal(t, 30*time.Second, s.Idle)
	require.Zero(t, s.Transaction)
}

func TestResolveCapsByRemainingDeadline(t *testing.T) {
	s, err := TxOptions{}.resolve(1200*time.Millisecond, true, 160000)
	require.NoError(t, err)
	require.Equal(t, 1200*time.Millisecond, s.Statement)
	require.Zero(t, s.Transaction, "no transaction_timeout before PostgreSQL 17")
}

func TestResolveTransactionTimeoutOnPG17(t *testing.T) {
	s, err := TxOptions{}.resolve(10*time.Second, true, 170002)
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, s.Statement)
	require.Equal(t, 10*time.Second, s.Transaction)
	s, err = TxOptions{}.resolve(0, false, 170002)
	require.NoError(t, err)
	require.Zero(t, s.Transaction, "no deadline: no transaction_timeout")
}

func TestResolveStatementTimeoutCeilings(t *testing.T) {
	s, err := TxOptions{StatementTimeout: time.Minute}.resolve(0, false, 160000)
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, s.Statement, "a read-write transaction never exceeds 5 s")
	s, err = ReadSnapshot().resolve(0, false, 160000)
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, s.Statement, "a read snapshot defaults to 30 s")
	s, err = TxOptions{Isolation: RepeatableRead, ReadOnly: true, StatementTimeout: time.Hour}.resolve(0, false, 160000)
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, s.Statement)
	s, err = TxOptions{StatementTimeout: 300 * time.Millisecond, LockTimeout: time.Hour, IdleTimeout: time.Second}.resolve(0, false, 160000)
	require.NoError(t, err)
	require.Equal(t, 300*time.Millisecond, s.Statement)
	require.Equal(t, 2*time.Second, s.Lock, "lock_timeout never exceeds 2 s")
	require.Equal(t, time.Second, s.Idle)
}

func TestResolveDeadlineSpent(t *testing.T) {
	_, err := TxOptions{}.resolve(500*time.Microsecond, true, 160000)
	require.True(t, problem.Is(err, problem.DomainBe, "DEADLINE_BUDGET_EXHAUSTED"), "%v", err)
	_, err = TxOptions{}.resolve(-time.Second, true, 160000)
	require.True(t, problem.Is(err, problem.DomainBe, "DEADLINE_BUDGET_EXHAUSTED"), "%v", err)
}

func TestMaxAttemptsDefault(t *testing.T) {
	require.Equal(t, 3, TxOptions{}.maxAttempts())
	require.Equal(t, 1, TxOptions{MaxAttempts: 1}.maxAttempts())
}
