package pg

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func pgErr(code string) error { return fmt.Errorf("query: %w", &pgconn.PgError{Code: code}) }

func TestClassifyRetriesSerializationAndDeadlock(t *testing.T) {
	for _, state := range []string{"40001", "40P01"} {
		d := classifyAttempt(pgErr(state), 1, 3, nil)
		require.True(t, d.retry, state)
		require.Equal(t, 10*time.Millisecond, d.base)
		require.Equal(t, state, d.reason)
		d = classifyAttempt(pgErr(state), 2, 3, nil)
		require.True(t, d.retry)
		require.Equal(t, 20*time.Millisecond, d.base)
		d = classifyAttempt(pgErr(state), 3, 3, nil)
		require.False(t, d.retry)
		require.True(t, problem.Is(d.err, problem.DomainBe, "TX_CONFLICT"))
		require.Equal(t, state, SQLState(d.err), "the original stays the cause")
	}
	d := classifyAttempt(pgErr("40001"), 1, 1, nil)
	require.False(t, d.retry, "MaxAttempts 1 never retries")
	require.True(t, problem.Is(d.err, problem.DomainBe, "TX_CONFLICT"))
}

func TestClassifyMapsTheSQLStateTable(t *testing.T) {
	cases := map[string]string{
		"55P03": "LOCK_TIMEOUT", "57014": "STATEMENT_TIMEOUT", "25P04": "STATEMENT_TIMEOUT",
		"53300": "DB_TOO_MANY_CONNECTIONS", "BE001": "UNIT_SEALED", "23505": "INTERNAL", "42P01": "INTERNAL",
	}
	for state, reason := range cases {
		d := classifyAttempt(pgErr(state), 1, 3, nil)
		require.False(t, d.retry, state)
		require.True(t, problem.Is(d.err, problem.DomainBe, reason), "%s → %v", state, d.err)
		require.Equal(t, state, SQLState(d.err))
	}
}

func TestClassifyCancelledStatement(t *testing.T) {
	d := classifyAttempt(pgErr("57014"), 1, 3, context.Canceled)
	var pe *problem.Error
	require.ErrorAs(t, d.err, &pe)
	require.Equal(t, codes.Canceled, pe.Code)
}

func TestClassifyProblemPassesThrough(t *testing.T) {
	own := problem.New(codes.FailedPrecondition, "erp/sales", "ORDER_CLOSED", nil, "")
	d := classifyAttempt(own, 1, 3, nil)
	require.Same(t, own, d.err)
	wrapped := fmt.Errorf("ctx: %w", own)
	require.Same(t, wrapped, classifyAttempt(wrapped, 1, 3, nil).err)
}

func TestClassifyWithoutSQLState(t *testing.T) {
	plain := errors.New("boom")
	d := classifyAttempt(plain, 1, 3, nil)
	require.True(t, problem.Is(d.err, problem.DomainBe, "INTERNAL"))
	require.ErrorIs(t, d.err, plain)
	d = classifyAttempt(context.DeadlineExceeded, 1, 3, context.DeadlineExceeded)
	require.True(t, problem.Is(d.err, problem.DomainBe, "STATEMENT_TIMEOUT"), "%v", d.err)
	d = classifyAttempt(context.Canceled, 1, 3, context.Canceled)
	var pe *problem.Error
	require.ErrorAs(t, d.err, &pe)
	require.Equal(t, codes.Canceled, pe.Code)
}

func TestJitterStaysWithinHalf(t *testing.T) {
	for range 1000 {
		j := jitter(10 * time.Millisecond)
		require.GreaterOrEqual(t, j, 5*time.Millisecond)
		require.Less(t, j, 15*time.Millisecond)
	}
}
