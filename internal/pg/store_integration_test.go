package pg

import (
	"context"
	"strings"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

// Every SET LOCAL lands inside the transaction and nothing leaks to the next borrower (P10.2, P10.3).
func TestRunSetsLocalSettingsAndNothingLeaks(t *testing.T) {
	for _, major := range []string{"16", "14"} {
		t.Run("pg"+major, func(t *testing.T) {
			id := testpg.NewOn(t, major)
			p, s := standalone(t, id, 1)
			var got [8]string
			var inPid int
			err := s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
				return tx.QueryRowContext(ctx, `SELECT current_user, current_schema(), current_setting('application_name'),
				  current_setting('statement_timeout'), current_setting('lock_timeout'),
				  current_setting('idle_in_transaction_session_timeout'), current_setting('TimeZone'),
				  current_setting('transaction_isolation'), pg_backend_pid()`).
					Scan(&got[0], &got[1], &got[2], &got[3], &got[4], &got[5], &got[6], &got[7], &inPid)
			})
			require.NoError(t, err)
			require.Equal(t, [8]string{id.User, id.Schema, "conformance/widget", "5s", "2s", "30s", "UTC", "read committed"}, got)

			var snap [3]string
			require.NoError(t, s.Run(within(t, 60e9), ReadSnapshot(), func(ctx context.Context, tx *Tx) error {
				return tx.QueryRowContext(ctx, `SELECT current_setting('transaction_isolation'),
				  current_setting('transaction_read_only'), current_setting('statement_timeout')`).Scan(&snap[0], &snap[1], &snap[2])
			}))
			require.Equal(t, [3]string{"repeatable read", "on", "30s"}, snap)

			var after [4]string
			var outPid int
			require.NoError(t, p.db.QueryRowContext(context.Background(), `SELECT current_setting('search_path'),
			  current_setting('statement_timeout'), current_setting('lock_timeout'), current_setting('application_name'), pg_backend_pid()`).
				Scan(&after[0], &after[1], &after[2], &after[3], &outPid))
			require.Equal(t, inPid, outPid, "pool of one: same physical connection")
			require.Equal(t, [4]string{`"$user", public`, "0", "0", ""}, after)
		})
	}
}

// The six SET LOCALs travel in one simple-protocol Query message, right after BEGIN (P10.2).
func TestRunSendsSettingsInOneRoundTrip(t *testing.T) {
	id := testpg.New(t)
	var w wireTrace
	p, err := openPool(PoolConfig{Host: id.Host, Port: id.Port, Database: id.Database, User: id.User,
		Password: func() string { return id.Password }, SSLMode: "disable", MaxConns: 1}, w.attach)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	s := NewStore(p, StoreConfig{ComponentID: "conformance/widget", Role: id.User, Schema: id.Schema})
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		_, err := tx.ExecContext(ctx, `SELECT 1`)
		return err
	}))
	var frontend []string
	for _, line := range strings.Split(w.String(), "\n") {
		if strings.HasPrefix(line, "F\t") {
			frontend = append(frontend, line)
		}
	}
	var sets []string
	beginAt := -1
	for i, l := range frontend {
		if strings.Contains(l, "SET LOCAL") {
			sets = append(sets, l)
			require.Equal(t, beginAt+1, i, "the batch directly follows BEGIN")
		}
		if strings.Contains(strings.ToLower(l), `"begin`) {
			beginAt = i
		}
	}
	require.Len(t, sets, 1, "one message carries every SET LOCAL:\n%s", strings.Join(frontend, "\n"))
	require.Contains(t, sets[0], "\tQuery\t", "simple protocol")
	for _, k := range []string{"ROLE", "search_path", "application_name", "statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout"} {
		require.Contains(t, sets[0], "SET LOCAL "+k)
	}
	require.Contains(t, w.String(), "/* be:"+id.Schema+" */ SELECT 1", "the body carries the member prefix")
	require.Contains(t, sets[0], "/* be:"+id.Schema+" */ SET LOCAL ROLE", "every statement sent for a member carries the prefix")
}

func TestRunRefusesNestedTransaction(t *testing.T) {
	id := testpg.New(t)
	_, s := standalone(t, id, 2)
	var inner error
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		inner = s.Run(ctx, TxOptions{}, func(context.Context, *Tx) error { return nil })
		return nil
	}))
	require.True(t, problem.Is(inner, problem.DomainBe, "NESTED_TX"), "%v", inner)
}

func TestRunRefusesSpentDeadline(t *testing.T) {
	id := testpg.New(t)
	_, s := standalone(t, id, 1)
	ctx, cancel := context.WithTimeout(context.Background(), -1)
	defer cancel()
	called := false
	err := s.Run(ctx, TxOptions{}, func(context.Context, *Tx) error { called = true; return nil })
	require.False(t, called)
	require.True(t, problem.Is(err, problem.DomainBe, "DEADLINE_BUDGET_EXHAUSTED"), "%v", err)
}
