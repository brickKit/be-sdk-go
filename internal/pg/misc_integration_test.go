package pg

import (
	"context"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

// A panicking body rolls back and gives its connection and budget slot back.
func TestRunPanicReleasesConnectionAndBudget(t *testing.T) {
	id := testpg.New(t)
	asOwner(t, id, `CREATE TABLE note (n int)`)
	p, _ := standalone(t, id, 1)
	s := NewStore(p, StoreConfig{ComponentID: "c/x", Role: id.User, Schema: id.Schema, Budget: 1, AcquireTimeout: 500 * time.Millisecond})
	require.Panics(t, func() {
		_ = s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
			_, _ = tx.ExecContext(ctx, `INSERT INTO note VALUES (1)`)
			panic("boom")
		})
	})
	var n int
	require.NoError(t, s.Run(within(t, 10e9), TxOptions{}, func(ctx context.Context, tx *Tx) error {
		return tx.QueryRowContext(ctx, `SELECT count(*) FROM note`).Scan(&n)
	}))
	require.Zero(t, n)
}

// PostgreSQL refusing a new connection with 53300 is DB_TOO_MANY_CONNECTIONS (P10.4).
func TestRunTooManyConnections(t *testing.T) {
	id := testpg.New(t)
	testpg.Exec(t, testpg.Open(t, id.SuperDSN), `ALTER ROLE `+id.User+` CONNECTION LIMIT 0`)
	_, s := standalone(t, id, 1)
	err := s.Run(within(t, 10e9), TxOptions{}, func(context.Context, *Tx) error { return nil })
	require.True(t, problem.Is(err, problem.DomainBe, "DB_TOO_MANY_CONNECTIONS"), "%v", err)
}

func TestServerVersionNum(t *testing.T) {
	for major, want := range map[string]int{"16": 160000, "14": 140000} {
		id := testpg.NewOn(t, major)
		p, _ := standalone(t, id, 1)
		v, err := p.ServerVersionNum(within(t, 10e9))
		require.NoError(t, err)
		require.GreaterOrEqual(t, v, want)
		require.Less(t, v, want+10000)
	}
}

// Inside a shell the probe runs as the member's PG_USER and passes (P10.7, P19.5).
func TestProbeThroughShellRole(t *testing.T) {
	a := testpg.New(t)
	_, err := MigrateUp(within(t, 60e9), migrateConfig(a, widgetFS(), nil))
	require.NoError(t, err)
	role, password := testpg.ShellRole(t, a)
	p := poolFor(t, a, role, password, 1)
	s := NewStore(p, StoreConfig{ComponentID: "conformance/widget", Role: a.User, Schema: a.Schema})
	r := Probe(within(t, 10e9), s, a.Owner, true)
	require.NoError(t, r.Err)
	require.NoError(t, r.Fatal)
	require.Empty(t, r.IdentityProblems)
	require.Equal(t, uint(2), r.ComponentVersion)
	require.Equal(t, 1, r.PlatformVersion)
}
