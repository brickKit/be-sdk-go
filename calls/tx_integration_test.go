//go:build integration

package calls

import (
	"context"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoCallsInsideATransaction(t *testing.T) { // P4.20
	host, port := pgtest.EnsureDatabase(t, pgtest.Database)
	pgtest.Member(t, "callsdb", "")
	env := pgtest.MemberEnv(host, port, "callsdb")
	values, problems := config.Parse(db.ServeKeys(), config.Map(env))
	require.Empty(t, problems)
	s := db.SettingsFrom(values)
	pool, err := db.Open(context.Background(), db.PoolConfig{Target: s.Target, User: s.User, Password: s.Password,
		AppName: "demo/calls", MaxConns: 2, Prepared: true})
	require.NoError(t, err)
	defer pool.Close()
	d := db.NewDB("demo/calls", pool, s)

	dep := newDep(t)
	sent := false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var callErr error
	require.NoError(t, d.Tx(ctx, func(ctx context.Context, _ db.Tx) error {
		callErr = dep.Do(ctx, true, func(context.Context) Result { sent = true; return Result{Code: "OK"} })
		return nil
	}))
	assert.False(t, sent)
	assert.True(t, errs.Is(callErr, "be", errs.ReasonInternal))
	assert.ErrorContains(t, callErr, "NETWORK_IN_TX")
}
