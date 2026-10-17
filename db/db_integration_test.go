//go:build integration

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDB = pgtest.Database

var (
	admin = pgtest.Admin
	exec  = pgtest.Exec
)

// fixture: database be_sdk_test with two members (alpha, beta), each an owner and a runtime role and a schema whose
// "items" tables differ in shape, and a shell login role holding both runtime roles WITH INHERIT FALSE.
func fixture(t *testing.T) (host string, port int) {
	t.Helper()
	host, port = pgtest.EnsureDatabase(t, testDB)
	c := admin(t, testDB)
	exec(t, c,
		"DROP SCHEMA IF EXISTS alpha CASCADE", "DROP SCHEMA IF EXISTS beta CASCADE",
		`DO $$ BEGIN
		  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sdk_alpha_owner') THEN CREATE ROLE sdk_alpha_owner NOLOGIN; END IF;
		  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sdk_beta_owner') THEN CREATE ROLE sdk_beta_owner NOLOGIN; END IF;
		  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sdk_alpha_rw') THEN CREATE ROLE sdk_alpha_rw LOGIN PASSWORD 'alpha-pw'; END IF;
		  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sdk_beta_rw') THEN CREATE ROLE sdk_beta_rw LOGIN PASSWORD 'beta-pw'; END IF;
		  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='sdk_shell') THEN CREATE ROLE sdk_shell LOGIN PASSWORD 'shell-pw'; END IF;
		END $$`,
		"GRANT sdk_alpha_rw TO sdk_shell WITH INHERIT FALSE, SET TRUE",
		"GRANT sdk_beta_rw TO sdk_shell WITH INHERIT FALSE, SET TRUE",
		"CREATE SCHEMA alpha AUTHORIZATION sdk_alpha_owner", "CREATE SCHEMA beta AUTHORIZATION sdk_beta_owner",
		"CREATE TABLE alpha.items(id int PRIMARY KEY, name text)", "ALTER TABLE alpha.items OWNER TO sdk_alpha_owner",
		"CREATE TABLE beta.items(id int PRIMARY KEY)", "ALTER TABLE beta.items OWNER TO sdk_beta_owner",
		"INSERT INTO alpha.items VALUES (1, 'a1'), (2, 'a2')", "INSERT INTO beta.items VALUES (10), (20), (30)",
		"GRANT USAGE ON SCHEMA alpha TO sdk_alpha_rw", "GRANT SELECT, INSERT, UPDATE ON alpha.items TO sdk_alpha_rw",
		"GRANT USAGE ON SCHEMA beta TO sdk_beta_rw", "GRANT SELECT, INSERT, UPDATE ON beta.items TO sdk_beta_rw",
	)
	return host, port
}

func secret(t *testing.T, v string) *config.Secret {
	vals, problems := config.Parse([]config.Key{{Name: "S", Type: config.String, Required: true, Secret: true}}, config.Map{"S": v})
	require.Empty(t, problems)
	return vals.Secret("S")
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func settings(host string, port int, member string, fg int) Settings {
	return Settings{Target: Target{Host: host, Port: port, Database: testDB}, User: "sdk_" + member + "_rw", Schema: member,
		Prepared: true, Timeouts: Timeouts{Statement: 5 * time.Second, Lock: 300 * time.Millisecond, IdleInTx: 10 * time.Second},
		Foreground: fg, Background: 2}
}

// sharedPool is the shell's pool: login role sdk_shell, one connection, so both members use the same session.
func sharedPool(t *testing.T, host string, port int, conns int32) *Pool {
	p, err := Open(context.Background(), PoolConfig{Target: Target{Host: host, Port: port, Database: testDB}, User: "sdk_shell",
		Password: secret(t, "shell-pw"), AppName: "be/go-core", MaxConns: conns, Prepared: true})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	return p
}

type sessionState struct{ user, path, timeout, app string }

func state(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (sessionState, error) {
	var s sessionState
	err := q.QueryRow(ctx, "SELECT current_user, current_setting('search_path'), current_setting('statement_timeout'), current_setting('application_name')").
		Scan(&s.user, &s.path, &s.timeout, &s.app)
	return s, err
}

func TestRoleAndSchemaArePerTransactionAndNeverLeak(t *testing.T) {
	host, port := fixture(t)
	pool := sharedPool(t, host, port, 1)
	a, b := NewDB("demo/alpha", pool, settings(host, port, "alpha", 4)), NewDB("demo/beta", pool, settings(host, port, "beta", 4))
	ctx := context.Background()
	for round := 0; round < 3; round++ {
		for _, m := range []struct {
			db         *DB
			user, path string
			count      int
		}{{a, "sdk_alpha_rw", `"alpha"`, 2}, {b, "sdk_beta_rw", `"beta"`, 3}} {
			require.NoError(t, m.db.Tx(ctx, func(ctx context.Context, tx Tx) error {
				assert.True(t, InTx(ctx))
				s, err := state(ctx, tx)
				require.NoError(t, err)
				assert.Equal(t, m.user, s.user)
				assert.Equal(t, m.path, s.path)
				assert.Equal(t, "5s", s.timeout)
				var n int
				require.NoError(t, tx.QueryRow(ctx, "SELECT count(*) FROM items").Scan(&n), "unqualified names resolve in the member's schema")
				assert.Equal(t, m.count, n)
				return nil
			}))
			s, err := state(ctx, pool.pool)
			require.NoError(t, err)
			assert.Equal(t, sessionState{"sdk_shell", `"$user", public`, "0", "be/go-core"}, s, "nothing stays on the pooled connection")
		}
	}
	// the shell's login role holds the members' roles WITH INHERIT FALSE: outside the switch it sees nothing
	_, err := pool.pool.Exec(ctx, "SELECT count(*) FROM alpha.items")
	assert.ErrorContains(t, err, "permission denied")
}

func TestSameSQLOfTwoMembersNeverSharesACachedStatement(t *testing.T) {
	host, port := fixture(t)
	pool := sharedPool(t, host, port, 1)
	a, b := NewDB("demo/alpha", pool, settings(host, port, "alpha", 4)), NewDB("demo/beta", pool, settings(host, port, "beta", 4))
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		for _, m := range []*DB{a, b} {
			require.NoError(t, m.Tx(ctx, func(ctx context.Context, tx Tx) error {
				rows, err := tx.Query(ctx, "SELECT * FROM items ORDER BY id")
				if err != nil {
					return err
				}
				rows.Close()
				return rows.Err()
			}), "same SQL, different table shapes, one connection")
		}
	}
	// without the member prefix the same statement would be served from the cache built for the other shape
	conn, err := pool.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	run := func(role, schema string) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "SELECT set_config('role',$1,true), set_config('search_path',$2,true)", role, schema); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, "SELECT * FROM items")
		if err != nil {
			return err
		}
		rows.Close()
		return rows.Err()
	}
	require.NoError(t, run("sdk_alpha_rw", "alpha"))
	err = run("sdk_beta_rw", "beta")
	require.Error(t, err, "the hazard the prefix avoids")
	assert.Contains(t, err.Error(), "cached plan must not change result type")
}

func TestTimeoutsNestingAndRetries(t *testing.T) {
	host, port := fixture(t)
	pool := sharedPool(t, host, port, 4)
	a := NewDB("demo/alpha", pool, settings(host, port, "alpha", 4))
	ctx := context.Background()

	err := a.Tx(ctx, func(ctx context.Context, tx Tx) error {
		_, err := tx.Exec(ctx, "SELECT pg_sleep(0.5)")
		return err
	}, StatementTimeout(100*time.Millisecond))
	assert.True(t, errs.Is(err, "be", errs.ReasonStatementTimeout), "%v", err)

	locked := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = a.Tx(ctx, func(ctx context.Context, tx Tx) error {
			if _, err := tx.Exec(ctx, "UPDATE items SET name='held' WHERE id=1"); err != nil {
				return err
			}
			close(locked)
			<-release
			return errors.New("roll back")
		})
	}()
	<-locked
	err = a.Tx(ctx, func(ctx context.Context, tx Tx) error {
		_, err := tx.Exec(ctx, "UPDATE items SET name='second' WHERE id=1")
		return err
	})
	close(release)
	assert.True(t, errs.Is(err, "be", errs.ReasonLockTimeout), "%v", err)

	err = a.Tx(ctx, func(ctx context.Context, tx Tx) error {
		return a.Tx(ctx, func(context.Context, Tx) error { return nil })
	})
	assert.True(t, errs.Is(err, "be", errs.ReasonInternal))
	assert.ErrorContains(t, err, "NESTED_TX")

	// write skew under SERIALIZABLE: one of the two fails with 40001 and is redone as a whole
	var attempts atomic.Int32
	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = a.Tx(ctx, func(ctx context.Context, tx Tx) error {
				attempts.Add(1)
				var n int
				if err := tx.QueryRow(ctx, "SELECT count(*) FROM items WHERE id > 100").Scan(&n); err != nil {
					return err
				}
				time.Sleep(100 * time.Millisecond)
				_, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1, 'skew')", 100+n+1+i*1000)
				return err
			}, Serializable())
		}(i)
	}
	wg.Wait()
	assert.NoError(t, results[0])
	assert.NoError(t, results[1])
	assert.Greater(t, attempts.Load(), int32(2), "a serialization failure was retried")
}

func TestForegroundBudgetAndDurableCheck(t *testing.T) {
	host, port := fixture(t)
	pool := sharedPool(t, host, port, 4)
	a := NewDB("demo/alpha", pool, settings(host, port, "alpha", 1))
	ctx := context.Background()
	hold := make(chan struct{})
	held := make(chan struct{})
	go func() {
		_ = a.Tx(ctx, func(context.Context, Tx) error { close(held); <-hold; return nil })
	}()
	<-held
	start := time.Now()
	err := a.Tx(ctx, func(context.Context, Tx) error { return nil })
	close(hold)
	assert.True(t, errs.Is(err, "be", errs.ReasonServiceBusy), "%v", err)
	assert.InDelta(t, ForegroundWait.Seconds(), time.Since(start).Seconds(), 0.5)
	var e *errs.Error
	require.True(t, errors.As(err, &e))
	assert.Equal(t, "db_pool", e.Metadata["resource"])
	assert.Equal(t, time.Second, e.RetryAfter)

	require.NoError(t, pool.CheckDurable(ctx))
	pgtest.EnsureDatabase(t, "be_sdk_test_async")
	exec(t, admin(t, ""), "ALTER DATABASE be_sdk_test_async SET synchronous_commit = off",
		"GRANT CONNECT ON DATABASE be_sdk_test_async TO sdk_shell")
	async, err := Open(ctx, PoolConfig{Target: Target{Host: host, Port: port, Database: "be_sdk_test_async"}, User: "sdk_shell",
		Password: secret(t, "shell-pw"), AppName: "t", MaxConns: 1, Prepared: true})
	require.NoError(t, err)
	defer async.Close()
	err = async.CheckDurable(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `synchronous_commit is "off"`)
}

func TestManyMembersConcurrentlyOnOnePool(t *testing.T) {
	host, port := fixture(t)
	pool := sharedPool(t, host, port, 3)
	members := map[string]*DB{"sdk_alpha_rw": NewDB("demo/alpha", pool, settings(host, port, "alpha", 8)),
		"sdk_beta_rw": NewDB("demo/beta", pool, settings(host, port, "beta", 8))}
	ctx := context.Background()
	var wg sync.WaitGroup
	var wrong atomic.Int32
	for i := 0; i < 40; i++ {
		for role, m := range members {
			wg.Add(1)
			go func(role string, m *DB) {
				defer wg.Done()
				err := m.Tx(ctx, func(ctx context.Context, tx Tx) error {
					s, err := state(ctx, tx)
					if err != nil {
						return err
					}
					if s.user != role || !strings.Contains(s.path, strings.TrimSuffix(strings.TrimPrefix(role, "sdk_"), "_rw")) {
						wrong.Add(1)
					}
					return nil
				})
				if err != nil && !errs.Is(err, "be", errs.ReasonServiceBusy) {
					t.Error(err)
				}
			}(role, m)
		}
	}
	wg.Wait()
	assert.Zero(t, wrong.Load(), "no transaction ever ran with another member's role or schema")
}
