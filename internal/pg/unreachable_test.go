package pg

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// requireDBUnavailable checks the stage-B ruling: an unreachable database is UNAVAILABLE /
// be DEPENDENCY_UNAVAILABLE with metadata.dependency = "db".
func requireDBUnavailable(t *testing.T, err error) {
	t.Helper()
	var pe *problem.Error
	require.ErrorAs(t, err, &pe, "%v", err)
	require.Equal(t, codes.Unavailable, pe.Code, "%v", err)
	require.Equal(t, "DEPENDENCY_UNAVAILABLE", pe.Reason, "%v", err)
	require.Equal(t, map[string]string{"dependency": "db"}, pe.Metadata)
}

func unreachableStore(t *testing.T, host string, acquire time.Duration) *Store {
	t.Helper()
	p, err := OpenPool(PoolConfig{Host: host, Port: 1, Database: "d", User: "u", Password: func() string { return "x" }})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	return NewStore(p, StoreConfig{ComponentID: "erp/x", Role: "r", Schema: "s", AcquireTimeout: acquire})
}

func TestRunOnRefusedDatabaseIsDependencyUnavailable(t *testing.T) {
	s := unreachableStore(t, "127.0.0.1", time.Second)
	err := s.Run(context.Background(), TxOptions{}, func(context.Context, *Tx) error { return nil })
	requireDBUnavailable(t, err)
	_, err = s.pool.ServerVersionNum(context.Background())
	requireDBUnavailable(t, err)
}

// A connect that runs out of the acquire wait (the host does not answer) is the database being
// unreachable, not the pool being exhausted.
func TestRunOnSilentDatabaseIsDependencyUnavailable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() { // accept and never answer the startup message
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	p, err := OpenPool(PoolConfig{Host: "127.0.0.1", Port: addr.Port, Database: "d", User: "u", SSLMode: "disable",
		Password: func() string { return "x" }})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })
	s := NewStore(p, StoreConfig{ComponentID: "erp/x", Role: "r", Schema: "s", AcquireTimeout: 200 * time.Millisecond})
	err = s.Run(context.Background(), TxOptions{}, func(context.Context, *Tx) error { return nil })
	requireDBUnavailable(t, err)
}

func TestClassifyConnectionLossIsDependencyUnavailable(t *testing.T) {
	for _, err := range []error{
		pgErr("08006"), pgErr("08003"), pgErr("57P01"),
		fmt.Errorf("read: %w", io.ErrUnexpectedEOF),
		fmt.Errorf("exec: %w", driver.ErrBadConn),
		&net.OpError{Op: "read", Net: "tcp", Err: fmt.Errorf("connection reset by peer")},
		&pgconn.ConnectError{},
	} {
		d := classifyAttempt(err, 1, 3, nil)
		require.False(t, d.retry)
		requireDBUnavailable(t, d.err)
		require.ErrorIs(t, d.err, err, "the original stays the cause")
	}
}
