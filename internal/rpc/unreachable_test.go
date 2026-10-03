package rpc

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

// A dependency that refuses the connection is UNAVAILABLE / be DEPENDENCY_UNAVAILABLE naming its
// component ID (stage-B ruling; P4 rc.2: refused, reset, or UNAVAILABLE without an ErrorInfo).
func TestClientUnreachableDependencyIsDependencyUnavailable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close()) // nothing listens there now
	_, err = probeClient(t, newTestConns(t, nil), "erp/inventory", addr).Create(context.Background(), &probepb.ProbeRequest{})
	var pe *problem.Error
	require.True(t, errors.As(err, &pe), "got %T %v", err, err)
	require.Equal(t, codes.Unavailable, pe.Code)
	require.Equal(t, problem.DomainBe, pe.Domain)
	require.Equal(t, "DEPENDENCY_UNAVAILABLE", pe.Reason)
	require.Equal(t, map[string]string{"dependency": "erp/inventory"}, pe.Metadata)
}

// A dependency that answered UNAVAILABLE with an ErrorInfo of its own is relayed as it is (P4.9).
func TestClientRelaysADependencysOwnUnavailable(t *testing.T) {
	ts := startServer(t, nil)
	failFirst(ts, 100)
	_, err := probeClient(t, newTestConns(t, nil), "erp/inventory", ts.addr).Create(context.Background(), &probepb.ProbeRequest{})
	pe := problem.From(err)
	require.Equal(t, codes.Unavailable, pe.Code)
	require.Equal(t, "NOT_READY", pe.Reason)
}
