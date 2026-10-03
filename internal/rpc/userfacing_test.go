package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

// P7.3 (stage-B ruling): a user-facing rpc kept in a contract answers UNAUTHENTICATED / TOKEN_INVALID
// from the runtime before any component code runs; the other methods are served as usual.
func TestServerRefusesUserFacingRPC(t *testing.T) {
	ts := startServer(t, func(c *ServerConfig) {
		c.UserFacing = []string{probepb.Probe_Create_FullMethodName}
	})
	_, err := rawClient(t, ts.addr).Create(withCaller(context.Background()), &probepb.ProbeRequest{Id: "1"})
	st, info, _ := errorInfo(t, err)
	require.Equal(t, codes.Unauthenticated, st.Code())
	require.NotNil(t, info)
	require.Equal(t, "TOKEN_INVALID", info.GetReason())
	require.Equal(t, "be", info.GetDomain())
	require.Zero(t, ts.probe.calls.Load(), "component code must not run")

	_, err = rawClient(t, ts.addr).Read(withCaller(context.Background()), &probepb.ProbeRequest{Id: "1"})
	require.NoError(t, err)
}
