package authz

import (
	"context"
	"net"
	"testing"

	authzv3 "github.com/brickKit/contract-infra-authz/v3/gen/go/be/authz/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type readOnlyMember struct {
	authzv3.UnimplementedViewServiceServer
}

func (readOnlyMember) Verify(_ context.Context, in *authzv3.VerifyRequest) (*authzv3.VerifyResponse, error) {
	return &authzv3.VerifyResponse{Nonce: in.GetNonce(), Instance: "authz-inst-a",
		RecoveryGeneration: &authzv3.RevisionRef{Domain: "authz.recovery", Value: "1"},
		Shards: []*authzv3.ShardAnswer{
			{Shard: "s1", Status: authzv3.ShardStatus_SHARD_STATUS_UNCHANGED, ReadOnly: true},
			{Shard: "s2", Status: authzv3.ShardStatus_SHARD_STATUS_UNCHANGED},
		}}, nil
}

func TestGRPCSourceCarriesReadOnly(t *testing.T) { // P8.11: the contract's ShardAnswer.read_only reaches the holder
	lis := bufconn.Listen(1 << 16)
	srv := grpc.NewServer()
	authzv3.RegisterViewServiceServer(srv, readOnlyMember{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///authz", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	a, err := GRPCSource{Conn: conn}.Verify(context.Background(), VerifyRequest{Component: "demo/orders", Nonce: "n1"})
	require.NoError(t, err)
	require.Len(t, a.Shards, 2)
	assert.True(t, a.Shards[0].ReadOnly)
	assert.False(t, a.Shards[1].ReadOnly)
}
