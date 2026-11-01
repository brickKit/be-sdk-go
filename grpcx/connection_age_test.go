package grpcx_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
)

// P4.30: the server ends every connection after its maximum age, gracefully; the caller connects again (and so
// resolves the name again, reaching other replicas over time) without a single call failing.
func TestTheServerEndsEachConnectionAfterItsAgeAndNoCallFails(t *testing.T) {
	var mu sync.Mutex
	peers := map[string]bool{}
	var s grpcx.Services
	s.Register(&echoDesc, sayFunc(func(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if p, ok := peer.FromContext(ctx); ok {
			mu.Lock()
			peers[p.Addr.String()] = true
			mu.Unlock()
		}
		return in, nil
	}), map[string]grpcx.Method{"Say": {Operation: "demo/c.say", Callers: callers}})
	d := deps(t, "demo/c")
	d.ConnectionAge = 200 * time.Millisecond
	srv, err := grpcx.NewServer(d, &s)
	require.NoError(t, err)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	conn, err := member(t, "demo/a").GRPC("demo/c", ln.Addr().String())
	require.NoError(t, err)
	for end := time.Now().Add(1200 * time.Millisecond); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := conn.Invoke(ctx, "/demo.Echo/Say", wrapperspb.String("x"), new(wrapperspb.StringValue), calls.Idempotent())
		cancel()
		require.NoError(t, err, "a call failed while the server ended its connection")
	}
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, len(peers), 3, "connections seen by the server in 1.2 s with a 200 ms age: %v", peers)
}
