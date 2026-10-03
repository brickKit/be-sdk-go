package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

// rawBytes sends its payload as it is, so a test can put bytes on the wire that do not decode.
type rawBytes struct{}

func (rawBytes) Marshal(v any) ([]byte, error)      { return v.([]byte), nil }
func (rawBytes) Unmarshal(data []byte, v any) error { *(v.(*[]byte)) = data; return nil }
func (rawBytes) Name() string                       { return "proto" }

// A request whose bytes do not decode is a malformed request the SDK detects: INVALID_ARGUMENT /
// be REQUEST_INVALID (stage-B ruling), logged and counted like any call, and the service never runs.
func TestServerAnswersUndecodableRequestRequestInvalid(t *testing.T) {
	ts := startServer(t, nil)
	conn, err := grpc.NewClient(ts.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	truncated := []byte{0x0a, 0x05, 'a'} // field 1, length 5, one byte present
	var reply []byte
	err = conn.Invoke(withCaller(context.Background()), probepb.Probe_Read_FullMethodName, truncated, &reply,
		grpc.ForceCodec(rawBytes{}))
	st, info, br := errorInfo(t, err)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.NotNil(t, info)
	require.Equal(t, "REQUEST_INVALID", info.GetReason())
	require.Equal(t, "be", info.GetDomain())
	require.NotNil(t, br)
	require.Len(t, br.GetFieldViolations(), 1)
	require.Zero(t, ts.probe.calls.Load(), "component code must not run")
	require.Contains(t, ts.metrics.all(), serverMetric{service: "betest.rpc.v1.Probe", method: "Read", code: "INVALID_ARGUMENT"})

	_, err = rawClient(t, ts.addr).Read(withCaller(context.Background()), &probepb.ProbeRequest{Id: "1"})
	require.NoError(t, err, "a later well-formed request is served")
}

// The streaming half reads the same mark: a message that does not decode fails RecvMsg with
// REQUEST_INVALID.
func TestStreamUndecodableMessageIsRequestInvalid(t *testing.T) {
	ts := startStreamServer(t)
	conn, err := grpc.NewClient(ts.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	cs, err := conn.NewStream(withCaller(context.Background()), &streamDesc.Streams[0],
		"/betest.rpc.v1.StreamProbe/Echo", grpc.ForceCodec(rawBytes{}))
	require.NoError(t, err)
	require.NoError(t, cs.SendMsg([]byte{0x0a, 0x05, 'a'}))
	require.NoError(t, cs.CloseSend())
	var out []byte
	err = cs.RecvMsg(&out)
	st, info, _ := errorInfo(t, err)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Equal(t, "REQUEST_INVALID", info.GetReason())
}
