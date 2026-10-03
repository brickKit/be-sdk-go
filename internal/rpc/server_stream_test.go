package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

// streamProbe is a hand-built server-streaming service: Echo receives one BatchRequest and answers
// one ProbeReply naming the caller and whether a deadline was set; with tag "panic" it panics.
var streamDesc = grpc.ServiceDesc{
	ServiceName: "betest.rpc.v1.StreamProbe",
	HandlerType: (*any)(nil),
	Streams: []grpc.StreamDesc{{StreamName: "Echo", ServerStreams: true, Handler: func(_ any, ss grpc.ServerStream) error {
		var req probepb.BatchRequest
		if err := ss.RecvMsg(&req); err != nil {
			return err
		}
		if len(req.GetTags()) == 1 && req.GetTags()[0] == "panic" {
			panic("stream secret")
		}
		c, _ := CallerFrom(ss.Context())
		d, ok := ss.Context().Deadline()
		note := c.Caller
		if ok && time.Until(d) > 9*time.Second && time.Until(d) <= 10*time.Second {
			note += "+floor"
		}
		return ss.SendMsg(&probepb.ProbeReply{Note: note})
	}}},
}

func streamCall(t *testing.T, ctx context.Context, addr string, req *probepb.BatchRequest) (*probepb.ProbeReply, error) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	cs, err := conn.NewStream(ctx, &streamDesc.Streams[0], "/betest.rpc.v1.StreamProbe/Echo")
	if err != nil {
		return nil, err
	}
	if err := cs.SendMsg(req); err != nil {
		return nil, err
	}
	if err := cs.CloseSend(); err != nil {
		return nil, err
	}
	var out probepb.ProbeReply
	return &out, cs.RecvMsg(&out)
}

func startStreamServer(t *testing.T) *testServer {
	return startServerWith(t, nil, func(s *grpc.Server) { s.RegisterService(&streamDesc, struct{}{}) })
}

func TestStreamWithoutCallerIsRejected(t *testing.T) {
	ts := startStreamServer(t)
	_, err := streamCall(t, context.Background(), ts.addr, &probepb.BatchRequest{})
	st, info, _ := errorInfo(t, err)
	require.Equal(t, codes.Unauthenticated, st.Code())
	require.Equal(t, "MISSING_CALLER", info.GetReason())
}

func TestStreamSeesCallerAndDeadlineFloor(t *testing.T) {
	ts := startStreamServer(t)
	out, err := streamCall(t, withCaller(context.Background()), ts.addr, &probepb.BatchRequest{})
	require.NoError(t, err)
	require.Equal(t, "erp/sales+floor", out.GetNote())
}

func TestStreamBatchOverLimit(t *testing.T) {
	ts := startStreamServer(t)
	_, err := streamCall(t, withCaller(context.Background()), ts.addr, &probepb.BatchRequest{Ids: ids(4)})
	st, info, br := errorInfo(t, err)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Equal(t, "BATCH_TOO_LARGE", info.GetReason())
	require.NotNil(t, br)
}

func TestStreamPanicBecomesInternal(t *testing.T) {
	ts := startStreamServer(t)
	_, err := streamCall(t, withCaller(context.Background()), ts.addr, &probepb.BatchRequest{Tags: []string{"panic"}})
	st, info, _ := errorInfo(t, err)
	require.Equal(t, codes.Internal, st.Code())
	require.NotContains(t, st.Message(), "secret")
	require.Equal(t, "INTERNAL", info.GetReason())
}
