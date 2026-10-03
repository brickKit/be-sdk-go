package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// At the end of the outbound budget gRPC does not always report DEADLINE_EXCEEDED: a stream
// reset at that moment comes back as CANCELLED, INTERNAL or UNAVAILABLE. The budget ran out
// either way, so the caller answers 504 and not 499, 500 or 503 (P7.7, P3.4). The conformance
// stub answered 500 to a hung peer once in 17 runs for exactly this reason.
func TestClientBudgetEndIsDeadlineWhateverCodeGRPCReports(t *testing.T) {
	for _, code := range []codes.Code{codes.Canceled, codes.Internal, codes.Unknown, codes.Unavailable} {
		ch := &clientChain{dep: "d"}
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		atBudgetEnd := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			<-ctx.Done() // the outbound budget (remaining − 50 ms) has ended
			return status.Error(code, "stream terminated")
		}
		err := ch.deadlineUnary(ctx, "/probe.Probe/Read", nil, nil, nil, atBudgetEnd)
		cancel()
		require.True(t, problem.Is(err, "be", "DEADLINE_BUDGET_EXHAUSTED"), "%v at the budget's end: got %v", code, err)
		require.Equal(t, codes.DeadlineExceeded, problem.From(err).Code, "%v at the budget's end", code)
	}
}

// An answer that arrives before the budget ends is the dependency's answer and is kept.
func TestClientKeepsAnErrorThatArrivesBeforeTheBudgetEnds(t *testing.T) {
	ch := &clientChain{dep: "d"}
	early := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.Internal, "boom")
	}
	err := ch.deadlineUnary(context.Background(), "/probe.Probe/Read", nil, nil, nil, early)
	require.Equal(t, codes.Internal, problem.From(err).Code)
	require.False(t, problem.Is(err, "be", "DEADLINE_BUDGET_EXHAUSTED"), "got %v", err)
}
