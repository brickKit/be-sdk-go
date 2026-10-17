package authz

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// VerifyMethod is the authorization member's view verification on its system surface. Until the family contract is
// generated the request and answer travel as their canonical JSON records (security 09a) in a BytesValue.
const VerifyMethod = "/be.authz.v3.ViewService/Verify"

// GRPCSource verifies views with the authorization member over the system surface (mTLS, the call policy).
type GRPCSource struct{ Conn *grpc.ClientConn }

// Verify implements Source.
func (g GRPCSource) Verify(ctx context.Context, req VerifyRequest) (VerifyAnswer, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return VerifyAnswer{}, err
	}
	out := new(wrapperspb.BytesValue)
	if err := g.Conn.Invoke(ctx, VerifyMethod, wrapperspb.Bytes(body), out, calls.Idempotent()); err != nil {
		return VerifyAnswer{}, err
	}
	var a VerifyAnswer
	if err := json.Unmarshal(out.Value, &a); err != nil {
		return VerifyAnswer{}, fmt.Errorf("verify answer: %w", err)
	}
	return a, nil
}
