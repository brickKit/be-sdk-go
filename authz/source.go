package authz

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/internal/records"
	authzv3 "github.com/brickKit/contract-infra-authz/v3/gen/go/be/authz/v3"
	"google.golang.org/grpc"
)

// VerifyMethod is the authorization member's view verification on its system surface (contract-infra-authz).
const VerifyMethod = authzv3.ViewService_Verify_FullMethodName

// GRPCSource verifies views with the authorization member over the system surface (mTLS, the call policy).
type GRPCSource struct{ Conn *grpc.ClientConn }

// Verify implements Source. A changed shard's bytes are opened as a be.security.view (canonical, its digest, the
// schema; D76); one that fails discards the whole answer, so nothing is renewed by it.
func (g GRPCSource) Verify(ctx context.Context, req VerifyRequest) (VerifyAnswer, error) {
	in := &authzv3.VerifyRequest{Component: req.Component, Nonce: req.Nonce}
	for _, h := range req.Held {
		in.Held = append(in.Held, &authzv3.HeldShard{Shard: h.Shard, Digest: h.Digest})
	}
	out, err := authzv3.NewViewServiceClient(g.Conn).Verify(ctx, in, calls.Idempotent())
	if err != nil {
		return VerifyAnswer{}, err
	}
	rg := out.GetRecoveryGeneration()
	a := VerifyAnswer{Nonce: out.GetNonce(), ComputedAt: out.GetComputedAt(), Instance: out.GetInstance(),
		RecoveryGeneration: Revision{Domain: rg.GetDomain(), Value: rg.GetValue(), Epoch: rg.GetEpoch(), Instance: rg.GetInstance()}}
	for _, s := range out.GetShards() {
		sa := ShardAnswer{Shard: s.GetShard()}
		switch s.GetStatus() {
		case authzv3.ShardStatus_SHARD_STATUS_UNCHANGED:
			sa.Status = "unchanged"
		case authzv3.ShardStatus_SHARD_STATUS_CHANGED:
			sa.Status = "changed"
			view, err := OpenView(s.GetView(), s.GetDigest())
			if err != nil {
				return VerifyAnswer{}, fmt.Errorf("shard %s: %w", s.GetShard(), err)
			}
			if view.Shard != s.GetShard() {
				return VerifyAnswer{}, fmt.Errorf("shard %s: the view is for shard %s", s.GetShard(), view.Shard)
			}
			sa.View = view
		case authzv3.ShardStatus_SHARD_STATUS_CLOSED:
			sa.Status = "closed"
		default:
			return VerifyAnswer{}, fmt.Errorf("shard %s: status %v", s.GetShard(), s.GetStatus())
		}
		a.Shards = append(a.Shards, sa)
	}
	return a, nil
}

// OpenView checks a view shard's bytes and decodes it; its Digest is the digest of those bytes.
func OpenView(raw []byte, digest string) (*Shard, error) {
	if _, err := records.Open("be.security.view", raw, digest); err != nil {
		return nil, err
	}
	var s Shard
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("the view: %w", err)
	}
	s.Digest = digest
	return &s, nil
}
