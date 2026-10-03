package rpc

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// Metadata keys of the system plane (P7 "Metadata"). gRPC metadata keys are lower case.
const (
	MDCaller    = "be-caller"    // the calling component's or member's ID (P7.2)
	MDActorSub  = "be-actor-sub" // the platform sub of the user behind the call, read at call time
	MDActorAct  = "be-actor-act" // the token's act chain as JSON
	MDRequestID = "x-request-id" // the inbound request's ID
)

// Caller is the system principal of an inbound call (P7.3): who called, and the user and delegation
// chain the call was made for. It is recorded (logs, audit), never used to grant access.
type Caller struct {
	Caller   string // be-caller
	ActorSub string // be-actor-sub; empty for background work
	Act      string // be-actor-act, JSON; empty without delegation
}

type callerKey struct{}

// CallerFrom returns the system principal of the inbound gRPC call ctx belongs to (P7.3); false
// outside an inbound call (and for the exempt health service).
func CallerFrom(ctx context.Context) (Caller, bool) {
	c, ok := ctx.Value(callerKey{}).(Caller)
	return c, ok
}

func withCallerValue(ctx context.Context, c Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, c)
}

// firstMD is the first value of an incoming metadata key, or "".
func firstMD(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}
