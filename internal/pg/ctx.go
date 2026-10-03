package pg

import (
	"context"
	"fmt"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// inTxKey marks a context that belongs to a unit of work holding an open transaction.
type inTxKey struct{}

func markInTx(ctx context.Context) context.Context { return context.WithValue(ctx, inTxKey{}, true) }

// InTx reports whether ctx belongs to a unit of work that holds an open transaction (P10.6, P8.4).
func InTx(ctx context.Context) bool {
	v, _ := ctx.Value(inTxKey{}).(bool)
	return v
}

// GuardNetwork refuses an outbound call (gRPC, user-plane or third-party HTTP, a direct publish)
// started while the unit of work holds an open transaction: NETWORK_IN_TX, a programming error
// answered as INTERNAL (P8.4, P4.3). what names the call for the log. Outside a transaction it
// returns nil.
func GuardNetwork(ctx context.Context, what string) error {
	if !InTx(ctx) {
		return nil
	}
	return problem.Abort(problem.Wrap(fmt.Errorf("outbound call %s inside an open transaction", what), "NETWORK_IN_TX", nil))
}
