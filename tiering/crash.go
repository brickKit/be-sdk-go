package tiering

import (
	"context"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// The crash points of the long simulation (assembly protocol 11's replacement checks: a crash injected at every state
// transition, the final state consistent). Every background transaction of this package (the jobs' steps; a request's
// own transaction, a hold or an export's status, is one atomic step) goes through tx, which calls crash just before it
// commits ("<point>/commit": a process that dies there leaves the transaction open, so it rolls back) and after it
// committed ("<point>/done"); each object the jobs write or delete calls it after the call ("<point>/done"). In
// production crash does nothing; the simulation (simulation_test.go) sets it to end the process at a chosen point.
var crash = func(point string) {}

// tx runs fn in a background transaction with the crash points around its commit.
func (c *Catalog) tx(ctx context.Context, point string, fn func(context.Context, db.Tx) error, opts ...db.TxOption) error {
	err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := fn(ctx, tx); err != nil {
			return err
		}
		crash(point + "/commit")
		return nil
	}, opts...)
	if err == nil {
		crash(point + "/done")
	}
	return err
}
