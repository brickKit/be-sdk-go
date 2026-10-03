package besdk

import (
	"context"
	"fmt"

	"github.com/brickKit/be-sdk-go/internal/events"
)

// Publish writes ev to the outbox in this transaction (P12.1): it is published only if the
// transaction commits, after the bus confirmed it, and never lost while the bus is down. The subject
// must be one Module.Events.Publishes declares and the contract lists; the payload is validated
// against the contract (P12.2). Causation and hop count come from the context: an event handled or
// a job run, else 0 (P12.8).
func (tx *Tx) Publish(ctx context.Context, ev Event) error {
	p := tx.rt.deps.producer
	if p == nil {
		return fmt.Errorf("besdk: Publish needs Module.Events.Publishes and contracts/events (P12.2)")
	}
	_, err := events.Write(ctx, tx.Tx, p, events.Outgoing{Subject: ev.Subject, AggregateID: ev.AggregateID,
		Version: ev.Version, Payload: ev.Payload, OccurredAt: ev.OccurredAt, Origin: originOf(ctx)})
	return err
}

// Permanent wraps err so the event is dead-lettered at once instead of redelivered (P12.7).
func Permanent(err error) error { return events.Permanent(err) }

// IsPermanent reports whether err was wrapped by Permanent.
func IsPermanent(err error) bool { return events.IsPermanent(err) }
