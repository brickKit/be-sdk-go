package besdk

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
)

// Event is one business event (P12): what a producer publishes with tx.Publish and what a handler
// receives. ID and the read-only fields are filled by the SDK.
type Event struct {
	ID          string // UUIDv7, = ce-id; filled at publish
	Subject     string // e.g. "erp.sales.order.confirmed.v1"
	AggregateID string // ce-subject
	Version     int64  // the aggregate's version after this change; one sequence per aggregate type
	Payload     any    // published: marshalled to JSON and validated against the contract (P12.2)

	// Read-only, filled when consuming.
	AggregateType, Source, TraceParent, CausationID, LegalEntity string
	OccurredAt                                                   time.Time
	HopCount                                                     int
	Delivery                                                     int
}

// Events declares what a component publishes and consumes (P12.16 lists the same subjects in
// component.yaml).
type Events struct {
	Publishes []string
	Subscribe []Subscription
}

// Subscription consumes one subject through the component's durable (P12.5). Exactly one of Apply
// (inside the cursor's transaction, local writes only) and Run (outside any transaction, may call the
// network, idempotent on a business key) is set (P12.7).
type Subscription struct {
	Subject  string
	Consumer string // cursor name (projection), default ""
	// AggregateType and TransactionDocument are the producer contract's x-aggregate-type and
	// x-transaction-document: when set, ce-aggregatetype must equal it and a missing legal entity is
	// dead-lettered (P11.8); empty takes any well-formed aggregate type.
	AggregateType       string
	TransactionDocument bool
	Apply               func(ctx context.Context, tx *Tx, ev Event) error // local writes, in the cursor's tx
	Run                 func(ctx context.Context, ev Event) error         // outside any transaction
	MaxDeliver          int                                               // 0 = 8; EVENTS_MAX_DELIVER, when set, overrides (P12.5)
	Backoff             []time.Duration                                   // nak delays, empty = 1s…1h; EVENTS_BACKOFF, when set, overrides
	Concurrency         int                                               // default 4
}

// Decode unmarshals an event's payload into T; a payload that does not decode is permanent.
func Decode[T any](ev Event) (T, error) {
	var v T
	raw, ok := ev.Payload.(json.RawMessage)
	if !ok {
		b, err := json.Marshal(ev.Payload)
		if err != nil {
			return v, Permanent(err)
		}
		raw = b
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, Permanent(fmt.Errorf("decode %s: %w", ev.Subject, err))
	}
	return v, nil
}

func eventFrom(e envelope.Event) Event {
	return Event{ID: e.ID, Subject: e.Subject, AggregateID: e.AggregateID, Version: e.Version,
		Payload: e.Payload, AggregateType: e.AggregateType, Source: e.Source, TraceParent: e.TraceParent,
		CausationID: e.CausationID, LegalEntity: e.LegalEntity, OccurredAt: e.OccurredAt,
		HopCount: e.HopCount, Delivery: e.Delivery}
}

type handlingKey struct{}

// withHandling marks ctx as handling ev, so events published from it carry causation and hop (P12.8).
func withHandling(ctx context.Context, ev envelope.Event) context.Context {
	return context.WithValue(ctx, handlingKey{}, envelope.Origin{Kind: envelope.OriginEvent,
		CausationID: ev.ID, HopCount: ev.HopCount})
}

// originOf is the publishing context of ctx: a handled event, else a request (hop 0).
func originOf(ctx context.Context) envelope.Origin {
	if o, ok := ctx.Value(handlingKey{}).(envelope.Origin); ok {
		return o
	}
	return envelope.Origin{Kind: envelope.OriginRequest}
}
