package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"go.opentelemetry.io/otel/propagation"
)

// Limits of protocol 02's parameters (P6.14, P6.15).
const (
	MaxPayload  = 64 << 10
	MaxHops     = 10
	MaxSubjects = 1000
)

// ClaimStore puts a large payload into object storage and returns its URI (protocol 12 provides it). The forwarder
// calls it outside any transaction.
type ClaimStore interface {
	Put(ctx context.Context, eventID string, data []byte) (uri string, err error)
}

// Producer writes a member's events into its outbox.
type Producer struct {
	source  string
	streams map[string]Stream
	claims  ClaimStore
	written chan struct{} // a commit wrote outbox rows: the member's forwarder starts at once (P6.29)
}

// NewProducer declares the member's streams.
func NewProducer(source string, streams []Stream, claims ClaimStore) (*Producer, error) {
	p := &Producer{source: source, streams: map[string]Stream{}, claims: claims, written: make(chan struct{}, 1)}
	for _, s := range streams {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		p.streams[s.Name] = s
	}
	return p, nil
}

// Streams returns the declared streams.
func (p *Producer) Streams() []Stream {
	out := make([]Stream, 0, len(p.streams))
	for _, s := range p.streams {
		out = append(out, s)
	}
	return out
}

type causeKey struct{}

type cause struct {
	eventID string
	hops    int
}

// WithCause marks ctx as the processing of an event; events emitted under it carry the causation and one more hop.
func WithCause(ctx context.Context, eventID string, hops int) context.Context {
	return context.WithValue(ctx, causeKey{}, cause{eventID, hops})
}

// Emit writes one event into the outbox inside the business transaction (P6.2) and returns its event ID.
func (p *Producer) Emit(ctx context.Context, tx db.Tx, stream string, ev Event) (string, error) {
	s, ok := p.streams[stream]
	if !ok {
		return "", internal(fmt.Errorf("events: stream %q is not declared", stream))
	}
	switch {
	case ev.Type == "" || ev.RecordID == "":
		return "", internal(errors.New("events: an event needs a type and a record ID"))
	case ev.Projection != Full && ev.Projection != Partial && ev.Projection != Delete:
		return "", internal(fmt.Errorf("events: projection %q", ev.Projection))
	case len(ev.Subjects) > MaxSubjects:
		return "", internal(fmt.Errorf("events: %d subjects, at most %d", len(ev.Subjects), MaxSubjects))
	}
	payload, err := json.Marshal(ev.Data)
	if err != nil {
		return "", internal(fmt.Errorf("events: encoding the payload: %w", err))
	}
	if ev.Data == nil {
		payload = []byte("{}")
	}
	if len(payload) > MaxPayload && p.claims == nil {
		return "", internal(fmt.Errorf("events: a %d-byte payload needs object storage for a claim check", len(payload)))
	}
	hops, causation := 0, ""
	if c, ok := ctx.Value(causeKey{}).(cause); ok {
		hops, causation = c.hops+1, c.eventID
	}
	if hops > MaxHops {
		return "", internal(fmt.Errorf("events: %d hops exceed the limit of %d (a causal loop?)", hops, MaxHops))
	}
	trace := propagation.MapCarrier{}
	obs.Propagator.Inject(ctx, trace)
	var traceJSON, subjectsJSON []byte
	if len(trace) > 0 {
		traceJSON, _ = json.Marshal(trace)
	}
	if ev.Subjects != nil {
		subjectsJSON, _ = json.Marshal(ev.Subjects)
	}
	id := ids.New().String()
	var causationArg, legal any
	if causation != "" {
		causationArg = causation
	}
	if ev.LegalEntity != "" {
		legal = ev.LegalEntity
	}
	_, err = tx.Exec(ctx, `INSERT INTO be_outbox (event_id, stream, partition, type, occurred_at, record_id, version,
		projection, legal_entity, subjects, causation, hops, trace, payload, digest)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		id, s.Name, s.PartitionOf(ev.RecordID), ev.Type, time.Now().UTC(), ev.RecordID, ev.Version, string(ev.Projection),
		legal, nullJSON(subjectsJSON), causationArg, hops, nullJSON(traceJSON), payload, Digest(id, payload))
	if err == nil {
		tx.AfterCommit(p.wake)
	}
	return id, err
}

// wake tells the forwarder that a commit wrote rows; one pending signal is enough, the forwarder takes all of them.
func (p *Producer) wake() {
	select {
	case p.written <- struct{}{}:
	default:
	}
}

func nullJSON(b []byte) any {
	if b == nil {
		return nil
	}
	return string(b)
}

func internal(err error) error { return errs.New(errs.ReasonInternal, "").Wrap(err) }
