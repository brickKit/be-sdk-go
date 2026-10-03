package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"go.opentelemetry.io/otel/propagation"
)

// Payload size limits (P12.2): a payload stays under 64 KiB (larger content goes through a claim
// check); 1 MiB is the hard limit enforced at publish.
const (
	PayloadWarnBytes  = 64 << 10
	PayloadLimitBytes = 1 << 20
)

// Producer is the publishing component, in a shell the member (P12 envelope: ce-source,
// ce-dataschema; P12.2 contract; P12.16 declared subjects).
type Producer struct {
	ComponentID string                        // ce-source
	Version     string                        // the component's version, x.y.z (ce-dataschema)
	Contract    *Contract                     // contracts/events/*.events.json
	Publishes   []string                      // the subjects the component declares it publishes
	Propagator  propagation.TextMapPropagator // W3C trace context of the current span; nil = none
	Logger      *slog.Logger                  // nil discards
	Now         func() time.Time              // occurred_at when the event leaves it zero; nil = time.Now
}

// Outgoing is one event a component publishes (P12.1).
type Outgoing struct {
	Subject     string          // ce-type
	AggregateID string          // ce-subject
	Version     int64           // ce-aggregateversion, ≥ 1, one sequence per aggregate type (P12.3)
	Payload     any             // marshalled to JSON; json.RawMessage and []byte are taken as they are
	OccurredAt  time.Time       // ce-time; zero = now
	Origin      envelope.Origin // what the event is published in: causation and hop (P12.8)
}

// outboxRow is one besdk_outbox row ready to insert.
type outboxRow struct {
	id, subject, aggregateType, aggregateID string
	createdAt, occurredAt                   time.Time
	aggregateVersion                        int64
	traceParent, causationID                string
	hopCount                                int
	headers, payload                        []byte
}

const insertOutbox = `INSERT INTO besdk_outbox (id, created_at, subject, aggregate_type, aggregate_id,
 aggregate_version, occurred_at, traceparent, causation_id, hop_count, headers, payload)
 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb)`

// Write adds ev to the outbox inside the caller's business transaction and returns its id, the
// ce-id (P12.1). The subject must be declared as published and be in the contract, the payload must
// match the contract and stay ≤ 1 MiB (P12.2), a transaction-document event needs its legal entity
// (P11.8); causation and hop come from ev.Origin (P12.8). A programming error of the component is
// a *problem.Error INTERNAL with a clear cause; a database error is returned wrapped so that
// Store.Run classifies it by SQLSTATE (and re-runs the transaction on 40001 / 40P01, P10.4).
func Write(ctx context.Context, tx *pg.Tx, p *Producer, ev Outgoing) (string, error) {
	r, err := p.prepare(ctx, ev)
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, insertOutbox, r.id, r.createdAt, r.subject, r.aggregateType, r.aggregateID,
		r.aggregateVersion, r.occurredAt, r.traceParent, r.causationID, r.hopCount, string(r.headers), string(r.payload))
	if err != nil {
		// Not converted here: Store.Run classifies it by SQLSTATE and re-runs on 40001 / 40P01.
		return "", fmt.Errorf("events: insert outbox row %s: %w", ev.Subject, err)
	}
	return r.id, nil
}

// prepare validates ev and builds its row; it does no I/O.
func (p *Producer) prepare(ctx context.Context, ev Outgoing) (outboxRow, error) {
	if !slices.Contains(p.Publishes, ev.Subject) {
		return outboxRow{}, programming("subject %s is not declared as published by %s", ev.Subject, p.ComponentID)
	}
	def, ok := p.Contract.Lookup(ev.Subject)
	if !ok {
		return outboxRow{}, programming("subject %s is not in the events contract of %s", ev.Subject, p.ComponentID)
	}
	payload, err := p.marshal(ev)
	if err != nil {
		return outboxRow{}, err
	}
	if err := def.Validate(payload); err != nil {
		return outboxRow{}, programming("%v", err)
	}
	id := envelope.NewID()
	causation, hop := envelope.Derive(ev.Origin)
	r := outboxRow{id: id.String(), createdAt: envelope.IDTime(id), subject: ev.Subject,
		aggregateType: def.AggregateType, aggregateID: ev.AggregateID, aggregateVersion: ev.Version,
		occurredAt: p.occurredAt(ev), traceParent: p.traceParent(ctx), causationID: causation,
		hopCount: hop, payload: payload}
	h, err := envelope.Headers(envelope.Producer{ComponentID: p.ComponentID, Version: p.Version, EventsFile: def.File},
		r.envelopeRow(), def.TransactionDocument)
	if err != nil {
		return outboxRow{}, programming("event %s: %v", ev.Subject, err)
	}
	r.headers = storedHeaders(h)
	return r, nil
}

// marshal turns the payload into JSON and enforces the size limits (P12.2).
func (p *Producer) marshal(ev Outgoing) ([]byte, error) {
	var b []byte
	switch v := ev.Payload.(type) {
	case json.RawMessage:
		b = v
	case []byte:
		b = v
	default:
		var err error
		if b, err = json.Marshal(v); err != nil {
			return nil, programming("event %s: marshal payload: %v", ev.Subject, err)
		}
	}
	switch {
	case len(b) > PayloadLimitBytes:
		return nil, programming("event %s: payload of %d bytes is above the 1 MiB hard limit; use a claim check (P12.2)",
			ev.Subject, len(b))
	case len(b) > PayloadWarnBytes:
		logger(p.Logger).Warn("event payload above 64 KiB; use a claim check (P12.2)",
			"subject", ev.Subject, "bytes", len(b))
	}
	return b, nil
}

func (p *Producer) occurredAt(ev Outgoing) time.Time {
	t := ev.OccurredAt
	if t.IsZero() {
		if p.Now != nil {
			t = p.Now()
		} else {
			t = time.Now()
		}
	}
	return t.UTC()
}

// traceParent is the W3C traceparent of the current span, "" when there is none (P18.1).
func (p *Producer) traceParent(ctx context.Context) string {
	if p.Propagator == nil {
		return ""
	}
	c := propagation.MapCarrier{}
	p.Propagator.Inject(ctx, c)
	return c.Get(envelope.HeaderTraceParent)
}

func (r outboxRow) envelopeRow() envelope.Row {
	return envelope.Row{ID: r.id, Subject: r.subject, AggregateType: r.aggregateType, AggregateID: r.aggregateID,
		AggregateVersion: r.aggregateVersion, OccurredAt: r.occurredAt, TraceParent: r.traceParent,
		CausationID: r.causationID, HopCount: r.hopCount, Payload: r.payload}
}

// storedHeaders keeps the ce-* attributes the outbox has no column for (ddl/02 headers): the
// data schema as written (the writer's version) and the legal entity.
func storedHeaders(h map[string]string) []byte {
	keep := map[string]string{envelope.HeaderDataSchema: h[envelope.HeaderDataSchema]}
	if le := h[envelope.HeaderLegalEntity]; le != "" {
		keep[envelope.HeaderLegalEntity] = le
	}
	b, _ := json.Marshal(keep) // a map of strings always marshals
	return b
}

// programming is a programming error of the component: INTERNAL with a clear cause (P4.3).
func programming(format string, args ...any) error {
	return problem.Wrap(fmt.Errorf("events: "+format, args...), "INTERNAL", nil)
}

// logger returns l, or a logger that discards when l is nil.
func logger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.New(slog.DiscardHandler)
	}
	return l
}
