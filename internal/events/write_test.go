package events

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var fixedNow = time.Date(2026, 10, 3, 8, 30, 0, 123456000, time.UTC)

func widgetProducer(t *testing.T, log *slog.Logger) *Producer {
	return &Producer{ComponentID: "conformance/widget", Version: "2.1.0", Contract: widgetContract(t),
		Publishes:  []string{"conformance.widget.reverted.v1", "conformance.widget.created.v1"},
		Propagator: propagation.TraceContext{}, Logger: log, Now: func() time.Time { return fixedNow }}
}

func reverted(version int64) Outgoing {
	return Outgoing{Subject: "conformance.widget.reverted.v1", AggregateID: "w-1", Version: version,
		Payload: map[string]any{"widget_id": "w-1", "legal_entity_id": "LE01", "number": "N1",
			"status": "DRAFT", "reason": "RESERVATION_EXPIRED", "version": version}}
}

func spanCtx(t *testing.T) context.Context {
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

func TestPrepareBuildsTheRow(t *testing.T) {
	ev := reverted(3)
	ev.Origin = envelope.Origin{Kind: envelope.OriginEvent, CausationID: "01920000-0000-7000-8000-000000000001", HopCount: 2}
	r, err := widgetProducer(t, nil).prepare(spanCtx(t), ev)
	require.NoError(t, err)
	id, err := envelope.ParseID(r.id)
	require.NoError(t, err)
	require.Equal(t, envelope.IDTime(id), r.createdAt)
	require.Equal(t, "conformance.widget.widget", r.aggregateType)
	require.Equal(t, fixedNow, r.occurredAt)
	require.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", r.traceParent)
	require.Equal(t, "01920000-0000-7000-8000-000000000001", r.causationID)
	require.Equal(t, 3, r.hopCount)
	var h map[string]string
	require.NoError(t, json.Unmarshal(r.headers, &h))
	require.Equal(t, map[string]string{"ce-legalentity": "LE01",
		"ce-dataschema": "conformance/widget@2.1.0/contracts/events/widget.events.json#conformance.widget.reverted.v1"}, h)
	require.JSONEq(t, `{"widget_id":"w-1","legal_entity_id":"LE01","number":"N1","status":"DRAFT","reason":"RESERVATION_EXPIRED","version":3}`, string(r.payload))
}

func TestPrepareFromARequestHasNoCausation(t *testing.T) {
	ev := reverted(1)
	ev.OccurredAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("x", 3600))
	ev.Payload = json.RawMessage(`{"widget_id":"w-1","legal_entity_id":"LE01","number":"N1","status":"DRAFT","reason":"RESERVATION_EXPIRED","version":1}`)
	r, err := widgetProducer(t, nil).prepare(context.Background(), ev)
	require.NoError(t, err)
	require.Equal(t, "", r.causationID)
	require.Equal(t, 0, r.hopCount)
	require.Equal(t, "", r.traceParent)
	require.Equal(t, time.Date(2025, 12, 31, 23, 0, 0, 0, time.UTC), r.occurredAt)
}

func requireInternal(t *testing.T, err error, contains string) {
	t.Helper()
	require.Error(t, err)
	var pe *problem.Error
	require.ErrorAs(t, err, &pe)
	require.Equal(t, "INTERNAL", pe.Reason)
	require.Contains(t, err.Error(), contains)
}

func TestPrepareRejectsProgrammingErrors(t *testing.T) {
	p := widgetProducer(t, nil)
	notPublished := reverted(1)
	notPublished.Subject = "conformance.widget.approved.v1"
	_, err := p.prepare(context.Background(), notPublished)
	requireInternal(t, err, "not declared")

	p2 := widgetProducer(t, nil)
	p2.Publishes = append(p2.Publishes, "conformance.widget.ghost.v1")
	ghost := reverted(1)
	ghost.Subject = "conformance.widget.ghost.v1"
	_, err = p2.prepare(context.Background(), ghost)
	requireInternal(t, err, "not in the events contract")

	invalid := reverted(1)
	invalid.Payload = map[string]any{"widget_id": "w-1"}
	_, err = p.prepare(context.Background(), invalid)
	requireInternal(t, err, "violates the contract")

	notObject := reverted(1)
	notObject.Payload = []int{1}
	_, err = p.prepare(context.Background(), notObject)
	requireInternal(t, err, "not a JSON object")

	unmarshalable := reverted(1)
	unmarshalable.Payload = func() {}
	_, err = p.prepare(context.Background(), unmarshalable)
	requireInternal(t, err, "marshal")

	zeroVersion := reverted(1)
	zeroVersion.Version = 0 // the payload is valid; the envelope rule (versions start at 1) is not
	_, err = p.prepare(context.Background(), zeroVersion)
	requireInternal(t, err, "ENVELOPE_INVALID")

	noAggregate := reverted(1)
	noAggregate.AggregateID = ""
	_, err = p.prepare(context.Background(), noAggregate)
	requireInternal(t, err, "ENVELOPE_INVALID")
}

func TestPrepareEnforcesTheLegalEntity(t *testing.T) {
	// A contract that marks the event a transaction document but whose payload schema does not
	// require legal_entity_id cannot exist (the contract schema forbids it), so check the envelope
	// rule with a payload whose legal_entity_id is empty: the schema accepts it, P11.8 does not.
	ev := reverted(1)
	ev.Payload = map[string]any{"widget_id": "w-1", "legal_entity_id": "", "number": "N1",
		"status": "DRAFT", "reason": "RESERVATION_EXPIRED", "version": 1}
	_, err := widgetProducer(t, nil).prepare(context.Background(), ev)
	requireInternal(t, err, envelope.ReasonLegalEntityMissing)
}

// P12.2 (stage-B ruling): a payload is at most 64 KiB (65,536 bytes of serialised JSON); a larger
// one is refused at publish, naming the subject and the size (rc.2 vectors
// envelope.headers.payload-at-limit / payload-too-large).
func TestPayloadSizeLimits(t *testing.T) {
	big := func(n int) Outgoing {
		ev := reverted(1)
		ev.Payload = map[string]any{"widget_id": "w-1", "legal_entity_id": "LE01", "number": strings.Repeat("x", n),
			"status": "DRAFT", "reason": "RESERVATION_EXPIRED", "version": 1}
		return ev
	}
	sizeOf := func(ev Outgoing) int { b, _ := json.Marshal(ev.Payload); return len(b) }
	fill := PayloadLimitBytes - sizeOf(big(0)) // the filler that makes the payload exactly 64 KiB
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	atLimit := big(fill)
	require.Equal(t, 65536, sizeOf(atLimit))
	_, err := widgetProducer(t, log).prepare(context.Background(), atLimit)
	require.NoError(t, err)
	require.Empty(t, buf.String(), "no warning: the limit is a hard one")

	_, err = widgetProducer(t, log).prepare(context.Background(), big(fill+1))
	requireInternal(t, err, "PAYLOAD_TOO_LARGE")
	require.Contains(t, err.Error(), "65537 bytes")
	require.Contains(t, err.Error(), "conformance.widget.reverted.v1")
}
