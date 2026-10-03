package telemetry

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/propagation"
)

// P18.1 (stage-B ruling): an inbound traceparent whose sampled flag is 0 is propagated — same trace
// ID, flag 0 outbound — and its spans are neither recorded nor exported. A request without one still
// gets a sampled root span.
func TestUnsampledTraceparentIsPropagatedNotRecorded(t *testing.T) {
	p := mustPlatform(t, "")
	m := mustMember(t, p, "erp/sales")
	const in = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
	ctx := p.Propagator().Extract(context.Background(), propagation.MapCarrier{"traceparent": in})
	ctx, s := m.Tracer().Start(ctx, "req")
	defer s.End()
	if s.IsRecording() || s.SpanContext().IsSampled() {
		t.Fatalf("an unsampled parent's span is recorded: %v", s.SpanContext())
	}
	if got := s.SpanContext().TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace id %s", got)
	}
	out := propagation.MapCarrier{}
	p.Propagator().Inject(ctx, out)
	if tp := out["traceparent"]; !strings.HasPrefix(tp, "00-4bf92f3577b34da6a3ce929d0e0e4736-") || !strings.HasSuffix(tp, "-00") {
		t.Fatalf("outbound traceparent %q", tp)
	}
	_, root := m.Tracer().Start(context.Background(), "root")
	defer root.End()
	if !root.IsRecording() || !root.SpanContext().IsSampled() {
		t.Fatal("a root span is sampled")
	}
}
