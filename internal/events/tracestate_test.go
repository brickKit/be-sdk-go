package events

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// P12 envelope (rc.2): the producing span's tracestate is stored in the outbox column tracestate and
// travels next to traceparent.
func TestPrepareKeepsTheTraceState(t *testing.T) {
	ts, err := trace.ParseTraceState("vendor1=opaque1,vendor2=opaque2")
	require.NoError(t, err)
	sc := trace.SpanContextFromContext(spanCtx(t)).WithTraceState(ts)
	r, err := widgetProducer(t, nil).prepare(trace.ContextWithSpanContext(context.Background(), sc), reverted(1))
	require.NoError(t, err)
	require.Equal(t, "vendor1=opaque1,vendor2=opaque2", r.traceState)
}

func TestPumpPublishesTheTraceState(t *testing.T) {
	r := pendingRow(t, 1)
	r.traceState = "vendor1=opaque1"
	pub := &fakePublisher{}
	p, _, _ := newTestPump(pub)
	_, err := p.newRunner(newFakeOutbox(r)).round(context.Background())
	require.NoError(t, err)
	require.Equal(t, "vendor1=opaque1", pub.messages()[0].Header["tracestate"])
}
