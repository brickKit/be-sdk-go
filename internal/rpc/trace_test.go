package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	"github.com/brickKit/be-sdk-go/internal/rpc/testdata/probepb"
)

// One trace across client and server, each span on its own member's provider, with the propagator
// passed explicitly and no global touched (P7.2, P18.1, r1-01).
func TestOneTraceAcrossClientAndServerOnTheirOwnProviders(t *testing.T) {
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	clientRec, serverRec := tracetest.NewSpanRecorder(), tracetest.NewSpanRecorder()
	clientTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(clientRec))
	serverTP := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(serverRec))
	t.Cleanup(func() { _ = clientTP.Shutdown(context.Background()); _ = serverTP.Shutdown(context.Background()) })

	ts := startServer(t, func(c *ServerConfig) { c.TracerProvider, c.Propagator = serverTP, prop })
	type seen struct {
		traceID     trace.TraceID
		traceparent []string
	}
	got := make(chan seen, 1)
	ts.probe.set(func(ctx context.Context, _, _ string) (string, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		got <- seen{trace.SpanContextFromContext(ctx).TraceID(), md.Get("traceparent")}
		return "ok", nil
	})
	c := newTestConns(t, func(cfg *ClientConfig) { cfg.TracerProvider, cfg.Propagator = clientTP, prop })

	ctx, parent := clientTP.Tracer("test").Start(context.Background(), "parent")
	_, err := probeClient(t, c, "conformance/widget", ts.addr).Read(ctx, &probepb.ProbeRequest{})
	parent.End()
	require.NoError(t, err)

	want := parent.SpanContext().TraceID()
	s := <-got
	require.Equal(t, want, s.traceID, "the server continues the client's trace")
	require.Len(t, s.traceparent, 1)

	require.Eventually(t, func() bool { return len(serverRec.Ended()) > 0 }, timeoutShort, tick)
	kinds := func(rec *tracetest.SpanRecorder) map[trace.SpanKind]trace.TraceID {
		out := map[trace.SpanKind]trace.TraceID{}
		for _, sp := range rec.Ended() {
			out[sp.SpanKind()] = sp.SpanContext().TraceID()
		}
		return out
	}
	client, server := kinds(clientRec), kinds(serverRec)
	require.Equal(t, want, client[trace.SpanKindClient])
	require.NotContains(t, client, trace.SpanKindServer)
	require.Equal(t, want, server[trace.SpanKindServer])
	require.NotContains(t, server, trace.SpanKindClient)
}
