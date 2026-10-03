package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func emit(tp trace.TracerProvider, name string) trace.SpanContext {
	_, s := tp.Tracer("test").Start(context.Background(), name)
	s.End()
	return s.SpanContext()
}

func mustPlatform(t *testing.T, base string) *Platform {
	t.Helper()
	p, err := NewPlatform(context.Background(), PlatformOptions{OTELBaseURL: base, ServiceName: "be/go-core", ServiceVersion: "1.0.0"})
	if err != nil {
		t.Fatalf("NewPlatform: %v", err)
	}
	return p
}

func mustMember(t *testing.T, p *Platform, id string) *Member {
	t.Helper()
	m, err := NewMember(p, Resource{ComponentID: id, ComponentVersion: "2.0.0", Namespace: "be-assembly-standard",
		InstanceID: "pod-1", Environment: "dev"})
	if err != nil {
		t.Fatalf("NewMember: %v", err)
	}
	return m
}

// P18.1: no OTEL_BASE_URL means no export and no error, and still a valid, sampled trace id.
func TestNoBaseURLStillYieldsValidTraceIDs(t *testing.T) {
	ctx := context.Background()
	p := mustPlatform(t, "")
	m := mustMember(t, p, "erp/sales")
	_, s := m.Tracer().Start(ctx, "req")
	s.End()
	if !s.SpanContext().IsValid() || !s.SpanContext().IsSampled() {
		t.Fatalf("member span %v", s.SpanContext())
	}
	if sc := emit(p.Globals().TracerProvider, "fallback"); !sc.IsValid() {
		t.Fatal("fallback span invalid")
	}
	if err := m.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// P19.3 / r1-01 S3: stopping member A must not close the shared exporter; B's later spans still arrive,
// each under its own member's resource.
func TestStoppingOneMemberKeepsTheSharedExporter(t *testing.T) {
	ctx := context.Background()
	c := newCollector(t)
	p := mustPlatform(t, c.srv.URL+"/")
	a, b := mustMember(t, p, "erp/sales"), mustMember(t, p, "erp/inventory")
	emit(a.TracerProvider(), "a1")
	emit(b.TracerProvider(), "b1")
	if err := a.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	emit(b.TracerProvider(), "b2")
	if err := b.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	got := c.byService()
	if got["erp/sales"] != 1 || got["erp/inventory"] != 2 || len(got) != 2 {
		t.Fatalf("received %v, want erp/sales:1 erp/inventory:2", got)
	}
	s, _ := c.find("b2")
	want := map[string]string{"service.name": "erp/inventory", "service.version": "2.0.0",
		"service.namespace": "be-assembly-standard", "service.instance.id": "pod-1", "deployment.environment.name": "dev"}
	for k, v := range want {
		if s.Resource[k] != v {
			t.Errorf("resource %s = %q, want %q", k, s.Resource[k], v)
		}
	}
	for _, path := range c.requestPaths() {
		if path != "/v1/traces" {
			t.Errorf("exported to %q", path)
		}
	}
}

// P18.1: the fallback global provider carries the platform's own service.name, so a missed
// instrumentation is visible.
func TestGlobalsFallbackCarriesPlatformName(t *testing.T) {
	c := newCollector(t)
	p := mustPlatform(t, c.srv.URL)
	g := p.Globals()
	emit(g.TracerProvider, "missed")
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	s, ok := c.find("missed")
	if !ok || s.Resource["service.name"] != "be/go-core" || s.Resource["service.version"] != "1.0.0" {
		t.Fatalf("got %v %v", s, ok)
	}
	if g.Propagator == nil {
		t.Fatal("no propagator")
	}
}

// P18.1: W3C TraceContext and Baggage, shared by the platform and every member.
func TestPropagatorCarriesTraceparentAndBaggage(t *testing.T) {
	p := mustPlatform(t, "")
	m := mustMember(t, p, "erp/sales")
	ctx, s := m.Tracer().Start(context.Background(), "out")
	defer s.End()
	mem, _ := baggage.NewMemberRaw("tenant", "t-42")
	bg, _ := baggage.New(mem)
	ctx = baggage.ContextWithBaggage(ctx, bg)
	carrier := propagation.MapCarrier{}
	m.Propagator().Inject(ctx, carrier)
	if carrier.Get("traceparent") == "" || carrier.Get("baggage") != "tenant=t-42" {
		t.Fatalf("carrier %v", carrier)
	}
	back := p.Propagator().Extract(context.Background(), carrier)
	if trace.SpanContextFromContext(back).TraceID() != s.SpanContext().TraceID() ||
		baggage.FromContext(back).Member("tenant").Value() != "t-42" {
		t.Fatal("round trip lost trace or baggage")
	}
}

func TestNewPlatformRejectsBadBaseURL(t *testing.T) {
	for _, u := range []string{"://x", "ftp://collector:4318", "collector:4318", "http://"} {
		if _, err := NewPlatform(context.Background(), PlatformOptions{OTELBaseURL: u, ServiceName: "s"}); err == nil {
			t.Errorf("%q: want an error", u)
		}
	}
}

func TestNewMemberValidates(t *testing.T) {
	p := mustPlatform(t, "")
	if _, err := NewMember(p, Resource{}); err == nil {
		t.Fatal("empty ComponentID accepted")
	}
	if _, err := NewMember(nil, Resource{ComponentID: "a/b"}); err == nil {
		t.Fatal("nil platform accepted")
	}
}
