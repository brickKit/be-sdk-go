package telemetry

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Resource is a member's OpenTelemetry resource (P18.1).
type Resource struct {
	ComponentID      string // service.name (required)
	ComponentVersion string // service.version
	Namespace        string // service.namespace: the component's domain (first segment of its ID)
	InstanceID       string // service.instance.id: the container or pod
	Environment      string // deployment.environment.name: DEPLOY_ENV (OTel semantic conventions ≥ 1.27)
}

// resource returns the resource attributes, leaving out the empty ones.
func (r Resource) resource() *resource.Resource {
	attrs := serviceAttrs(r.ComponentID, r.ComponentVersion)
	for _, kv := range [...][2]string{
		{"service.namespace", r.Namespace}, {"service.instance.id", r.InstanceID}, {"deployment.environment.name", r.Environment},
	} {
		if kv[1] != "" {
			attrs = append(attrs, attribute.String(kv[0], kv[1]))
		}
	}
	return resource.NewSchemaless(attrs...)
}

// Member is one component's telemetry (P19.4): its own tracer provider over the platform's shared
// exporter, its own meter provider and its own Prometheus registry whose every series carries
// component=<ComponentID> (P18.3).
type Member struct {
	id         string
	platform   *Platform
	tp         *sdktrace.TracerProvider
	mp         *sdkmetric.MeterProvider
	registry   *prometheus.Registry
	registerer prometheus.Registerer
}

// NewMember creates a member's providers and registry on the platform (P18.1, P18.3, P19.4).
func NewMember(p *Platform, r Resource) (*Member, error) {
	if p == nil {
		return nil, errors.New("telemetry: NewMember: nil platform")
	}
	if r.ComponentID == "" {
		return nil, errors.New("telemetry: NewMember: empty ComponentID")
	}
	reg := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWith(prometheus.Labels{"component": r.ComponentID}, reg)
	res := r.resource()
	exp, err := otelprom.New(otelprom.WithRegisterer(registerer))
	if err != nil {
		return nil, fmt.Errorf("telemetry: %s: prometheus exporter: %w", r.ComponentID, err)
	}
	return &Member{
		id:         r.ComponentID,
		platform:   p,
		tp:         p.newTracerProvider(res),
		mp:         sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(exp)),
		registry:   reg,
		registerer: registerer,
	}, nil
}

// TracerProvider returns the member's own tracer provider; every instrumentation is given it
// explicitly, never the global one (P18.1).
func (m *Member) TracerProvider() trace.TracerProvider { return m.tp }

// Tracer returns the member's tracer, named after the component.
func (m *Member) Tracer() trace.Tracer { return m.tp.Tracer(m.id) }

// MeterProvider returns the member's own meter provider (P18.1, P19.4).
func (m *Member) MeterProvider() metric.MeterProvider { return m.mp }

// Meter returns the member's meter, named after the component; its instruments appear in the member's
// registry with the component label.
func (m *Member) Meter() metric.Meter { return m.mp.Meter(m.id) }

// Propagator returns the platform's shared propagator (P19.3).
func (m *Member) Propagator() propagation.TextMapPropagator { return m.platform.propagator }

// Gatherer returns the member's own registry, served on its /metrics and aggregated on a shell's
// (P18.3, P19.7).
func (m *Member) Gatherer() prometheus.Gatherer { return m.registry }

// Registerer returns the member's registry wrapped with the constant label component=<ComponentID>, so
// every series registered through it carries that label (P18.3).
func (m *Member) Registerer() prometheus.Registerer { return m.registerer }

// Shutdown flushes and stops only this member's providers; the shared exporter stays open for the
// other members until the platform shuts down (P19.3).
func (m *Member) Shutdown(ctx context.Context) error {
	return errors.Join(m.tp.Shutdown(ctx), m.mp.Shutdown(ctx))
}
