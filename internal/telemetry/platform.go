// Package telemetry holds the OpenTelemetry and Prometheus plumbing of P18.1, P18.3, P19.3 and P19.4:
// one Platform per process (the shared OTLP/HTTP span exporter and the propagator) and one Member per
// component (its own tracer, meter provider and Prometheus registry). The package never installs process
// globals; the platform owner does, from Platform.Globals.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// tracesPath is the OTLP/HTTP traces path appended to OTEL_BASE_URL (P18.1).
const tracesPath = "/v1/traces"

// PlatformOptions configures the process-wide part of telemetry (P19.3).
type PlatformOptions struct {
	OTELBaseURL    string // OTEL_BASE_URL; empty means no export and no error (P18.1)
	ServiceName    string // the process's own ID: the component standalone, the shell in a shell
	ServiceVersion string // the process's own version
}

// Platform is the process-wide telemetry of P19.3: the shared span exporter and the propagator, plus
// the fallback tracer provider the owner installs as the global one (P18.1).
type Platform struct {
	exporter   sdktrace.SpanExporter // nil when nothing is exported
	propagator propagation.TextMapPropagator
	fallback   *sdktrace.TracerProvider
}

// Globals is what the platform owner installs as the process globals (otel.SetTextMapPropagator,
// otel.SetTracerProvider); the package never does it itself. The tracer provider is a fallback only: its
// service.name is the platform's own, so a span under that name reveals a missed instrumentation (P18.1).
type Globals struct {
	Propagator     propagation.TextMapPropagator
	TracerProvider trace.TracerProvider
}

// NewPlatform creates the shared OTLP/HTTP exporter to <OTELBaseURL>/v1/traces when the base URL is set,
// and the W3C TraceContext + Baggage propagator (P18.1, P19.3).
func NewPlatform(ctx context.Context, o PlatformOptions) (*Platform, error) {
	p := &Platform{propagator: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})}
	if o.OTELBaseURL != "" {
		endpoint, err := tracesURL(o.OTELBaseURL)
		if err != nil {
			return nil, err
		}
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
		if err != nil {
			return nil, fmt.Errorf("telemetry: OTLP exporter: %w", err)
		}
		p.exporter = exp
	}
	p.fallback = p.newTracerProvider(resource.NewSchemaless(serviceAttrs(o.ServiceName, o.ServiceVersion)...))
	return p, nil
}

// tracesURL validates OTEL_BASE_URL (an http or https URL with a host) and appends the traces path.
func tracesURL(base string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("telemetry: OTEL_BASE_URL %q: %w", base, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("telemetry: OTEL_BASE_URL %q: want http(s)://host[:port][/path]", base)
	}
	return strings.TrimRight(base, "/") + tracesPath, nil
}

// serviceAttrs returns service.name and, when set, service.version.
func serviceAttrs(name, version string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("service.name", name)}
	if version != "" {
		attrs = append(attrs, attribute.String("service.version", version))
	}
	return attrs
}

// newTracerProvider returns a provider with its own batch span processor over the shared exporter (whose
// Shutdown is a no-op, r1-01 S3). Every root span is sampled, so trace ids exist even without export;
// a span under an inbound unsampled traceparent follows its parent — kept trace ID, not recorded, flag
// 0 propagated outbound (P18.1, stage-B ruling).
func (p *Platform) newTracerProvider(res *resource.Resource) *sdktrace.TracerProvider {
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample()))}
	if p.exporter != nil {
		opts = append(opts, sdktrace.WithBatcher(sharedExporter{p.exporter}))
	}
	return sdktrace.NewTracerProvider(opts...)
}

// Propagator returns the shared W3C TraceContext + Baggage propagator (P18.1).
func (p *Platform) Propagator() propagation.TextMapPropagator { return p.propagator }

// Globals returns the propagator and the fallback tracer provider for the owner to install (P18.1).
func (p *Platform) Globals() Globals {
	return Globals{Propagator: p.propagator, TracerProvider: p.fallback}
}

// Shutdown flushes the fallback provider and then flushes and closes the shared exporter. Only the
// platform owner calls it, after every member has stopped (P19.3).
func (p *Platform) Shutdown(ctx context.Context) error {
	err := p.fallback.Shutdown(ctx)
	if p.exporter != nil {
		err = errors.Join(err, p.exporter.Shutdown(ctx))
	}
	return err
}

// sharedExporter wraps the platform's exporter for one tracer provider: shutting the provider down
// flushes its own queue but must not close the exporter other members still use (r1-01 S1/S3).
type sharedExporter struct{ sdktrace.SpanExporter }

// Shutdown is a no-op: the platform closes the real exporter (P19.3).
func (sharedExporter) Shutdown(context.Context) error { return nil }
