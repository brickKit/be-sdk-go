// Package obs builds each member's own logger, metrics registry and tracer provider (protocol 17's shell invariant 2:
// everything per member). Nothing here touches a process-wide global: no slog.SetDefault, no prometheus.DefaultRegisterer,
// no otel.SetTracerProvider.
package obs

import (
	"context"
	"io"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Member names whose logs, metrics and traces these are.
type Member struct {
	ID      string // component ID, e.g. erp/sales
	Version string
}

// ParseLevel maps the LOG_LEVEL values to slog levels (unknown values are rejected by config before this).
func ParseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}

// NewLogger returns the member's JSON logger: every line carries the component ID and version.
func NewLogger(m Member, level slog.Level, w io.Writer) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: utcTime})
	return slog.New(h).With("component", m.ID, "version", m.Version)
}

// TimeFormat is the log lines' time: RFC 3339 in UTC to the millisecond (P10.2), whatever the host's time zone.
const TimeFormat = "2006-01-02T15:04:05.000Z"

func utcTime(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 && a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
		a.Value = slog.StringValue(a.Value.Time().UTC().Format(TimeFormat))
	}
	return a
}

// DurationBuckets are the upper bounds of every duration histogram, the same in every language (P10.6).
var DurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// NewRegistry returns the member's own metrics registry; the launcher or shell serves it with a component label.
func NewRegistry() *prometheus.Registry { return prometheus.NewRegistry() }

// NewTracerProvider returns the member's tracer provider, service name = component ID. With an empty endpoint nothing
// is exported (basic observability tier); spans still carry trace IDs for the logs.
func NewTracerProvider(ctx context.Context, m Member, otlpEndpoint string) (*sdktrace.TracerProvider, error) {
	res := resource.NewSchemaless(semconv.ServiceName(m.ID), semconv.ServiceVersion(m.Version))
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if otlpEndpoint != "" {
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(otlpEndpoint))
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	return sdktrace.NewTracerProvider(opts...), nil
}

// Propagator is the W3C trace-context propagator (traceparent, tracestate). It is a value, not the global one.
var Propagator = propagation.TraceContext{}
