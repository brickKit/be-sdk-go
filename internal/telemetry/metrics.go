package telemetry

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// The protocol metric groups of P18.3, with the exact `be_` names and labels of the spec/18 table. Each
// constructor registers its collectors on a Registerer (a member's, which adds component=<ID>) all or
// nothing. client_golang is used directly so the names are exact.

// durationBuckets are the seconds buckets of request and call durations: 1 ms to 30 s.
func durationBuckets() []float64 {
	return []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}
}

// register registers every collector or none: on the first error the ones already registered are
// unregistered again.
func register(reg prometheus.Registerer, cs ...prometheus.Collector) error {
	for i, c := range cs {
		if err := reg.Register(c); err != nil {
			for _, done := range cs[:i] {
				reg.Unregister(done)
			}
			return fmt.Errorf("telemetry: register metrics: %w", err)
		}
	}
	return nil
}

func counterVec(name, help string, labels ...string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
}

func gaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
}

func histogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
}

// HTTPServerMetrics are the inbound HTTP metrics (P18.3); route is the route template, status_code numeric.
type HTTPServerMetrics struct {
	Requests *prometheus.CounterVec   // be_http_server_requests_total{method,route,status_code}
	Duration *prometheus.HistogramVec // be_http_server_duration_seconds{method,route}
}

// NewHTTPServerMetrics registers the inbound HTTP metrics (P18.3).
func NewHTTPServerMetrics(reg prometheus.Registerer) (*HTTPServerMetrics, error) {
	m := &HTTPServerMetrics{
		Requests: counterVec("be_http_server_requests_total", "HTTP requests served.", "method", "route", "status_code"),
		Duration: histogramVec("be_http_server_duration_seconds", "HTTP request duration in seconds.", durationBuckets(), "method", "route"),
	}
	return m, register(reg, m.Requests, m.Duration)
}

// HTTPClientMetrics are the outbound HTTP metrics (P18.3); target is the called component.
type HTTPClientMetrics struct {
	Requests *prometheus.CounterVec   // be_http_client_requests_total{target,method,status_code}
	Duration *prometheus.HistogramVec // be_http_client_duration_seconds{target,method}
}

// NewHTTPClientMetrics registers the outbound HTTP metrics (P18.3).
func NewHTTPClientMetrics(reg prometheus.Registerer) (*HTTPClientMetrics, error) {
	m := &HTTPClientMetrics{
		Requests: counterVec("be_http_client_requests_total", "Outbound HTTP requests.", "target", "method", "status_code"),
		Duration: histogramVec("be_http_client_duration_seconds", "Outbound HTTP request duration in seconds.", durationBuckets(), "target", "method"),
	}
	return m, register(reg, m.Requests, m.Duration)
}

// GRPCServerMetrics are the inbound gRPC metrics (P18.3); code is the canonical code name.
type GRPCServerMetrics struct {
	Handled  *prometheus.CounterVec   // be_grpc_server_handled_total{service,method,code}
	Duration *prometheus.HistogramVec // be_grpc_server_duration_seconds{service,method}
}

// NewGRPCServerMetrics registers the inbound gRPC metrics (P18.3).
func NewGRPCServerMetrics(reg prometheus.Registerer) (*GRPCServerMetrics, error) {
	m := &GRPCServerMetrics{
		Handled:  counterVec("be_grpc_server_handled_total", "gRPC calls handled.", "service", "method", "code"),
		Duration: histogramVec("be_grpc_server_duration_seconds", "gRPC call duration in seconds.", durationBuckets(), "service", "method"),
	}
	return m, register(reg, m.Handled, m.Duration)
}

// GRPCClientMetrics are the outbound gRPC metrics and the outbound bulkhead gauge (P18.3).
type GRPCClientMetrics struct {
	Handled  *prometheus.CounterVec   // be_grpc_client_handled_total{target,method,code}
	Duration *prometheus.HistogramVec // be_grpc_client_duration_seconds{target,method}
	Inflight *prometheus.GaugeVec     // be_outbound_inflight{target}
}

// NewGRPCClientMetrics registers the outbound gRPC metrics and be_outbound_inflight (P18.3).
func NewGRPCClientMetrics(reg prometheus.Registerer) (*GRPCClientMetrics, error) {
	m := &GRPCClientMetrics{
		Handled:  counterVec("be_grpc_client_handled_total", "Outbound gRPC calls completed.", "target", "method", "code"),
		Duration: histogramVec("be_grpc_client_duration_seconds", "Outbound gRPC call duration in seconds.", durationBuckets(), "target", "method"),
		Inflight: gaugeVec("be_outbound_inflight", "Outbound calls in flight per target (bulkhead occupancy).", "target"),
	}
	return m, register(reg, m.Handled, m.Duration, m.Inflight)
}
