package besdk

import (
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/internal/telemetry"
)

// Adapters from the internal packages' metric hooks to the protocol metric groups (P18.3).

func httpObserver(m *telemetry.HTTPServerMetrics) func(method, route string, status int, d time.Duration) {
	return func(method, route string, status int, d time.Duration) {
		m.Requests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
		m.Duration.WithLabelValues(method, route).Observe(d.Seconds())
	}
}

type grpcServerMetrics struct{ m *telemetry.GRPCServerMetrics }

func (g grpcServerMetrics) Handled(service, method, code string, elapsed time.Duration) {
	g.m.Handled.WithLabelValues(service, method, code).Inc()
	g.m.Duration.WithLabelValues(service, method).Observe(elapsed.Seconds())
}

type grpcClientMetrics struct{ m *telemetry.GRPCClientMetrics }

func (g grpcClientMetrics) Handled(target, method, code string, elapsed time.Duration) {
	g.m.Handled.WithLabelValues(target, method, code).Inc()
	g.m.Duration.WithLabelValues(target, method).Observe(elapsed.Seconds())
}

func (g grpcClientMetrics) Inflight(target string, delta int) {
	g.m.Inflight.WithLabelValues(target).Add(float64(delta))
}
