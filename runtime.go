package besdk

import (
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Runtime is what a component sees of the SDK: methods only, nothing to reach around it
// (sdk-redesign-apis §2.2). One Runtime per component; in a shell one per member.
type Runtime struct {
	id, version string
	cfg         *Config
	log         *slog.Logger
	tel         *telemetry.Member
	catalogue   *problem.Catalogue
	locale      string
	clock       func() time.Time
	deps        runtimeDeps // wired by the serving process: store, bus, connections …
}

// ID is the component's ID (the member's in a shell).
func (rt *Runtime) ID() string { return rt.id }

// Version is the component's version.
func (rt *Runtime) Version() string { return rt.version }

// Config is the component's configuration (P2).
func (rt *Runtime) Config() *Config { return rt.cfg }

// Logger is the component's JSON logger: component_id and version on every line, trace and request
// fields from the context, personal data redacted (P18.2). Log with a context: Logger().InfoContext.
func (rt *Runtime) Logger() *slog.Logger { return rt.log }

// Tracer is the component's own tracer (P18.1, P19.4).
func (rt *Runtime) Tracer() trace.Tracer { return rt.tel.Tracer() }

// Meter is the component's own meter, exported through its Prometheus registry.
func (rt *Runtime) Meter() metric.Meter { return rt.tel.Meter() }

// Registry is where the component registers its own metrics (names start with its domain and name);
// every series carries the component label (P18.3).
func (rt *Runtime) Registry() prometheus.Registerer { return rt.tel.Registerer() }

// Now is the current time as the component must read it (tests may replace the clock).
func (rt *Runtime) Now() time.Time { return rt.clock() }

// runtimeDeps holds what the serving process wires into a Runtime after the configuration loaded.
type runtimeDeps struct {
	out *outbound
}
