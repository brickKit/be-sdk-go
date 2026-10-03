package besdk

import (
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/authz/acl"
	"github.com/brickKit/be-sdk-go/internal/events"
	"github.com/brickKit/be-sdk-go/internal/jobs"
	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Runtime is what a component sees of the SDK: methods only, nothing to reach around it
// (sdk-redesign-apis §2.2). One Runtime per component; in a shell one per member.
type Runtime struct {
	id, version  string
	cfg          *Config
	log          *slog.Logger
	tel          *telemetry.Member
	catalogue    *problem.Catalogue
	locale       string
	clock        func() time.Time
	deps         runtimeDeps    // wired by the serving process: store, bus, connections …
	authzCatalog *authz.Catalog // Spec.Catalog's resource types; nil when it declares none
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
	out        *outbound
	store      *pg.Store
	producer   *events.Producer
	jobs       *jobs.Engine
	lifecycle  *lifecycle.Engine
	projection *acl.Projection                // the ACL projection (P6.12); nil without resource types
	authzGRPC  string                         // AUTHZ_GRPC_URL's dial target; "" when not declared
	loaders    map[ResourceType]SharingLoader // Module.Sharing by type: PermKey.On and the resource contract
}
