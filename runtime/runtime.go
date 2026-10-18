// Package runtime is the only door a module has to the outside: its member identity, configuration, dependency
// addresses, logger, metrics and tracer (component implementation conventions: "the runtime is the only entry").
// The launcher (single run) or the shell builds one Runtime per member.
package runtime

import (
	"log/slog"
	"net/url"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/audit"
	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// Runtime is what a module receives. Fields are set by the launcher; modules only read them.
type Runtime struct {
	Member  obs.Member
	Config  *config.Values
	Logger  *slog.Logger
	Metrics prometheus.Registerer
	Tracer  trace.Tracer
	// DB is the member's database; nil when the component declares no migrations.
	DB *db.DB
	// Events writes the member's events into its outbox inside a transaction; nil without declared streams.
	Events *events.Producer
	// Work enqueues the module's queue jobs inside a transaction; nil without a database.
	Work *work.Runner
	// Audit writes the member's audit records (with the business change, before a sensitive read, on a denial);
	// nil without a database.
	Audit *audit.Auditor

	addresses config.Source // where the platform's *_ENDPOINT values are looked up
	calls     *calls.Member
}

// Parts are what the launcher or the shell gives a module.
type Parts struct {
	Member    obs.Member
	Config    *config.Values
	Logger    *slog.Logger
	Metrics   prometheus.Registerer
	Tracer    trace.Tracer
	Addresses config.Source
	DB        *db.DB
	Outbound  *calls.Member
	Events    *events.Producer
	Work      *work.Runner
	Audit     *audit.Auditor
}

// New is used by the launcher and the shell.
func New(p Parts) *Runtime {
	return &Runtime{Member: p.Member, Config: p.Config, Logger: p.Logger, Metrics: p.Metrics, Tracer: p.Tracer, DB: p.DB,
		Events: p.Events, Work: p.Work, Audit: p.Audit, addresses: p.Addresses, calls: p.Outbound}
}

// HTTP returns the client for a dependency's user surface (protocol chapter 04: deadline, retries, breaker and limit
// per dependency). ok=false: the dependency is absent.
func (r *Runtime) HTTP(componentID string) (*calls.HTTP, bool) {
	ep, ok := r.Dependency(componentID)
	if !ok {
		return nil, false
	}
	return r.HTTPAt(componentID, ep.URL)
}

// GRPC returns the connection to a dependency's system surface (its "grpc" port). Mark idempotent methods with
// calls.Idempotent() and commands with calls.IdempotencyKey(key). ok=false: the dependency is absent.
func (r *Runtime) GRPC(componentID string) (*grpc.ClientConn, bool) {
	ep, ok := r.DependencyPort(componentID, "grpc")
	if !ok {
		return nil, false
	}
	return r.GRPCAt(componentID, ep.Host)
}

// HTTPAt returns the client for a component's user surface at an address the configuration gives (a $endpoint:
// reference, protocol P4.26): the same call policy as a dependency's, counted per component ID. ok=false: no address.
func (r *Runtime) HTTPAt(componentID, address string) (*calls.HTTP, bool) {
	if address == "" || r.calls == nil {
		return nil, false
	}
	return r.calls.HTTP(componentID, address), true
}

// GRPCAt returns the connection to a component's system surface at an address the configuration gives
// (http://host:port as $endpoint:<id>:grpc derives it, or host:port; protocol P4.26), over mTLS like any system call.
// ok=false: no address, or it cannot be dialled.
func (r *Runtime) GRPCAt(componentID, address string) (*grpc.ClientConn, bool) {
	if address == "" || r.calls == nil {
		return nil, false
	}
	if u, err := url.Parse(address); err == nil && u.Host != "" {
		address = u.Host
	}
	conn, err := r.calls.GRPC(componentID, address)
	if err != nil {
		r.Logger.Error("the system surface of a component cannot be dialled", "component", componentID, "error", err.Error())
		return nil, false
	}
	return conn, true
}

// Endpoint is a dependency's address as BrickKit derived it (http://<versioned-service>:<port>).
type Endpoint struct {
	URL  string // as injected, e.g. http://erp-inventory-3-0-0:8086
	Host string // host:port, for dialling gRPC
}

// EnvPrefix turns a component ID into its variable prefix the way BrickKit does: "/" and "-" become "_", upper case.
func EnvPrefix(id string) string {
	return strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(id))
}

// Dependency returns the address of a dependency's main port. ok=false means the dependency is absent (an optional
// one that is not installed): degrade as the boundary card says; it is never an empty string.
func (r *Runtime) Dependency(componentID string) (Endpoint, bool) {
	return r.lookup(EnvPrefix(componentID) + "_ENDPOINT")
}

// DependencyPort returns the address of a named extra port of a dependency (e.g. its "grpc" system port).
func (r *Runtime) DependencyPort(componentID, port string) (Endpoint, bool) {
	return r.lookup(EnvPrefix(componentID) + "_" + EnvPrefix(port) + "_ENDPOINT")
}

func (r *Runtime) lookup(name string) (Endpoint, bool) {
	if r.addresses == nil {
		return Endpoint{}, false
	}
	raw, ok := r.addresses.Lookup(name)
	if !ok || raw == "" {
		return Endpoint{}, false
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return Endpoint{URL: raw, Host: raw}, true
	}
	return Endpoint{URL: raw, Host: u.Host}, true
}
