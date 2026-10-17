// Package runtime is the only door a module has to the outside: its member identity, configuration, dependency
// addresses, logger, metrics and tracer (component implementation conventions: "the runtime is the only entry").
// The launcher (single run) or the shell builds one Runtime per member.
package runtime

import (
	"log/slog"
	"net/url"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/obs"
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

	addresses config.Source // where the platform's *_ENDPOINT values are looked up
	calls     *calls.Member
}

// New is used by the launcher and the shell.
func New(m obs.Member, cfg *config.Values, logger *slog.Logger, metrics prometheus.Registerer, tracer trace.Tracer,
	addresses config.Source, database *db.DB, outbound *calls.Member) *Runtime {
	return &Runtime{Member: m, Config: cfg, Logger: logger, Metrics: metrics, Tracer: tracer, DB: database,
		addresses: addresses, calls: outbound}
}

// HTTP returns the client for a dependency's user surface (protocol chapter 04: deadline, retries, breaker and limit
// per dependency). ok=false: the dependency is absent.
func (r *Runtime) HTTP(componentID string) (*calls.HTTP, bool) {
	ep, ok := r.Dependency(componentID)
	if !ok || r.calls == nil {
		return nil, false
	}
	return r.calls.HTTP(componentID, ep.URL), true
}

// GRPC returns the connection to a dependency's system surface (its "grpc" port). Mark idempotent methods with
// calls.Idempotent() and commands with calls.IdempotencyKey(key). ok=false: the dependency is absent.
func (r *Runtime) GRPC(componentID string) (*grpc.ClientConn, bool) {
	ep, ok := r.DependencyPort(componentID, "grpc")
	if !ok || r.calls == nil {
		return nil, false
	}
	conn, err := r.calls.GRPC(componentID, ep.Host)
	if err != nil {
		r.Logger.Error("the system surface of a dependency cannot be dialled", "dependency", componentID, "error", err.Error())
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
