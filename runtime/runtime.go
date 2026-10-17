// Package runtime is the only door a module has to the outside: its member identity, configuration, dependency
// addresses, logger, metrics and tracer (component implementation conventions: "the runtime is the only entry").
// The launcher (single run) or the shell builds one Runtime per member.
package runtime

import (
	"log/slog"
	"net/url"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
)

// Runtime is what a module receives. Fields are set by the launcher; modules only read them.
type Runtime struct {
	Member  obs.Member
	Config  *config.Values
	Logger  *slog.Logger
	Metrics prometheus.Registerer
	Tracer  trace.Tracer

	addresses config.Source // where the platform's *_ENDPOINT values are looked up
}

// New is used by the launcher and the shell.
func New(m obs.Member, cfg *config.Values, logger *slog.Logger, metrics prometheus.Registerer, tracer trace.Tracer,
	addresses config.Source) *Runtime {
	return &Runtime{Member: m, Config: cfg, Logger: logger, Metrics: metrics, Tracer: tracer, addresses: addresses}
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
