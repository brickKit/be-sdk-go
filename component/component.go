// Package component is what a component hands the launcher or the shell: its identity, its declared configuration
// keys and the one module entry, used the same way when it runs alone and when a shell hosts it.
package component

import (
	"context"
	"io/fs"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/runtime"
)

// Definition describes a component. Build it in the component's root package and pass it to launcher.Main (single
// run) or list it in a shell.
type Definition struct {
	ID           string // component ID, e.g. erp/sales
	Version      string // the version this code is; checked against COMPONENT_VERSION at start
	Keys         []config.Key
	Capabilities []string
	// Migrations are the component's migration files (<version>_<title>.up.sql at the root). Non-nil means the
	// component owns a database: the PG_* keys are added to Keys, rt.DB is set, and the migrate entry applies them.
	Migrations fs.FS
	// GRPCPort is the "grpc" extra port declared in component.yaml; the system surface listens on it when the module
	// registers services (in a shell the port comes from BrickKit instead).
	GRPCPort int
	// New builds the module from its runtime. It must not read the process environment, start listeners, initialise
	// process-wide state or exit the process: everything comes from rt.
	New func(rt *runtime.Runtime) (Module, error)
}

// Module is a built component: it registers its user-surface routes.
type Module interface {
	Routes(r *httpx.Routes) error
}

// SystemModule is implemented by modules with a system surface: they register gRPC services, each method declaring
// its operation.
type SystemModule interface {
	Services(s *grpcx.Services) error
}

// Starter is implemented by modules with background work (consumers, work items); Start returns once that work
// runs, and before the member reports ready.
type Starter interface {
	Start(ctx context.Context) error
}

// Stopper is implemented by modules that must finish something on stop (complete the current transaction, release
// leases). The context ends at the stop deadline.
type Stopper interface {
	Stop(ctx context.Context) error
}
