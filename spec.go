// Package besdk is the official Go implementation of the BrickEnterprise component protocol
// (be-protocol 1.0). A component declares itself with a Spec and calls Main from its main function;
// the SDK owns the process, the ports, the engine, the database identity, the bus and every timeout,
// and hands the component a Runtime.
//
//	var Spec = besdk.Spec{ID: "erp/sales", Manifest: manifest, Migrations: migrations.FS,
//		Contracts: contracts.FS, New: New}
//	func main() { besdk.Main(Spec) }
package besdk

import (
	"context"
	"io/fs"

	"google.golang.org/grpc"
)

// Spec declares a component (sdk-redesign §5.2). A shell imports the same Spec.
type Spec struct {
	ID         string // "erp/sales"; must equal COMPONENT_ID and the manifest's metadata.id (exit 78)
	Manifest   []byte // the component's own component.yaml (go:embed): configSchema, ports, events
	Migrations fs.FS  // <version>_<name>.up.sql / .down.sql + lifecycle.yaml; nil = no database
	Contracts  fs.FS  // contracts/: errors.yaml, events/*.events.json; nil = none
	New        func(ctx context.Context, rt *Runtime) (*Module, error)
}

// Module is what a component's New returns: declarations only, the SDK runs them (P1.10).
type Module struct {
	HTTP func(r *Router)      // user-plane routes; the engine and middleware are the SDK's (P3)
	GRPC func(s *grpc.Server) // system-plane services (P7)
	// Events: published and consumed events (P12) — added with the events wave.
	Start func(ctx context.Context) error // one-time initialisation, at most 30 s; never a loop
	Stop  func(ctx context.Context) error // called once on shutdown, after the servers stopped
}
