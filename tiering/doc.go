// Package tiering is hot and cold data (be-protocol chapter 15; the assembly project's protocol 11): the owner's unit
// catalog, the classes a component declares, candidates and sealing behind a write barrier, the transfer to a route
// (the warm rows in the same database, or Parquet with a manifest on the object store) under the claim's generation
// and epoch, the release of the online rows, reads by catalog snapshot, holds, destruction and erasure.
//
// A component declares its classes in its definition (component.Definition.Tiering); the host builds the member's
// catalog (NewCatalog: the deployment's residence, retention and route, the cold store, the columns checked against
// the tables at start), runs the SDK's jobs, registers the recovery check before the component's own and the standard
// routes after the module's; the module reaches it as rt.Tiering and registers its blockers by implementing
// Participant, and its readers with Catalog.Register.
//
// The files:
//
//   - class.go: a class's declaration and its check (P15.1-P15.5); children may nest (Child.Parent; rootExpr walks a
//     row up to its root), a leaf that is no one's parent may have a key of several columns (Table.Key).
//   - rows.go: the canonical row of every column type, text[] included, and a unit's digest (P15.8, P15.9);
//     FromCanonical turns a canonical row back into the driver's values a copy writes.
//   - plan.go: the plan, a pure function from a unit and the time to its action and what holds it back (P15.37).
//   - manifest.go: the manifest and the objects' keys (P15.16).
//   - barrier.go: the write barrier's trigger (P15.13), created by migrate after the component's migrations, its
//     search_path pinned to the one it was created under.
//   - catalog.go: groupings and sealing (P15.6, P15.11, P15.12); the Catalog and its options.
//   - member.go: the configuration keys, the catalog's construction, the columns check and the candidates job.
//   - transfer.go: claim, copy, verify, publish (P15.14-P15.18); the orphans' cleanup and its daily job
//     be.tiering.cleanup (P15.20).
//   - release.go: the release of a published unit's online rows (P15.19), the latest backup read before its
//     transaction.
//   - reads.go: reads by key, the snapshot and its lease, the cold units of a list, thawing (P15.22-P15.27).
//   - history.go: the history read with its limit, and the asynchronous export (P15.26, P15.38); a component
//     registers a Reader, whose online part decides each row with authz.Subject.Decider.
//   - holds.go: holds and destruction approvals (P15.28, P15.29).
//   - destroy.go: destruction and its replay from the evidence (P15.29, P15.30).
//   - erase.go: erasure (P15.31): a sealed unit's online rows and digest in one transaction, a published or released
//     unit written again as a new manifest version (rows through FromCanonical); every erasure voids the class's
//     exports (VoidExports); a unit that moved on under it is erased again from its state now.
//   - reroute.go: writing a unit again: the route change (P15.35), the old versions' release (P15.39), the older
//     structures' converters and the readable check (P15.33, P15.34).
//   - check.go: the recovery check be.tiering (P15.36).
//   - routes.go: the standard routes of holds, approvals and exports' status (P15.40).
//   - store.go: the cold store, one bucket and three credentials (write, read, delete); parquet.go: the Parquet files.
//   - crash.go: the crash points of the long simulation (tx: every background transaction of the jobs; the hook is
//     set only by tests, sim_test.go).
package tiering
