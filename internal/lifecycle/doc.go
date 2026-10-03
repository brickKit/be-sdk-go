// Package lifecycle is the data lifecycle of be-protocol P16 for the hot and warm tiers: the
// declaration migrations/lifecycle.yaml v1 (Load, Parse: schema, P16.1 invariants, P11.11 coverage),
// the deployment value DATA_LIFECYCLE (ParseConfig, Config.Apply: adapters, lengthen-only
// overrides, P16.9), the partition windows (RangeWindow, OutboxWindow, EnsureWindows,
// EnsureListPartition: P11.3, P16.6, G1) and the engine run as the singleton job be.lifecycle
// (Engine.Step, Engine.Seal: P16.2, P16.5, P16.7, G2, G3, G5, G6, G11).
//
// With cold store and cold query `none` (the only adapters of this SDK version) nothing is frozen,
// exported or destroyed from document, ledger or audit tables (G4); only platform (outbox) and queue
// partitions expire. The engine never runs DDL itself: it calls the platform's SECURITY DEFINER
// functions (ddl/10-lifecycle-functions.sql) as the runtime role (P10.12).
//
// The package does not import internal/pg (pg's migration step imports it): *sql.Tx and *pg.Tx both
// satisfy Querier, *pg.Tx satisfies Tx, and the root adapts pg.Store.Run to RunFunc.
package lifecycle
