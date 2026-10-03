// Package pg is the runtime's PostgreSQL layer (be-protocol P10, P11): the shared physical pool, the
// per-member Store that opens every transaction with the protocol's SET LOCAL batch, the transaction
// handle components hand to sqlc, the start-up probe, and the migration entry points (component
// migrations plus the platform migration) on a dedicated owner connection.
//
// Merge-safety: nothing here is package-level mutable state, nothing reads the process environment
// and nothing logs through slog.Default; every pool, store and migration is an instance with its own
// configuration, so N shell members share one process safely (P19).
package pg
