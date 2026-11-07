// Package db is the only way a module reaches its database (protocol 14, "database"): one transaction entry that
// switches to the member's role and schema with SET LOCAL and sets per-transaction timeouts; pools per database
// target, owned by the launcher or the shell; startup checks of the durable settings; the migrate entry.
//
// The files:
//
//   - keys.go: the configuration keys of serve and migrate, the target and its settings.
//   - pool.go: one physical pool per target (Open); check.go: the start checks of the durable settings (P1.10).
//   - tx.go: the one transaction entry (InTx, DB.Tx, DB.BackgroundTx): role, search_path and timeouts per transaction
//     with set_config(..., true) (P5.5); after-commit and after-rollback hooks.
//   - session.go: refuses statements that leave state on a connection (P5.30, P5.31); pool.go compares the server
//     parameters before a connection goes back.
//   - migrate.go: the migrate entry, the SDK's own migrations and the lock waits; lint.go: the migration check
//     (vector migration-lint).
//   - docno.go: document number series, gapped and continuous, and their raise in a recovery (protocol 14).
//   - partitions.go: the declared tables' monthly partitions (P5.22); batch.go: deletes in bounded batches;
//     statistics.go: analyzing tables whose statistics lag (P5.33).
package db
