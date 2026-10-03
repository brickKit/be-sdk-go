// Package jobs is the runtime's background-work engine (be-protocol P14): the five declared kinds
// (every, singleton, cron, queue, reconciler) over the tables of ddl/05-jobs.sql in the member's own
// schema, JOBS_OVERRIDES (P14.5), the run-once entry `job run <name>` (P14.8), the P14.3 metrics and
// the retention helpers of the runtime's be.cleanup job (P14.7).
//
// The engine starts no goroutine of its own: Engine.Loops returns named loops that the root runs
// under its supervisor (P1.7), which recovers a failure or panic and restarts the loop with backoff.
// A loop may start goroutines scoped to one call (lease renewal, a worker's concurrent handlers) and
// waits for them before it returns.
//
// Merge-safety: no package-level mutable state, no process environment, no slog.Default; one Engine
// per member, its holder named after the member (P19).
package jobs
