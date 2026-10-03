// Package events is the event runtime of be-sdk-go (be-protocol P12): the events contract
// (P12.2), the outbox write inside the caller's business transaction (P12.1, P12.8, P11.8), the
// outbox pump (P12.1), the consumer runner with its aggregate-stream cursor and dead letters
// (P12.5–P12.9) and the bus topology (P12.4, P12.5).
//
// Merge-safety: nothing here is package-level mutable state; every pump and consumer is an
// instance with its own store, logger and metric hooks, so N shell members share one process.
// The package knows no root type: the root maps its public Event, Subscription and Permanent onto
// the types here.
package events
