// Package idem is command idempotency (be-protocol P13, P3.7): the RFC 8785 (JCS) canonicaliser and
// the request fingerprint over I-JSON text, the caller namespaces, the header/body key resolution,
// the 30-day expiry, the pure decision of vectors/idempotency, and the besdk_idempotency table
// operations (Claim, Lookup, Get, Complete, Release, DeleteExpired) plus the one-step Idempotent
// helper the root exposes as besdk.Idempotent.
//
// Every table operation takes a Querier (a *pg.Tx) and runs inside the caller's transaction, and
// every time comes from the now argument (the runtime clock), never from SQL now(). Nothing here
// holds package-level mutable state; ErrMismatch and ErrInProgress are shared read-only values.
package idem
