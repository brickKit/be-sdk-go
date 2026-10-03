// Package envelope holds the pure computations of the event runtime (be-protocol P11.5, P12): event
// ids, subject and durable names, the outbound CloudEvents headers, causation and hop count, the
// inbound accept-or-dead-letter decision, the redelivery decision and the state-mode cursor rule.
// It does no I/O: the outbox pump and the subscription runtime call these functions and only do the
// wiring. Every function is checked against the be-protocol vectors `envelope`.
package envelope
