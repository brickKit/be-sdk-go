package envelope

import "time"

// Outcome is the result of running a handler (P12.7).
type Outcome int

// Handler outcomes (P12.7).
const (
	OutcomeOK        Outcome = iota // success: acknowledge
	OutcomeError                    // a retryable error: nak with delay
	OutcomePermanent                // a permanent error (unparsable payload, contract violation): dead-letter at once
)

// ActionKind is what the subscription runtime does with a message (P12.7).
type ActionKind int

// Action kinds (P12.7).
const (
	ActionAck        ActionKind = iota // Ack
	ActionNak                          // NakWithDelay(Delay)
	ActionDeadLetter                   // publish to the dead letters with DLQMsgID, then Term
)

// Action is the runtime's decision for one delivery (P12.7, vectors envelope/cursor redelivery).
type Action struct {
	Kind     ActionKind
	Delay    time.Duration // ActionNak
	Reason   string        // ActionDeadLetter: be-dlq-reason, MAX_DELIVER or PERMANENT
	DLQMsgID string        // ActionDeadLetter: dlq:<durable>:<stream sequence>
	Handled  bool          // whether the handler ran for this delivery
}

// MaxDeliverReached reports whether a delivery must be dead-lettered on receipt, before any
// handler runs (P12.7): the broker's delivery count (1-based) is above maxDeliver. A maxDeliver ≤ 0
// means DefaultMaxDeliver.
func MaxDeliverReached(delivery, maxDeliver int) bool {
	if maxDeliver <= 0 {
		maxDeliver = DefaultMaxDeliver
	}
	return delivery > maxDeliver
}

// Redelivery decides what happens to one delivery (P12.5, P12.7). The broker's durable has
// ServerMaxDeliver and no backoff; the runtime decides:
//   - delivery above maxDeliver: dead-letter MAX_DELIVER without running the handler (outcome is
//     ignored, Handled false);
//   - otherwise the handler ran (Handled true): ok acks; a permanent error dead-letters PERMANENT;
//     an error naks with backoff[min(delivery, len) − 1], so the last allowed delivery still naks.
//
// maxDeliver ≤ 0 means DefaultMaxDeliver and an empty backoff DefaultBackoff (EVENTS_MAX_DELIVER /
// EVENTS_BACKOFF are resolved by the caller).
func Redelivery(delivery, maxDeliver int, backoff []time.Duration, outcome Outcome, durable string, streamSeq uint64) Action {
	if MaxDeliverReached(delivery, maxDeliver) {
		return Action{Kind: ActionDeadLetter, Reason: ReasonMaxDeliver, DLQMsgID: DLQMsgID(durable, streamSeq)}
	}
	switch outcome {
	case OutcomeOK:
		return Action{Kind: ActionAck, Handled: true}
	case OutcomePermanent:
		return Action{Kind: ActionDeadLetter, Reason: ReasonPermanent, DLQMsgID: DLQMsgID(durable, streamSeq), Handled: true}
	default:
		return Action{Kind: ActionNak, Delay: NakDelay(delivery, backoff), Handled: true}
	}
}

// NakDelay returns the delay of a negative acknowledgement after a failed delivery (P12.7):
// backoff[n − 1] for the broker's delivery count n, the last value repeating; an empty backoff
// means DefaultBackoff.
func NakDelay(delivery int, backoff []time.Duration) time.Duration {
	if len(backoff) == 0 {
		backoff = DefaultBackoff()
	}
	i := min(max(delivery, 1), len(backoff)) - 1
	return backoff[i]
}
