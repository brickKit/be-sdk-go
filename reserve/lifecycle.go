// Package reserve is a reservation participant's side of the assembly project's protocol 04 (be-protocol chapter 16):
// the branches a coordinator holds on a resource owner (stock, credit, later budgets), their states and transitions,
// the idempotency by branch ID and content digest, the tombstone, the hold period and its expiry job, consumptions by
// consumption ID, the lock order and an event on every change.
//
// The runtime keeps a branch's state in its own tables (be_reserve_branch, be_reserve_consumption; be-protocol's
// migration 0024); what a branch takes is the component's, in its own tables keyed by the branch's ID. A component
// describes one participation as a Kind (how to lock its resources, give back what a branch has not consumed, and
// publish a branch), and passes what only the command knows as a function: what a hold takes, what a consumption
// takes off. The component answers on its own contract: Answer says what happened, the words are the component's.
//
// The files:
//
//   - lifecycle.go: the transitions as one table, Decide (checked against the protocol's vector reserve-participant).
//   - reserve.go: Kind, Branch, Answer; Hold, Confirm, Cancel, Query, Consume (each in the component's transaction),
//     ExpireDue.
package reserve

// State is a stored branch's state (P16.1); Absent is the answer for a branch not stored, never stored itself.
type State string

const (
	Held      State = "held"
	Committed State = "committed"
	Consumed  State = "consumed"
	Released  State = "released"
	Expired   State = "expired"
	Rejected  State = "rejected"
	Cancelled State = "cancelled"
	Absent    State = "absent"
)

// States are the stored states, in the protocol's order.
var States = []State{Held, Committed, Consumed, Released, Expired, Rejected, Cancelled}

// Ended reports a final state: nothing moves it any more (P16.1).
func (s State) Ended() bool { return s != Held && s != Committed && s != Absent }

// Command is what arrives on a branch: the coordinator's commands and the participant's own expiry job. The hold and
// the consumption come in the forms the table tells apart: a new branch with its resources enough or short, an
// existing one with the same or other content; a new consumption ID within, exactly at or beyond what is left, or a
// used one with the same or other content.
type Command string

const (
	OnQuery            Command = "query"
	OnConfirm          Command = "confirm"
	OnCancel           Command = "cancel"
	OnExpire           Command = "expire"
	OnHoldEnough       Command = "hold-enough"
	OnHoldShort        Command = "hold-short"
	OnHoldSame         Command = "hold-same"
	OnHoldOther        Command = "hold-other"
	OnConsumeWithin    Command = "consume-within"
	OnConsumeExact     Command = "consume-exact"
	OnConsumeBeyond    Command = "consume-beyond"
	OnConsumeUsedSame  Command = "consume-used-same"
	OnConsumeUsedOther Command = "consume-used-other"
)

// Resource is what a transition does to the component's resource.
type Resource string

const (
	Untouched Resource = "none"
	Takes     Resource = "take"      // the hold's content is taken (the component did it while answering enough)
	GivesBack Resource = "give_back" // what the branch has not consumed comes back
	Consumes  Resource = "consume"   // the consumption is taken off what the branch holds
)

// Refusal is why a consumption is refused (P16.7); "" none.
type Refusal string

const (
	NoBranch        Refusal = "absent"
	NotCommitted    Refusal = "not_committed"
	BeyondRemaining Refusal = "beyond_remaining"
)

// Decision is one cell: the state after, the answer (a conflict, a refusal, or neither), what happens to the resource,
// and whether the branch changed (its version raised, its event published).
type Decision struct {
	To       State
	Conflict bool
	Refused  Refusal
	Resource Resource
	Changed  bool
}

// Decide is the table of P16.3-P16.8: what command c does to a branch in state s (Absent: none stored), past telling
// whether a held branch's hold period is over by the database's clock.
func Decide(s State, past bool, c Command) Decision {
	stay := Decision{To: s, Resource: Untouched}
	switch c {
	case OnQuery: // P16.6
		return stay
	case OnHoldEnough, OnHoldShort: // P16.3, a new branch
		if s != Absent {
			return stay
		}
		if c == OnHoldEnough {
			return Decision{To: Held, Resource: Takes, Changed: true}
		}
		return Decision{To: Rejected, Resource: Untouched, Changed: true}
	case OnHoldSame: // P16.3, a branch seen before: answered as it is
		return stay
	case OnHoldOther: // a tombstone answers cancelled whatever the content; else a conflict
		if s == Cancelled {
			return stay
		}
		return Decision{To: s, Conflict: true, Resource: Untouched}
	case OnConfirm: // P16.4
		if s != Held {
			return stay
		}
		if past {
			return Decision{To: Expired, Resource: GivesBack, Changed: true}
		}
		return Decision{To: Committed, Resource: Untouched, Changed: true}
	case OnCancel: // P16.5
		switch s {
		case Absent:
			return Decision{To: Cancelled, Resource: Untouched, Changed: true}
		case Held, Committed:
			return Decision{To: Released, Resource: GivesBack, Changed: true}
		}
		return stay
	case OnExpire: // P16.8: under the lock, judged again
		if s == Held && past {
			return Decision{To: Expired, Resource: GivesBack, Changed: true}
		}
		return stay
	case OnConsumeUsedSame: // P16.7
		return stay
	case OnConsumeUsedOther:
		return Decision{To: s, Conflict: true, Resource: Untouched}
	case OnConsumeWithin, OnConsumeExact, OnConsumeBeyond:
		switch {
		case s == Absent:
			return Decision{To: s, Refused: NoBranch, Resource: Untouched}
		case s != Committed:
			return Decision{To: s, Refused: NotCommitted, Resource: Untouched}
		case c == OnConsumeBeyond:
			return Decision{To: s, Refused: BeyondRemaining, Resource: Untouched}
		case c == OnConsumeExact:
			return Decision{To: Consumed, Resource: Consumes, Changed: true}
		}
		return Decision{To: Committed, Resource: Consumes, Changed: true}
	}
	panic("reserve: no rule for command " + string(c))
}
