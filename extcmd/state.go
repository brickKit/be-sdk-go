// Package extcmd is an adapter component's external commands (be-protocol chapter 13; the assembly project's
// protocol 10): a command is written in the business transaction, a job sends it, and a result that is not known is
// queried before anything is sent again. The state table is Step, a pure function: the job reads a command, asks Step
// what an event makes of it, and writes the answer.
package extcmd

import "time"

// States (P13.3).
const (
	Prepared  = "prepared"
	RetryWait = "retry_wait"
	Sending   = "sending"
	Unknown   = "unknown"
	Accepted  = "accepted"
	Manual    = "manual"
	Succeeded = "succeeded"
	Failed    = "failed"
	Rejected  = "rejected"
)

// Send results as an adapter classifies them (P13.5).
const (
	NotReached   = "not_reached"   // certainly not processed: a connection failure, an explicit rate limit
	MaybeReached = "maybe_reached" // may have been processed: a read timeout, a gateway error, a cut before the answer
)

// Event kinds.
const (
	Claim          = "claim"           // the job takes a prepared or retry_wait command
	BeforeSend     = "before_send"     // the last check before the request leaves
	LeaseLost      = "lease_lost"      // a sending command whose claimant is gone
	SendResult     = "send_result"     // Result: not_reached, maybe_reached, accepted, succeeded, failed, rejected
	QueryFound     = "query_found"     // External: the external system's state
	QueryNotFound  = "query_not_found" // the external system has no record of the reference
	QueryDown      = "query_unavailable"
	FinalityPassed = "finality_passed" // accepted for longer than the adapter's finality limit
	Registered     = "registered"      // Result: a person's verified outcome of a manual command
)

// WhyExpired is the reason of a command failed at its local abandon time (P13.14): an adapter's settle hook tells an
// expiry from a failure by it.
const WhyExpired = "past the local abandon time: expired"

// Parameters (the assembly project's protocol 10).
const (
	WarnAfter   = time.Hour      // unknown this long: alert
	ManualAfter = 24 * time.Hour // unknown, or accepted without a query, this long: manual
	MinMargin   = time.Hour      // the resend deadline's safety margin, at least
)

// Capabilities are what the external system provides for one operation (P13.9).
type Capabilities struct {
	DedupeWindow  time.Duration // deduplicates by the business reference for this long; 0: does not
	LastUse       bool          // the window counts from the last use (else from the first)
	Query         bool          // answers a query by the business reference
	Expiry        bool          // honours a request's expiry
	MaxProcessing time.Duration // a request older than this no longer lands; 0: not declared
	Finality      time.Duration // an accepted request reaches its final state within this; 0: not declared
	Irrevocable   bool          // repeating it has business consequences (money, invoices, signatures, carrier orders)
}

// Facts are a command's times as Step reads them.
type Facts struct {
	State                       string
	ResendDeadline, ExpiresAt   *time.Time
	FirstReached, LastReached   *time.Time
	UnknownSince, AcceptedSince *time.Time
	// UnrecordedUntil: after a full recovery, sends may have gone on until this moment without a record (P13.12).
	UnrecordedUntil *time.Time
	// AbandonAt is the local abandon time (P13.14): past it a prepared, retry_wait or unknown command fails as expired.
	AbandonAt *time.Time
}

// Event is one thing that happened to a command.
type Event struct {
	Kind     string
	Result   string // SendResult, Registered
	External string // QueryFound: accepted, succeeded, failed, rejected, or anything not final
}

func terminal(s string) bool { return s == Succeeded || s == Failed || s == Rejected }

// Deadline is the resend deadline (P13.7): nil without dedupe or without a send that may have reached the external
// system.
func Deadline(f Facts, c Capabilities) *time.Time {
	if c.DedupeWindow <= 0 {
		return nil
	}
	base := f.FirstReached
	if c.LastUse && f.LastReached != nil {
		base = f.LastReached
	}
	if base == nil {
		return nil
	}
	m := max(c.DedupeWindow/10, MinMargin)
	d := base.Add(c.DedupeWindow - m)
	return &d
}

// resendable answers a "not found" query (P13.6): only when the earlier request can no longer land.
func resendable(f Facts, c Capabilities, now time.Time) (bool, string) {
	if d := Deadline(f, c); d != nil && !now.After(*d) {
		return true, "the external system deduplicates and the deadline has not passed"
	}
	if f.ExpiresAt != nil && now.After(*f.ExpiresAt) {
		return true, "the request's expiry has passed"
	}
	latest := f.LastReached
	if f.UnrecordedUntil != nil && (latest == nil || f.UnrecordedUntil.After(*latest)) {
		latest = f.UnrecordedUntil
	}
	if c.MaxProcessing > 0 && latest != nil && now.Sub(*latest) > c.MaxProcessing {
		return true, "the longest processing time has passed"
	}
	return false, "the earlier request may still land"
}

// Step is the state table (P13.3-P13.8), first matching row wins: the next state and why. Terminal states absorb
// everything; an event the table has no row for leaves the state as it is (the caller logs it).
func Step(f Facts, e Event, c Capabilities, now time.Time) (string, string) {
	s := f.State
	if terminal(s) {
		return s, "a terminal state absorbs every later result"
	}
	if f.AbandonAt != nil && now.After(*f.AbandonAt) && (s == Prepared || s == RetryWait || s == Unknown) {
		return Failed, WhyExpired // P13.14: not sent, queried or awaited any more
	}
	past := f.ResendDeadline != nil && now.After(*f.ResendDeadline)
	switch {
	case e.Kind == Claim && (s == Prepared || s == RetryWait):
		if past {
			return Unknown, "claimed past the resend deadline"
		}
		return Sending, "claimed: the transition commits before the send"
	case e.Kind == BeforeSend && s == Sending:
		if past {
			return Unknown, "past the resend deadline before the send: not sent"
		}
		return Sending, "sent"
	case e.Kind == LeaseLost && s == Sending:
		return Unknown, "the claimant is gone: whether it sent is not known"
	case e.Kind == SendResult && s == Sending:
		switch e.Result {
		case NotReached:
			return RetryWait, "certainly not processed: backoff and send again"
		case MaybeReached:
			return Unknown, "may have been processed: query first"
		case Accepted:
			return Accepted, "accepted, not final"
		case Succeeded, Failed:
			return e.Result, "final"
		case Rejected:
			return Rejected, "refused for good"
		}
	case e.Kind == QueryFound && (s == Unknown || s == Accepted):
		if terminal(e.External) || e.External == Accepted {
			return e.External, "the external record"
		}
		return s, "the external record, not final"
	case e.Kind == QueryDown && (s == Unknown || s == Accepted):
		since := f.UnknownSince
		if s == Accepted {
			since = f.AcceptedSince
		}
		if since != nil && now.Sub(*since) > ManualAfter {
			if s == Unknown && f.AbandonAt != nil { // P13.14: never left for a person
				return s, "past the manual limit with a local abandon time: waits for it"
			}
			return Manual, "no answer within the manual limit"
		}
		return s, "the query is unavailable: backoff"
	case e.Kind == QueryNotFound && s == Unknown:
		ok, why := resendable(f, c, now)
		switch {
		case ok:
			return Sending, why
		case f.AbandonAt != nil: // P13.14: never left for a person
			return s, "not found and not to be sent again: waits for the local abandon time"
		}
		return Manual, why
	case e.Kind == FinalityPassed && s == Accepted:
		if c.Query {
			return Accepted, "past the finality limit: query"
		}
		if f.AcceptedSince != nil && now.Sub(*f.AcceptedSince) > ManualAfter {
			return Manual, "no query and past the manual limit"
		}
		return Accepted, "waiting for the callback or the reconciliation"
	case e.Kind == Registered && s == Manual:
		return e.Result, "registered by a person, with its grounds"
	}
	return s, "not a transition of the table: unchanged, logged"
}

// ParamsRecord is the record type the parameters' digest is computed under (P13.2).
const ParamsRecord = "be.external.command_params"
