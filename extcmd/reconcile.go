package extcmd

import (
	"context"
	"sort"
	"time"
)

// The conclusions of a full recovery's external reconciliation (P13.13).
const (
	ConclusionMatched          = "matched"           // the external record: final, or accepted
	ConclusionDeadline         = "deadline"          // a resend deadline written, the state unchanged
	ConclusionUnknown          = Unknown             // the earlier request can no longer land: queried, then sent again
	ConclusionManual           = Manual              // it may still land: a person decides
	ConclusionExternalOnly     = "external_only"     // the external system has it, this member does not
	ConclusionPossiblyExecuted = "possibly_executed" // an issue record, no command, no external record
)

// Record is one external record a Lister lists.
type Record struct {
	Reference  string
	State      string
	ExternalID string
	Outcome    map[string]any
	Response   string
}

// Lister is an adapter that also lists the external system's records in a time window (P13.11): the reconciliation
// then matches one listing instead of one query per command.
type Lister interface {
	List(ctx context.Context, from, to time.Time) ([]Record, error)
}

// Open is a local command not final, as the reconciliation reads it.
type Open struct {
	Reference string
	Command   string
	Operation string
	Facts
	SendingAt *time.Time // the start of a send attempt that has not ended
}

// Conclusion is the reconciliation's conclusion on one business reference.
type Conclusion struct {
	Reference      string
	Command        string // empty: external_only, possibly_executed
	Conclusion     string
	State          string     // matched: the state taken from the external record
	ResendDeadline *time.Time // deadline
	FirstReached   *time.Time // not matched: the earliest send that may have reached, written with UnrecordedUntil
	Detail         string
}

// Match is the step 7 matching (P13.12), first matching row wins: open are the local commands not final, external
// the external records' states by reference, local every reference this member has a command for (final ones too),
// issued the issue records' references; unrecordedUntil is until when sends may have gone on without a record (the
// failure). It answers one conclusion per reference, ordered by reference.
func Match(open []Open, external map[string]string, local map[string]bool, issued []string,
	caps func(operation string) Capabilities, t, unrecordedUntil, now time.Time) []Conclusion {
	out := map[string]Conclusion{}
	for _, o := range open {
		c := Conclusion{Reference: o.Reference, Command: o.Command}
		if s, ok := external[o.Reference]; ok {
			c.Conclusion, c.State = ConclusionMatched, Accepted
			if terminal(s) {
				c.State = s
			}
			out[o.Reference] = c
			continue
		}
		if o.State == Manual {
			c.Conclusion, c.Detail = ConclusionManual, "manual: only a person registers it"
			out[o.Reference] = c
			continue
		}
		f := o.Facts
		f.FirstReached = earliest(o, t)
		f.UnrecordedUntil = &unrecordedUntil
		c.FirstReached = f.FirstReached
		cp := caps(o.Operation)
		if d := Deadline(f, cp); d != nil && !now.After(*d) {
			c.Conclusion, c.ResendDeadline = ConclusionDeadline, d
			out[o.Reference] = c
			continue
		}
		f.ResendDeadline = nil
		cp.DedupeWindow = 0
		ok, why := resendable(f, cp, now)
		c.Conclusion, c.Detail = ConclusionManual, why
		if ok {
			c.Conclusion = ConclusionUnknown
		}
		out[o.Reference] = c
	}
	for ref := range external {
		if _, seen := out[ref]; !seen && !local[ref] {
			out[ref] = Conclusion{Reference: ref, Conclusion: ConclusionExternalOnly}
		}
	}
	for _, ref := range issued {
		if _, seen := out[ref]; !seen && !local[ref] {
			out[ref] = Conclusion{Reference: ref, Conclusion: ConclusionPossiblyExecuted}
		}
	}
	list := make([]Conclusion, 0, len(out))
	for _, c := range out {
		list = append(list, c)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Reference < list[j].Reference })
	return list
}

// earliest is the earliest send of an open command that may have reached the external system (P13.12): the first
// that may have, else the start of a send attempt that has not ended, else the recovery point.
func earliest(o Open, t time.Time) *time.Time {
	switch {
	case o.FirstReached != nil:
		return o.FirstReached
	case o.SendingAt != nil:
		return o.SendingAt
	}
	return &t
}
