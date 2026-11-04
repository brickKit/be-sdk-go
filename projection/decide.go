package projection

import (
	"strconv"
	"strings"
)

// The watermark source's decision tables (chapter 09, P9.22-P9.29; protocol 02 steps 6, 9, 10 and its alignment
// state table). Pure functions: the watermark-* vectors of be-protocol run against them, and the consumer's code
// (watermark.go) only reads its state, calls them and writes what they return.

// Alignment states of a watermark source (P9.26).
const (
	Aligned  = "aligned"
	Aligning = "aligning"
	Stalled  = "stalled"
	Blocked  = "blocked"
)

// Judgements (P9.24).
const (
	Usable         = "usable"
	ProvablyAbsent = "provably_absent"
	Unprovable     = "unprovable"
	QueryTheSource = "query_the_source"
	ServingState   = "serving" // a projection's own state when nothing rebuilds it
	UntrustedState = "untrusted"
	AligningState  = "aligning"
)

// JudgeInput is what a judgement reads (protocol 02 step 9).
type JudgeInput struct {
	// LastApplied asks by the last applied facts (P9.34): a stalled source is judged by its rows too, and the
	// watermark's age does not count. Only for security 03's tightening-only operations.
	LastApplied     bool
	WatermarkSource bool
	SourceState     string
	ProjectionState string
	Quarantined     bool // the subjects asked about or behind the rows read
	HasWatermark    bool
	WatermarkAge    float64 // seconds, on the consumer primary's clock
	Freshness       float64 // seconds
	Row             *JudgedRow
}

// JudgedRow is the projected row a judgement read; nil: none.
type JudgedRow struct {
	State   string
	Adopted bool // of the adopted implementation instance and epoch
}

// Judge is protocol 02's judgement table, first matching row wins.
func Judge(q JudgeInput) string {
	switch {
	case !q.WatermarkSource:
		return QueryTheSource
	case q.SourceState != Aligned && !(q.LastApplied && q.SourceState == Stalled), q.ProjectionState != ServingState,
		q.Quarantined:
		return Unprovable
	case !q.LastApplied && (!q.HasWatermark || q.WatermarkAge > q.Freshness):
		return Unprovable
	case q.Row == nil:
		return ProvablyAbsent
	case !q.Row.Adopted:
		return Unprovable
	case q.Row.State == Complete:
		return Usable
	case q.Row.State == Tombstone:
		return ProvablyAbsent
	}
	return Unprovable
}

// Epoch decisions on a declared source identity (P9.27).
const (
	Establish = "establish"
	Use       = "use"
	Adopt     = "adopt"
	IgnoreIt  = "ignore"
	Stall     = "stall"
	Block     = "block"
	Discard   = "discard"
)

// Identity is a source identity: adopted by the consumer, or declared by a response.
type Identity struct {
	Deployment string
	Impl       string // empty in the adopted identity: the first contact
	SystemID   string
	Epoch      string // <recovery generation>.<timeline>
	Primary    bool   // a declaration: answered by the primary
	Purpose    string // a replica's answer: "adopt" and "align" may not use one
}

// DecideEpoch is P9.27 for one declaration against the adopted identity.
func DecideEpoch(adopted, r Identity) string {
	if r.Deployment != adopted.Deployment {
		return Block
	}
	if adopted.Impl == "" {
		if r.Primary {
			return Establish
		}
		return Discard
	}
	if r.Impl != adopted.Impl {
		return Block
	}
	ae, re := epochKey(adopted.Epoch), epochKey(r.Epoch)
	if !r.Primary {
		if r.SystemID == adopted.SystemID && r.Epoch == adopted.Epoch && r.Purpose != "adopt" && r.Purpose != "align" {
			return Use
		}
		return Discard
	}
	if r.SystemID != adopted.SystemID && re[0] <= ae[0] {
		return Stall // a new system without a larger recovery generation: restored without advancing it
	}
	switch cmpEpoch(re, ae) {
	case 1:
		return Adopt
	case -1:
		return IgnoreIt
	}
	return Use
}

// Generation is an epoch's recovery generation (its first part).
func Generation(epoch string) int64 { return epochKey(epoch)[0] }

func epochKey(e string) [2]int64 {
	g, t, _ := strings.Cut(e, ".")
	a, _ := strconv.ParseInt(g, 10, 64)
	b, _ := strconv.ParseInt(t, 10, 64)
	return [2]int64{a, b}
}

func cmpEpoch(a, b [2]int64) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// Pull outcomes for one partition answered by the adopted instance and epoch (P9.22, P9.29).
const (
	ApplyToEcho = "apply_to_echo"
	Realign     = "realign"
	StallIt     = "stall"
	Hold        = "hold"
)

// PullOutcome decides one partition of a pull: requested is the position the request carried (0: echo only).
func PullOutcome(requested int64, unreachable bool, need string, echo int64) string {
	switch {
	case unreachable:
		return Hold
	case need == "beyond_retention":
		return Realign
	case need == "rollback_detected":
		return StallIt
	case requested > 0 && echo < requested:
		return StallIt // the counter went back below what this request carried
	}
	return ApplyToEcho
}

// SourceState is what the consumer keeps of a source's alignment state (P9.26).
type SourceState struct {
	State      string
	Confirmed  bool   // blocked: an operator confirmed the correction
	Rollback   bool   // a cause of the alignment is a rollback
	StalledGen *int64 // the recovery generation when it entered stalled
	BlockedGen *int64 // ... when it entered blocked
	Prior      string // the state before blocked
}

// SourceEvent is an input to the alignment state table.
type SourceEvent struct {
	Kind     string // first_contact, adopt_epoch, beyond_retention, rollback, foreign, operator_confirms, primary_response, alignment_done, environment_copy
	Gen      int64  // the recovery generation the input declares
	Matches  bool   // primary_response: the adopted instance and deployment, the same system or a larger generation
	RowCheck string // primary_response after a confirmed correction: passed, failed
}

// Actions of the alignment state table.
const (
	ActRefetch         = "refetch"
	ActKeepRefetching  = "keep_refetching"
	ActWatermark       = "watermark_from_echoes"
	ActAlert           = "alert"
	ActAlertAndHold    = "alert_and_hold"
	ActHold            = "hold"
	ActConfirm         = "record_confirmation"
	ActRowCheckRefetch = "row_check_then_refetch"
	ActClearAndRefetch = "clear_and_refetch"
)

// NextState is protocol 02's alignment state table without step 12's rows, first matching row wins.
func NextState(cur SourceState, ev SourceEvent) (SourceState, string) {
	next, gen := cur, ev.Gen
	above := func(p *int64) bool { return p == nil || gen > *p }
	switch {
	case ev.Kind == "environment_copy":
		next.State, next.Rollback, next.Confirmed = Aligning, false, false
		return next, ActClearAndRefetch
	case ev.Kind == "foreign" && cur.State == Blocked:
		return next, ActAlert
	case ev.Kind == "foreign":
		next.State, next.Prior, next.BlockedGen = Blocked, cur.State, &gen
		return next, ActAlertAndHold
	case cur.State == Blocked:
		switch {
		case ev.Kind == "operator_confirms":
			next.Confirmed = true
			return next, ActConfirm
		case !cur.Confirmed || ev.Kind != "primary_response":
			return next, ActHold
		case !ev.Matches:
			next.Confirmed = false
			return next, ActAlert
		case cur.Prior == Stalled && !above(cur.StalledGen):
			next.State, next.Confirmed = Stalled, false
			return next, ActRefetch
		case ev.RowCheck == "failed":
			next.State, next.Confirmed, next.Rollback, next.StalledGen = Stalled, false, true, &gen
			return next, ActRefetch
		}
		next.State, next.Confirmed = Aligning, false
		return next, ActRowCheckRefetch
	case ev.Kind == "rollback":
		next.State, next.Rollback, next.StalledGen = Stalled, true, &gen
		return next, ActRefetch
	case cur.State == Stalled && ev.Kind == "adopt_epoch" && above(cur.StalledGen):
		next.State = Aligning
		return next, ActRefetch
	case cur.State == Stalled:
		return next, ActKeepRefetching
	case ev.Kind == "adopt_epoch", ev.Kind == "first_contact", ev.Kind == "beyond_retention":
		next.State = Aligning
		return next, ActRefetch
	case cur.State == Aligning && ev.Kind == "alignment_done":
		if cur.Rollback && !above(cur.StalledGen) {
			next.State = Stalled
			return next, ActAlert
		}
		next.State, next.Rollback = Aligned, false
		return next, ActWatermark
	}
	return next, ActAlert
}

// Echo is one echo the consumer applied in a partition: its position and, when it is one of this consumer's pulls
// whose record is still kept, that pull's m_p (seconds on the consumer primary's clock).
type Echo struct {
	Seq      int64
	MP       float64
	Recorded bool
}

// LastEcho is a partition's mark (P9.23): the m_p of the last recorded echo within the contiguous range.
func LastEcho(contiguous int64, echoes []Echo) (float64, bool) {
	mark, ok := 0.0, false
	for _, e := range echoes {
		if e.Recorded && e.Seq <= contiguous && (!ok || e.MP > mark) {
			mark, ok = e.MP, true
		}
	}
	return mark, ok
}

// Watermark is the minimum of the partitions' marks; none when a partition has none (P9.23).
func Watermark(marks []float64, have []bool) (float64, bool) {
	w := 0.0
	for i := range marks {
		if !have[i] {
			return 0, false
		}
		if i == 0 || marks[i] < w {
			w = marks[i]
		}
	}
	return w, len(marks) > 0
}

// ClockReset is P9.20: the recorded pulls are dropped when the clock went back or the consumer's epoch changed.
func ClockReset(now, maxSeen float64, consumerEpoch, recordedEpoch string) bool {
	return now < maxSeen || consumerEpoch != recordedEpoch
}
