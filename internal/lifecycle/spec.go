package lifecycle

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// Span is a calendar length of lifecycle.yaml: N units of d (days), w (weeks), mo (months) or y
// (years) (schemas/lifecycle.schema.json $defs/duration).
type Span struct {
	N    int
	Unit string
}

// AddTo adds the span to t in UTC calendar arithmetic (time.AddDate semantics), whatever t's zone.
func (s Span) AddTo(t time.Time) time.Time {
	t = t.UTC()
	switch s.Unit {
	case "d":
		return t.AddDate(0, 0, s.N)
	case "w":
		return t.AddDate(0, 0, 7*s.N)
	case "mo":
		return t.AddDate(0, s.N, 0)
	}
	return t.AddDate(s.N, 0, 0)
}

// days and months normalise a span within its family; ok is false for the other family.
func (s Span) days() (int, bool) {
	switch s.Unit {
	case "d":
		return s.N, true
	case "w":
		return 7 * s.N, true
	}
	return 0, false
}

func (s Span) months() (int, bool) {
	switch s.Unit {
	case "mo":
		return s.N, true
	case "y":
		return 12 * s.N, true
	}
	return 0, false
}

// AtLeast reports whether s is never shorter than o, whatever the calendar (P16.9: an override may
// only lengthen). Within a family it compares exactly; across families conservatively: a month
// counts 28 days on the long side and 31 days on the short side.
func (s Span) AtLeast(o Span) bool {
	sd, sDays := s.days()
	od, oDays := o.days()
	sm, _ := s.months()
	om, _ := o.months()
	switch {
	case sDays && oDays:
		return sd >= od
	case !sDays && !oDays:
		return sm >= om
	case !sDays: // s in months, o in days
		return sm*28 >= od
	}
	return sd >= om*31 // s in days, o in months
}

func (s Span) String() string { return strconv.Itoa(s.N) + s.Unit }

// After is "<span> after <anchor>", anchor one of created, closed, sealed, fiscal_year_end.
type After struct {
	Span
	Anchor string
}

func (a After) String() string { return a.Span.String() + " after " + a.Anchor }

var (
	spanRe  = regexp.MustCompile(`^([1-9][0-9]*)(d|w|mo|y)$`)
	afterRe = regexp.MustCompile(`^([1-9][0-9]*)(d|w|mo|y) after (created|closed|sealed|fiscal_year_end)$`)
)

func parseSpan(s string) (Span, bool) {
	m := spanRe.FindStringSubmatch(s)
	if m == nil {
		return Span{}, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return Span{}, false
	}
	return Span{N: n, Unit: m[2]}, true
}

func parseAfter(s string) (After, bool) {
	m := afterRe.FindStringSubmatch(s)
	if m == nil {
		return After{}, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return After{}, false
	}
	return After{Span: Span{N: n, Unit: m[2]}, Anchor: m[3]}, true
}

// SealKind is what tiers.seal says.
type SealKind int

// The seal kinds of $defs/sealSpec; absent and `never` are both SealNone.
const (
	SealNone      SealKind = iota
	SealImmediate          // guarded from the partition's creation; SEALED once its range ended
	SealOnSignal           // sealed only by tx.Seal (a business signal, e.g. a period locked)
	SealAfter              // sealed once the anchor plus the span has passed
)

// Seal is a parsed tiers.seal.
type Seal struct {
	Kind  SealKind
	After After // SealAfter only
}

func parseSeal(s string) (Seal, error) {
	switch s {
	case "", "never":
		return Seal{Kind: SealNone}, nil
	case "immediate":
		return Seal{Kind: SealImmediate}, nil
	case "on_signal":
		return Seal{Kind: SealOnSignal}, nil
	}
	if a, ok := parseAfter(s); ok {
		return Seal{Kind: SealAfter, After: a}, nil
	}
	return Seal{}, fmt.Errorf("tiers.seal %q is not immediate, on_signal, never or <n><unit> after <anchor>", s)
}
