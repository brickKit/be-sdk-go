package authz

import (
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
)

// Tri is a three-valued result (security 07).
type Tri int

// Values.
const (
	False Tri = iota
	True
	Unknown
)

// Fact is one fact of a record with explicit presence: a value, a known null, or not obtained.
type Fact struct {
	State    string // value, null, unobtained
	Value    string
	Currency string // amount facts
}

// Value is a present fact.
func Value(v string) Fact { return Fact{State: "value", Value: v} }

// Money is a present amount fact.
func Money(amount, currency string) Fact {
	return Fact{State: "value", Value: amount, Currency: currency}
}

// Null is a known null.
var Null = Fact{State: "null"}

// Unobtained is a fact that could not be obtained (unknown).
var Unobtained = Fact{State: "unobtained"}

// Facts are a record's facts by dimension.
type Facts map[string]Fact

func atom(a Atom, f Fact, principal string) Tri {
	if a.Kind == "all" {
		return True
	}
	switch f.State {
	case "unobtained", "":
		return Unknown
	case "null":
		return False
	}
	switch a.Kind {
	case "in":
		for _, v := range a.Values {
			if v == f.Value {
				return True
			}
		}
		return False
	case "self":
		if f.Value == principal {
			return True
		}
		return False
	case "tree":
		if f.Value == "" {
			return False // an empty path is not the root
		}
		for _, it := range a.Items {
			if f.Value == it.Path || (it.Mode == "subtree" && strings.HasPrefix(f.Value, it.Path+"/")) {
				return True
			}
		}
		return False
	case "interval":
		return interval(a, f)
	}
	return Unknown // an atom this code does not understand never allows
}

func interval(a Atom, f Fact) Tri {
	if f.Currency != a.Currency {
		return False
	}
	amount, _, err := apd.NewFromString(f.Value)
	if err != nil {
		return Unknown
	}
	if a.Lo != nil {
		lo, _, err := apd.NewFromString(*a.Lo)
		if err != nil {
			return Unknown
		}
		if c := amount.Cmp(lo); c < 0 || (c == 0 && !*a.LoClosed) {
			return False
		}
	}
	if a.Hi != nil {
		hi, _, err := apd.NewFromString(*a.Hi)
		if err != nil {
			return Unknown
		}
		if c := amount.Cmp(hi); c > 0 || (c == 0 && !*a.HiClosed) {
			return False
		}
	}
	return True
}

func segment(s Segment, facts Facts, principal string, at time.Time) Tri {
	if !s.ActiveAt(at) {
		return False
	}
	out := True
	for dim, a := range s.Atoms {
		switch atom(a, facts[dim], principal) {
		case False:
			return False
		case Unknown:
			out = Unknown
		}
	}
	return out
}

// entitled is OR over the operation's segments; fields are the union of the true segments' read fields.
func entitled(segs []Segment, op string, facts Facts, principal string, at time.Time) (Tri, map[string]bool) {
	res, fields := False, map[string]bool{}
	for _, s := range segs {
		if s.Operation != op {
			continue
		}
		switch segment(s, facts, principal, at) {
		case True:
			res = True
			for _, f := range s.ReadFields {
				fields[f] = true
			}
		case Unknown:
			if res == False {
				res = Unknown
			}
		}
	}
	return res, fields
}

// Decision is the result for one record (security 01's formulas).
type Decision struct {
	Visible Tri
	Action  Tri      // PermissionAllowed for the operation asked; equals Visible for the view operation
	Fields  []string // readable fields (never a secret one), sorted; empty unless visible
}

// Evaluate decides one record for a subject's segments: visibility by the resource's view operation, the
// operation's permission, and the readable fields.
func Evaluate(r *Resource, segs []Segment, op string, facts Facts, principal string, at time.Time) Decision {
	visible, fields := entitled(segs, r.ViewOperation, facts, principal, at)
	d := Decision{Visible: visible, Action: visible}
	if op != r.ViewOperation {
		act, _ := entitled(segs, op, facts, principal, at)
		d.Action = and(visible, act)
	}
	if visible == True {
		for f := range fields {
			if fd, ok := r.Fields[f]; ok && fd.Class != Secret {
				d.Fields = append(d.Fields, f)
			}
		}
		sort.Strings(d.Fields)
	}
	return d
}

func and(a, b Tri) Tri {
	if a == False || b == False {
		return False
	}
	if a == Unknown || b == Unknown {
		return Unknown
	}
	return True
}
