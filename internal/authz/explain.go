package authz

import (
	"cmp"
	"slices"
)

// Fact kinds of an explanation (E12; openapi resource-authz.yaml fact.kind).
const (
	FactRoleKey    = "role_key"
	FactLevel      = "level"
	FactDimension  = "dimension"
	FactDelegation = "delegation"
	FactCeiling    = "ceiling"
	FactRelation   = "relation"
	FactShare      = "share"
	FactCapability = "capability"
)

// Fact is one explain fact (E12).
type Fact struct {
	Kind   string `json:"kind"`
	Source string `json:"source"`
	Detail string `json:"detail"`
}

// Explanation is E12's answer: Reasons when vis(key, row), otherwise Missing. Both are sorted by (kind,
// source, detail), deduplicated and never nil.
type Explanation struct {
	Reasons []Fact `json:"reasons"`
	Missing []Fact `json:"missing"`
}

// Explain says why key holds or not on row (E12). A dimension fact of a record that is not visible
// (vis(view_key) false) carries an empty detail (R62).
func (e *Evaluator) Explain(rt *ResourceType, key string, row Row, acl []ACLRow, graph GraphIDs) Explanation {
	ka := e.Key(rt, key, graph)
	br := e.branchesOf(rt, ka, row, acl)
	visible := br.visible()
	if key != rt.ViewKey {
		visible = e.Vis(rt, e.Key(rt, rt.ViewKey, graph), row, acl)
	}
	reveal := func(d string) string {
		if visible {
			return row.Values[d]
		}
		return ""
	}
	out := Explanation{Reasons: []Fact{}, Missing: []Fact{}}
	if br.visible() {
		out.Reasons = e.reasons(rt, ka, row, br, reveal)
	} else {
		out.Missing = e.missing(rt, ka, br, reveal)
	}
	out.Reasons, out.Missing = sortFacts(out.Reasons), sortFacts(out.Missing)
	return out
}

// reasons are E12's facts for a key that holds on the record.
func (e *Evaluator) reasons(rt *ResourceType, ka KeyAccess, row Row, br branches, reveal func(string) string) []Fact {
	var out []Fact
	f := ka.facts
	if br.rule {
		for _, h := range f.holders {
			out = append(out, Fact{FactRoleKey, h, ka.Key})
		}
		if (rt.HasDimension(DimOwner) || rt.HasDimension(DimOrg)) && len(f.holders) > 0 {
			out = append(out, Fact{FactLevel, topHolder(f), ka.Level.String()})
		}
		for _, d := range rt.ResourceDimensions() {
			out = append(out, Fact{FactDimension, d, reveal(d)})
		}
		for _, d := range f.dk {
			if d.From == row.Owner {
				out = append(out, Fact{FactDelegation, d.ID, d.From})
			}
		}
		for _, code := range e.t.Ceil {
			out = append(out, Fact{FactCeiling, code, ka.Key})
		}
	}
	for _, a := range br.acl {
		kind := FactShare
		if rt.Relations[a.Relation].OwnedBy == OwnedByComponent {
			kind = FactRelation
		}
		out = append(out, Fact{kind, a.Relation, a.Subject})
	}
	if br.graph {
		out = append(out, Fact{FactRelation, "graph", row.ID})
	}
	return out
}

// missing are E12's facts for a key that does not hold on the record.
func (e *Evaluator) missing(rt *ResourceType, ka KeyAccess, br branches, reveal func(string) string) []Fact {
	var out []Fact
	f := ka.facts
	switch {
	case !f.allowed:
		for _, code := range e.t.Ceil {
			if !ceilingsAllow(e.b, []string{code}, ka.Key) {
				out = append(out, Fact{FactCeiling, code, ka.Key})
			}
		}
	case len(f.holders) == 0 && len(f.dk) == 0:
		out = append(out, Fact{FactRoleKey, "", ka.Key})
	default:
		if !br.identity {
			out = append(out, Fact{FactLevel, "", ka.Level.String()})
		}
		for _, d := range br.failing {
			out = append(out, Fact{FactDimension, d, reveal(d)})
		}
	}
	for _, name := range rt.RelationsGiving(ka.Key) {
		if c := rt.Relations[name].capabilityOf(); !e.b.Capabilities.Has(c) {
			out = append(out, Fact{FactCapability, c, ""})
		}
	}
	if rt.Derivation == DerivationGraph && !e.b.Capabilities.Graph {
		out = append(out, Fact{FactCapability, "graph", ""})
	}
	return out
}

// topHolder is the lexicographically first holder with the highest uncapped level (E12).
func topHolder(f *keyFacts) string {
	best := f.holders[0]
	for _, h := range f.holders[1:] {
		if f.uncapped[h] > f.uncapped[best] {
			best = h
		}
	}
	return best
}

// sortFacts sorts by (kind, source, detail) and deduplicates; it never returns nil.
func sortFacts(in []Fact) []Fact {
	out := append([]Fact{}, in...)
	slices.SortFunc(out, func(a, b Fact) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Source, b.Source), cmp.Compare(a.Detail, b.Detail))
	})
	return slices.Compact(out)
}
