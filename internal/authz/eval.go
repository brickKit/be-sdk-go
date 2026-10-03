package authz

import (
	"slices"
	"time"
)

// GraphIDs returns the ids ListObjects gave for key (E9), or nil. The root calls the provider before it
// evaluates and only when NeedsGraph says so; the evaluator never reaches the network.
type GraphIDs func(key string) []string

// Evaluator evaluates one verified token against one bundle at one instant (EVALUATION.md E3–E12). It
// is built per request and never cached across requests (P6.14). Its methods do not run E2's token
// checks: call CheckToken first. An Evaluator is not safe for concurrent use.
type Evaluator struct {
	b    *Bundle
	t    Token
	now  time.Time
	keys map[string]*keyFacts
}

// NewEvaluator binds a bundle, a token that passed CheckToken and the decision instant.
func NewEvaluator(b *Bundle, t Token, now time.Time) *Evaluator {
	return &Evaluator{b: b, t: t, now: now, keys: map[string]*keyFacts{}}
}

// KeyAccess is everything E5–E9 derive for one key on one resource type.
type KeyAccess struct {
	Key      string
	Has      bool        // has(K), E5
	Level    Level       // level(K), E6
	Params   ScopeParams // the canonical predicate's parameters, E6–E9
	Degraded []string    // "graph" when a graph type lacks the capability (E9); never nil

	facts *keyFacts
}

// keyFacts are the type-independent facts of one key (E4–E6), kept for Explain.
type keyFacts struct {
	allowed  bool             // the ceilings allow the key (E4)
	holders  []string         // active roles holding the key, sorted (E5)
	uncapped map[string]Level // each holder's level before the ceiling cap (E6)
	dk       []Delegation     // D_K (E5)
	level    Level            // level(K), capped (E6)
	org      []string         // org values after the ceiling drops (E6)
}

// HasKey is has(key) at the evaluator's instant (E5).
func (e *Evaluator) HasKey(key string) bool { return HasKey(e.b, e.t, key, e.now) }

// NeedsGraph reports whether rt's list or decision for key needs ListObjects (E9): a graph type, a
// provider with the graph capability and a key the ceilings allow.
func (e *Evaluator) NeedsGraph(rt *ResourceType, key string) bool {
	return rt.Derivation == DerivationGraph && e.b.Capabilities.Graph && ceilingsAllow(e.b, e.t.Ceil, key)
}

// Key evaluates key on rt (E5–E9). graph may be nil when NeedsGraph is false.
func (e *Evaluator) Key(rt *ResourceType, key string, graph GraphIDs) KeyAccess {
	f := e.factsOf(key)
	ka := KeyAccess{Key: key, Has: f.allowed && (len(f.holders) > 0 || len(f.dk) > 0), Level: f.level,
		Degraded: []string{}, facts: f}
	ka.Params = e.identityParams(f)
	ka.Params.Dims = e.dimParams(rt, f)
	ka.Params.Relations = e.relations(rt, key, f)
	ka.Params.ACL = len(ka.Params.Relations) > 0
	ka.Params.Subjects = e.subjects(f)
	ka.Params.GraphIDs = []string{}
	if rt.Derivation == DerivationGraph {
		switch {
		case !e.b.Capabilities.Graph:
			ka.Degraded = []string{"graph"}
		case f.allowed && graph != nil:
			ka.Params.GraphIDs = sortedUnique(graph(key))
		}
	}
	return ka
}

// factsOf computes and memoises E4–E6 for key.
func (e *Evaluator) factsOf(key string) *keyFacts {
	if f, ok := e.keys[key]; ok {
		return f
	}
	f := &keyFacts{allowed: ceilingsAllow(e.b, e.t.Ceil, key), uncapped: map[string]Level{}, org: []string{}}
	f.dk = OnBehalf(e.b, e.t.Sub, key, e.now)
	unix := e.now.Unix()
	for _, r := range e.t.Roles {
		if roleActive(e.b, r, unix) && slices.Contains(e.b.Roles[r], key) {
			f.holders = append(f.holders, r)
		}
	}
	f.holders = sortedUnique(f.holders)
	if f.allowed && len(f.holders) > 0 {
		var top Level
		for _, r := range f.holders {
			f.uncapped[r] = e.roleLevel(r, key)
			top = max(top, f.uncapped[r])
		}
		ceiling := e.ceilingCap()
		f.level = min(top, ceiling)
		f.org = dropByCap(e.holderValues(f.holders, DimOrg), ceiling)
	}
	e.keys[key] = f
	return f
}

// roleLevel is E6: grants[r].levels[K], else default_level, else own. A name that is not a level counts
// as own, the lowest.
func (e *Evaluator) roleLevel(role, key string) Level {
	g := e.b.Grants[role]
	name, ok := g.Levels[key]
	if !ok {
		name = g.DefaultLevel
	}
	if l, ok := ParseLevel(name); ok {
		return l
	}
	return LevelOwn
}

// ceilingCap is the lowest max_level of the token's ceilings (E4, E6); all without ceilings. An unknown
// profile is an empty profile whose max_level is own.
func (e *Evaluator) ceilingCap() Level {
	lowest := LevelAll
	for _, code := range e.t.Ceil {
		l := LevelOwn
		if p, ok := e.b.Profiles[code]; ok {
			if pl, ok := ParseLevel(p.MaxLevel); ok {
				l = pl
			}
		}
		lowest = min(lowest, l)
	}
	return lowest
}

// holderValues is the union of grants[r].values[dim] over the holders (E6, E7), sorted.
func (e *Evaluator) holderValues(holders []string, dim string) []string {
	var out []string
	for _, r := range holders {
		out = append(out, e.b.Grants[r].Values[dim]...)
	}
	return sortedUnique(out)
}

// dropByCap is E6's ceiling rule for org values: paths go when the cap is below subtree, * when it is
// below all.
func dropByCap(values []string, ceiling Level) []string {
	out := []string{}
	for _, v := range values {
		if (v == "*" && ceiling >= LevelAll) || (v != "*" && ceiling >= LevelSubtree) {
			out = append(out, v)
		}
	}
	return out
}

// identityParams fills s_all, s_owners, s_dept_exact and s_dept_prefix (E6).
func (e *Evaluator) identityParams(f *keyFacts) ScopeParams {
	p := ScopeParams{Owners: []string{}, DeptExact: []string{}, DeptPrefix: []string{}}
	p.All = f.level != LevelNone && (f.level == LevelAll || slices.Contains(f.org, "*"))
	if f.level != LevelNone {
		p.Owners = append(p.Owners, e.t.Sub)
	}
	if f.allowed {
		for _, d := range f.dk {
			p.Owners = append(p.Owners, d.From)
		}
	}
	dept, ok := e.department()
	if ok && f.level == LevelDept {
		p.DeptExact = append(p.DeptExact, dept)
	}
	if ok && f.level == LevelSubtree {
		p.DeptPrefix = append(p.DeptPrefix, DeptPrefix(dept))
	}
	for _, v := range f.org {
		if v != "*" && deptPattern.MatchString(v) {
			p.DeptPrefix = append(p.DeptPrefix, DeptPrefix(v))
		}
	}
	p.Owners, p.DeptExact, p.DeptPrefix = sortedUnique(p.Owners), sortedUnique(p.DeptExact), sortedUnique(p.DeptPrefix)
	return p
}

// dimParams fills s_dims for every resource dimension of rt (E7).
func (e *Evaluator) dimParams(rt *ResourceType, f *keyFacts) map[string]DimParam {
	dims := map[string]DimParam{}
	for _, d := range rt.ResourceDimensions() {
		var values []string
		if f.allowed {
			values = e.holderValues(f.holders, d)
		}
		dp := DimParam{IDs: []string{}}
		for _, v := range values {
			if v == "*" {
				dp.All = true
			} else {
				dp.IDs = append(dp.IDs, v)
			}
		}
		dims[d] = dp
	}
	return dims
}

// relations is s_relations (E8): the relations of rt that give key, whose capability is on, inside
// every ceiling's relations; empty when the ceilings do not allow key.
func (e *Evaluator) relations(rt *ResourceType, key string, f *keyFacts) []string {
	out := []string{}
	if !f.allowed {
		return out
	}
	for _, name := range rt.RelationsGiving(key) {
		if e.b.Capabilities.Has(rt.Relations[name].capabilityOf()) && e.ceilingsAllowRelation(name) {
			out = append(out, name)
		}
	}
	return out
}

// ceilingsAllowRelation is E4/E8: every ceiling lists the relation; an unknown profile lists none.
func (e *Evaluator) ceilingsAllowRelation(name string) bool {
	for _, code := range e.t.Ceil {
		p, ok := e.b.Profiles[code]
		if !ok || !slices.Contains(p.Relations, name) {
			return false
		}
	}
	return true
}

// subjects is S(K) (P6.4, E8).
func (e *Evaluator) subjects(f *keyFacts) []string {
	out := []string{"user:" + e.t.Sub}
	unix := e.now.Unix()
	for _, r := range e.t.Roles {
		if roleActive(e.b, r, unix) {
			out = append(out, "role:"+r)
		}
	}
	if dept, ok := e.department(); ok {
		out = append(out, "dept:"+dept)
		for _, p := range DeptAncestors(dept) {
			out = append(out, "dept_tree:"+p)
		}
	}
	for _, d := range f.dk {
		out = append(out, "user:"+d.From)
	}
	return sortedUnique(out)
}

// department returns the token's department when it is valid (E6, R60).
func (e *Evaluator) department() (string, bool) {
	return e.t.DeptPath, deptPattern.MatchString(e.t.DeptPath)
}
