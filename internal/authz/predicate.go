package authz

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Columns names the columns the canonical predicate reads (P6.5). Alias is the list query's table
// alias. ID is required; Owner when the type declares owner, DeptPath when it declares org, and one
// entry of Dims per resource dimension. Every name is a plain SQL identifier, checked by NewPredicate:
// the predicate never quotes or concatenates anything else.
type Columns struct {
	Alias    string
	ID       string
	Owner    string
	DeptPath string
	Dims     map[string]string // resource dimension -> column
}

// Predicate renders the canonical list predicate of one resource type (P6.5, E10 "List/Can
// consistency"): static SQL text with positional placeholders, every value an argument. It is
// immutable once built and safe to share.
type Predicate struct {
	rt   *ResourceType
	c    Columns
	dims []string
}

// Fragment is one rendered SQL fragment and its arguments, in placeholder order.
type Fragment struct {
	SQL  string
	Args []any
}

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// NewPredicate checks the columns against the resource type's dimensions.
func NewPredicate(rt *ResourceType, c Columns) (*Predicate, error) {
	need := map[string]string{"alias": c.Alias, "id": c.ID}
	if rt.HasDimension(DimOwner) {
		need["owner"] = c.Owner
	}
	if rt.HasDimension(DimOrg) {
		need["dept_path"] = c.DeptPath
	}
	dims := rt.ResourceDimensions()
	for _, d := range dims {
		need["dimension "+d] = c.Dims[d]
	}
	for what, ident := range need {
		if !identPattern.MatchString(ident) {
			return nil, fmt.Errorf("authz predicate for %s: %s column %q is not a plain identifier", rt.Type, what, ident)
		}
	}
	return &Predicate{rt: rt, c: c, dims: dims}, nil
}

// SQL renders the whole predicate, a parenthesised boolean expression to AND into a list query's
// WHERE. Placeholders start at $first; now is the instant ACL expiry is compared with (E3), the same
// clock as the single-record decision.
func (p *Predicate) SQL(params ScopeParams, now time.Time, first int) (string, []any) {
	b := &builder{first: first}
	rule, acl, graph := p.rule(b, params), p.acl(b, params, now), p.graph(b, params)
	return "(" + rule + " OR " + acl + " OR " + graph + ")", b.args
}

// Branches renders the three OR branches as disjoint fragments (P6.5): the rule branch; the ACL branch
// minus the rule rows; the graph branch minus both. Their UNION ALL is exactly the rows of SQL. Each
// fragment numbers its own placeholders from $first.
func (p *Predicate) Branches(params ScopeParams, now time.Time, first int) []Fragment {
	b1 := &builder{first: first}
	f1 := Fragment{SQL: p.rule(b1, params)}
	f1.Args = b1.args

	b2 := &builder{first: first}
	rule2 := p.rule(b2, params)
	f2 := Fragment{SQL: "(" + p.acl(b2, params, now) + " AND NOT COALESCE(" + rule2 + ", false))"}
	f2.Args = b2.args

	b3 := &builder{first: first}
	rule3, acl3 := p.rule(b3, params), p.acl(b3, params, now)
	f3 := Fragment{SQL: "(" + p.graph(b3, params) + " AND NOT COALESCE(" + rule3 + ", false) AND NOT COALESCE(" + acl3 + ", false))"}
	f3.Args = b3.args
	return []Fragment{f1, f2, f3}
}

// rule is the identity part ANDed with every resource dimension (E10 rule branch). A type that
// declares neither owner nor org has an identity part that always holds, so the branch is has(K),
// which is s_owners non-empty (E6: sub when level(K) ≠ none, plus every D_K delegator).
func (p *Predicate) rule(b *builder, params ScopeParams) string {
	col := func(name string) string { return p.c.Alias + "." + name }
	owner, org := p.rt.HasDimension(DimOwner), p.rt.HasDimension(DimOrg)
	var identity string
	if !owner && !org {
		identity = "cardinality(" + b.arg(nonNil(params.Owners), "text[]") + ") > 0"
	} else {
		parts := []string{b.arg(params.All, "boolean")}
		if owner {
			parts = append(parts, col(p.c.Owner)+"::text = ANY("+b.arg(nonNil(params.Owners), "text[]")+")")
		}
		if org {
			parts = append(parts,
				col(p.c.DeptPath)+" = ANY("+b.arg(nonNil(params.DeptExact), "text[]")+")",
				col(p.c.DeptPath)+" LIKE ANY("+b.arg(nonNil(params.DeptPrefix), "text[]")+")")
		}
		identity = strings.Join(parts, " OR ")
	}
	out := []string{"(" + identity + ")"}
	for _, d := range p.dims {
		dp := params.Dims[d]
		out = append(out, "("+b.arg(dp.All, "boolean")+" OR "+col(p.c.Dims[d])+"::text = ANY("+b.arg(nonNil(dp.IDs), "text[]")+"))")
	}
	return "(" + strings.Join(out, " AND ") + ")"
}

// acl is the ACL branch over the projection (E8, E10); expiry compares with now (E3).
func (p *Predicate) acl(b *builder, params ScopeParams, now time.Time) string {
	return "(" + b.arg(params.ACL, "boolean") + " AND EXISTS (SELECT 1 FROM besdk_authz_acl besdk_acl" +
		" WHERE besdk_acl.rtype = " + b.arg(p.rt.Type, "text") +
		" AND besdk_acl.rid = " + p.c.Alias + "." + p.c.ID + "::text" +
		" AND besdk_acl.relation = ANY(" + b.arg(nonNil(params.Relations), "text[]") + ")" +
		" AND besdk_acl.subject = ANY(" + b.arg(nonNil(params.Subjects), "text[]") + ")" +
		" AND (besdk_acl.expires_at IS NULL OR besdk_acl.expires_at > " + b.arg(now.UTC(), "timestamptz") + ")))"
}

// graph is the graph branch (E9): an empty array for every other type.
func (p *Predicate) graph(b *builder, params ScopeParams) string {
	return p.c.Alias + "." + p.c.ID + "::text = ANY(" + b.arg(nonNil(params.GraphIDs), "text[]") + ")"
}

// builder numbers placeholders and collects their arguments.
type builder struct {
	first int
	args  []any
}

// arg appends v and returns its placeholder with an explicit cast, so PostgreSQL never has to infer a
// parameter type.
func (b *builder) arg(v any, cast string) string {
	b.args = append(b.args, v)
	return fmt.Sprintf("$%d::%s", b.first+len(b.args)-1, cast)
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
