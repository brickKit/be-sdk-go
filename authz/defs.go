// Package authz is the component side of authorization (protocol P8.7-P8.18): the resource, operation and field
// declarations, the bounded local view held and verified against the authorization member, three-valued
// evaluation of complete segments on one record, and the compilation of a subject's segments into a SQL predicate
// with a per-row hit bitmap for field masks.
package authz

import (
	"cmp"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/canon"
)

// keySet is a Set<Key> in canonical order (ASCII keys: byte order).
func keySet(keys []string) []any {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	out := make([]any, len(sorted))
	for i, k := range sorted {
		out[i] = k
	}
	return out
}

// Dimension kinds (security 07).
const (
	ValueSet  = "value_set"
	OrgTree   = "org_tree"
	Principal = "principal"
	Amount    = "amount"
)

// Field classes.
const (
	Ordinary   = "ordinary"
	Restricted = "restricted"
	Secret     = "secret"
)

// Pushdown modes of a dimension (security 09a).
const (
	LocalColumn     = "local_column"
	LocalProjection = "local_projection"
	NotPushdownable = "not_pushdownable"
)

// Dimension declares one authorization dimension of a resource and where its fact lives.
type Dimension struct {
	Kind           string
	Column         string // the column on the owner's table
	PrincipalRole  string // principal dimensions: which party (owner, creator)
	CurrencyColumn string // amount dimensions
	// Pushdown is where the query compiler finds the fact; empty is LocalColumn.
	Pushdown string
	// ProjectionWatermark is the revision domain of the projection's completeness (LocalProjection).
	ProjectionWatermark string
}

// Field declares one output field.
type Field struct {
	Class       string
	ReadKey     string // restricted fields
	WriteKey    string
	DerivedFrom []string // reading it needs reading all of these
	Queryable   []string // filter, sort, group, aggregate; none by default
}

// Resource declares a resource type the component owns.
type Resource struct {
	Type          string
	ViewOperation string
	Dimensions    map[string]Dimension // all of them; none is an explicit empty map
	Fields        map[string]Field     // every field that may be output
}

// Operation declares one operation (security 09a's operation definition). The zero values of the flags are the
// safe ones: not delegable, never under impersonation, no appointment needed, the default path.
type Operation struct {
	Key          string
	ResourceType string
	Purpose      string // read, special_read, export, action, create, manage
	RiskClass    string // low, high
	SideEffects  []string
	Audit        bool // audit: required
	// MinimumPath is default, strict_acceptance or strict_barrier; empty is default.
	MinimumPath          string
	RequiresAppointment  bool
	Delegable            bool
	ImpersonationAllowed bool // only operations without side effects
	TighteningOnly       bool // only manage operations whose only side effect is security_change
	OutputReadOperation  string
	SelfReviewRule       string
}

// Definitions are everything a component declares.
type Definitions struct {
	Component  string
	Resources  []Resource
	Operations []Operation

	resources  map[string]*Resource
	operations map[string]*Operation
}

var keyRe = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`)

// The closed value lists of security 09a's definitions.
var (
	purposes    = []string{"read", "special_read", "export", "action", "create", "manage"}
	paths       = []string{"default", "strict_acceptance", "strict_barrier"}
	sideEffects = []string{"business_write", "external_command", "job_or_subscription", "file_capability", "security_change", "notification"}
	queryUses   = []string{"filter", "sort", "group", "aggregate"}
)

// Validate checks the declarations (P8.7) and indexes them.
func (d *Definitions) Validate() error {
	var problems []string
	d.resources, d.operations = map[string]*Resource{}, map[string]*Operation{}
	for i := range d.Resources {
		r := &d.Resources[i]
		if !keyRe.MatchString(r.Type) {
			problems = append(problems, fmt.Sprintf("resource %q: not a registry key", r.Type))
		}
		if r.Dimensions == nil {
			problems = append(problems, r.Type+": dimensions must be declared (an empty map when there are none)")
		}
		for name, dim := range r.Dimensions {
			switch {
			case dim.Kind != ValueSet && dim.Kind != OrgTree && dim.Kind != Principal && dim.Kind != Amount:
				problems = append(problems, fmt.Sprintf("%s.%s: dimension kind %q", r.Type, name, dim.Kind))
			case dim.Column == "":
				problems = append(problems, fmt.Sprintf("%s.%s: no column", r.Type, name))
			case dim.Kind == Amount && dim.CurrencyColumn == "":
				problems = append(problems, fmt.Sprintf("%s.%s: an amount needs its currency column", r.Type, name))
			case dim.Pushdown != "" && dim.Pushdown != LocalColumn && dim.Pushdown != LocalProjection && dim.Pushdown != NotPushdownable:
				problems = append(problems, fmt.Sprintf("%s.%s: pushdown %q", r.Type, name, dim.Pushdown))
			case dim.Pushdown == LocalProjection && dim.ProjectionWatermark == "":
				problems = append(problems, fmt.Sprintf("%s.%s: a local projection needs its watermark domain", r.Type, name))
			}
		}
		for name, f := range r.Fields {
			if f.Class != Ordinary && f.Class != Restricted && f.Class != Secret {
				problems = append(problems, fmt.Sprintf("%s.%s: field class %q (every field is classified)", r.Type, name, f.Class))
			}
			if f.Class == Restricted && f.ReadKey == "" {
				problems = append(problems, fmt.Sprintf("%s.%s: a restricted field needs its read key", r.Type, name))
			}
			for _, q := range f.Queryable {
				if !slices.Contains(queryUses, q) {
					problems = append(problems, fmt.Sprintf("%s.%s: query use %q", r.Type, name, q))
				}
			}
		}
		d.resources[r.Type] = r
	}
	for i := range d.Operations {
		o := &d.Operations[i]
		if !keyRe.MatchString(o.Key) {
			problems = append(problems, fmt.Sprintf("operation %q: not a registry key", o.Key))
		}
		if _, ok := d.resources[o.ResourceType]; !ok {
			problems = append(problems, o.Key+": resource type "+o.ResourceType+" is not declared")
		}
		if o.RiskClass != "low" && o.RiskClass != "high" {
			problems = append(problems, o.Key+": risk class must be low or high (never assumed low)")
		}
		if !slices.Contains(purposes, o.Purpose) {
			problems = append(problems, fmt.Sprintf("%s: purpose %q", o.Key, o.Purpose))
		}
		if o.MinimumPath != "" && !slices.Contains(paths, o.MinimumPath) {
			problems = append(problems, fmt.Sprintf("%s: minimum path %q", o.Key, o.MinimumPath))
		}
		for _, e := range o.SideEffects {
			if !slices.Contains(sideEffects, e) {
				problems = append(problems, fmt.Sprintf("%s: side effect %q", o.Key, e))
			}
		}
		if o.ImpersonationAllowed && len(o.SideEffects) > 0 {
			problems = append(problems, o.Key+": an operation with side effects is never allowed under impersonation")
		}
		if o.TighteningOnly && (o.Purpose != "manage" || !slices.Equal(o.SideEffects, []string{"security_change"})) {
			problems = append(problems, o.Key+": only a manage operation whose only side effect is security_change can be tightening only")
		}
		if _, dup := d.operations[o.Key]; dup {
			problems = append(problems, o.Key+": declared twice")
		}
		d.operations[o.Key] = o
	}
	for _, r := range d.Resources {
		if o, ok := d.operations[r.ViewOperation]; !ok || o.ResourceType != r.Type {
			problems = append(problems, r.Type+": its view operation "+r.ViewOperation+" is not declared for it")
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return errors.New("authorization declarations: " + strings.Join(problems, "; "))
	}
	return nil
}

// Operation returns a declared operation.
func (d *Definitions) Operation(key string) (*Operation, bool) {
	o, ok := d.operations[key]
	return o, ok
}

// Resource returns a declared resource.
func (d *Definitions) Resource(typ string) (*Resource, bool) { r, ok := d.resources[typ]; return r, ok }

// Record renders a resource definition as its security record body (security 09a), for the digest and submission.
func (r Resource) Record() map[string]any {
	dims := map[string]any{}
	for name, d := range r.Dimensions {
		m := map[string]any{"kind": d.Kind, "column": d.Column, "pushdown": cmp.Or(d.Pushdown, LocalColumn)}
		if d.PrincipalRole != "" {
			m["principal_role"] = d.PrincipalRole
		}
		if d.CurrencyColumn != "" {
			m["currency_column"] = d.CurrencyColumn
		}
		if d.ProjectionWatermark != "" {
			m["projection_watermark"] = d.ProjectionWatermark
		}
		dims[name] = m
	}
	fields := map[string]any{}
	for name, f := range r.Fields {
		m := map[string]any{"class": f.Class}
		if f.ReadKey != "" {
			m["read_key"] = f.ReadKey
		}
		if f.WriteKey != "" {
			m["write_key"] = f.WriteKey
		}
		if len(f.DerivedFrom) > 0 {
			m["derived_from"] = keySet(f.DerivedFrom)
		}
		if len(f.Queryable) > 0 {
			m["queryable"] = keySet(f.Queryable)
		}
		fields[name] = m
	}
	return map[string]any{"type": r.Type, "view_operation": r.ViewOperation, "dimensions": dims, "fields": fields}
}

// Digest is the resource definition's digest (P8.8).
func (r Resource) Digest() (string, error) {
	return canon.Digest("be.security.resource_definition", 1, r.Record())
}

// Record renders an operation definition as its security record body (security 09a).
func (o Operation) Record() map[string]any {
	audit := "none"
	if o.Audit {
		audit = "required"
	}
	m := map[string]any{"key": o.Key, "resource_type": o.ResourceType, "purpose": o.Purpose, "risk_class": o.RiskClass,
		"minimum_path": cmp.Or(o.MinimumPath, "default"), "side_effects": keySet(o.SideEffects),
		"requires_appointment": o.RequiresAppointment, "delegable": o.Delegable,
		"impersonation_allowed": o.ImpersonationAllowed, "audit": audit}
	if o.TighteningOnly {
		m["tightening_only"] = true
	}
	if o.OutputReadOperation != "" {
		m["output_read_operation"] = o.OutputReadOperation
	}
	if o.SelfReviewRule != "" {
		m["self_review_rule"] = o.SelfReviewRule
	}
	return m
}
