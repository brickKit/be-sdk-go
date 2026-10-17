// Package authz is the component side of authorization (protocol P8.7-P8.18): the resource, operation and field
// declarations, the bounded local view held and verified against the authorization member, three-valued
// evaluation of complete segments on one record, and the compilation of a subject's segments into a SQL predicate
// with a per-row hit bitmap for field masks.
package authz

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/canon"
)

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

// Dimension declares one authorization dimension of a resource and where its fact lives.
type Dimension struct {
	Kind           string
	Column         string // the column on the owner's table
	PrincipalRole  string // principal dimensions: which party (owner, creator)
	CurrencyColumn string // amount dimensions
}

// Field declares one output field.
type Field struct {
	Class    string
	ReadKey  string // restricted fields
	WriteKey string
}

// Resource declares a resource type the component owns.
type Resource struct {
	Type          string
	ViewOperation string
	Dimensions    map[string]Dimension // all of them; none is an explicit empty map
	Fields        map[string]Field     // every field that may be output
}

// Operation declares one operation (security 09a's operation definition).
type Operation struct {
	Key          string
	ResourceType string
	Purpose      string // read, special_read, export, action, create, manage
	RiskClass    string // low, high
	SideEffects  []string
	Audit        bool // audit: required
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
			}
		}
		for name, f := range r.Fields {
			if f.Class != Ordinary && f.Class != Restricted && f.Class != Secret {
				problems = append(problems, fmt.Sprintf("%s.%s: field class %q (every field is classified)", r.Type, name, f.Class))
			}
			if f.Class == Restricted && f.ReadKey == "" {
				problems = append(problems, fmt.Sprintf("%s.%s: a restricted field needs its read key", r.Type, name))
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
		m := map[string]any{"kind": d.Kind, "column": d.Column, "pushdown": "local_column"}
		if d.PrincipalRole != "" {
			m["principal_role"] = d.PrincipalRole
		}
		if d.CurrencyColumn != "" {
			m["currency_column"] = d.CurrencyColumn
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
		fields[name] = m
	}
	return map[string]any{"type": r.Type, "view_operation": r.ViewOperation, "dimensions": dims, "fields": fields}
}

// Digest is the resource definition's digest (P8.8).
func (r Resource) Digest() (string, error) {
	return canon.Digest("be.security.resource_definition", 1, r.Record())
}
