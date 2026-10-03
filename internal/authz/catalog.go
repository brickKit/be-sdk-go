package authz

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
)

// ErrCatalogInvalid marks a resource catalogue or resource type that cannot be used (malformed JSON, a
// missing required member, an unknown derivation or a relation cycle). It is a startup error: the
// component's own declaration is wrong.
var ErrCatalogInvalid = errors.New("authz: resource catalogue invalid")

// Derivations of a resource type (catalog.schema.json resource_type.derivation; E9).
const (
	DerivationDirect = "direct"
	DerivationGraph  = "graph"
)

// Identity dimensions are evaluated against the token, not against a column value (E6, E7).
const (
	DimOwner = "owner"
	DimOrg   = "org"
)

// Relation owners (catalog.schema.json relation.owned_by; E8).
const (
	OwnedByAuthz     = "authz"
	OwnedByComponent = "component"
)

var resourceTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z0-9_-]+){2,}$`)

// ResourceType is one entry of the catalogue's resource_types (contract-infra-authz
// schemas/catalog.schema.json): the input T of EVALUATION.md E6–E12. Parse it with ParseResourceType or
// ParseCatalog; a parsed value is immutable and safe to share.
type ResourceType struct {
	Type           string              `json:"type"`
	OwnerComponent string              `json:"owner_component"`
	ViewKey        string              `json:"view_key"`
	Keys           []string            `json:"keys,omitempty"`
	Dimensions     []string            `json:"dimensions,omitempty"`
	Relations      map[string]Relation `json:"relations"`
	Share          *ShareSpec          `json:"share,omitempty"`
	Fields         []FieldSet          `json:"fields,omitempty"`
	Inherits       []Inherit           `json:"inherits,omitempty"`
	Derivation     string              `json:"derivation"`

	// gives[k] = the relations that give key k, through grants and includes, transitively (E8).
	gives map[string][]string
}

// Relation is one relation of a resource type (E8). OwnedBy "" means authz (a share).
type Relation struct {
	Includes []string `json:"includes,omitempty"`
	Grants   []string `json:"grants,omitempty"`
	OwnedBy  string   `json:"owned_by,omitempty"`
}

// ShareSpec says who may share a record and with which relations and subject kinds (P6.10).
type ShareSpec struct {
	Key       string   `json:"key"`
	Relations []string `json:"relations"`
	Subjects  []string `json:"subjects"`
}

// FieldSet is a set of columns guarded by a read key and an optional edit key (P6.8, E11).
type FieldSet struct {
	Set     string   `json:"set"`
	Columns []string `json:"columns"`
	Read    string   `json:"read"`
	Edit    string   `json:"edit,omitempty"`
}

// Inherit is a one-hop derivation from a parent type (catalog.schema.json inherits); the projection
// pulls the parent type's tuples too (P6.12).
type Inherit struct {
	From     string `json:"from"`
	Via      string `json:"via"`
	Relation string `json:"relation"`
	As       string `json:"as"`
}

// HasDimension reports whether T declares dimension d (owner, org or a resource dimension).
func (rt *ResourceType) HasDimension(d string) bool { return slices.Contains(rt.Dimensions, d) }

// ResourceDimensions are T's dimensions other than owner and org, sorted (E7).
func (rt *ResourceType) ResourceDimensions() []string {
	out := make([]string, 0, len(rt.Dimensions))
	for _, d := range rt.Dimensions {
		if d != DimOwner && d != DimOrg {
			out = append(out, d)
		}
	}
	return sortedUnique(out)
}

// RelationsGiving are the relations of T that give key k through grants or includes, sorted (E8).
func (rt *ResourceType) RelationsGiving(k string) []string { return rt.gives[k] }

// capabilityOf names the capability a relation needs (E8): relation_sync for a component-owned relation,
// sharing otherwise.
func (r Relation) capabilityOf() string {
	if r.OwnedBy == OwnedByComponent {
		return "relation_sync"
	}
	return "sharing"
}

// ParseResourceType reads one resource_type object (catalog.schema.json) and checks it.
func ParseResourceType(raw []byte) (*ResourceType, error) {
	var rt ResourceType
	if err := json.Unmarshal(raw, &rt); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCatalogInvalid, err)
	}
	if err := rt.prepare(); err != nil {
		return nil, err
	}
	return &rt, nil
}

// prepare validates the required members and computes the grants closure.
func (rt *ResourceType) prepare() error {
	switch {
	case !resourceTypePattern.MatchString(rt.Type):
		return fmt.Errorf("%w: type %q is not <domain>.<name>.<aggregate>", ErrCatalogInvalid, rt.Type)
	case rt.ViewKey == "":
		return fmt.Errorf("%w: %s has no view_key", ErrCatalogInvalid, rt.Type)
	case rt.Derivation != DerivationDirect && rt.Derivation != DerivationGraph:
		return fmt.Errorf("%w: %s derivation %q is neither direct nor graph", ErrCatalogInvalid, rt.Type, rt.Derivation)
	}
	for name, r := range rt.Relations {
		if r.OwnedBy != "" && r.OwnedBy != OwnedByAuthz && r.OwnedBy != OwnedByComponent {
			return fmt.Errorf("%w: %s relation %s owned_by %q", ErrCatalogInvalid, rt.Type, name, r.OwnedBy)
		}
		for _, inc := range r.Includes {
			if _, ok := rt.Relations[inc]; !ok {
				return fmt.Errorf("%w: %s relation %s includes unknown relation %s", ErrCatalogInvalid, rt.Type, name, inc)
			}
		}
	}
	rt.gives = map[string][]string{}
	for name := range rt.Relations {
		for k := range rt.grantsOf(name) {
			rt.gives[k] = append(rt.gives[k], name)
		}
	}
	for k, rels := range rt.gives {
		rt.gives[k] = sortedUnique(rels)
	}
	return nil
}

// grantsOf is the transitive closure of a relation's grants over includes (E8); cycles terminate.
func (rt *ResourceType) grantsOf(name string) map[string]bool {
	keys := map[string]bool{}
	seen := map[string]bool{}
	stack := []string{name}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[n] {
			continue
		}
		seen[n] = true
		r := rt.Relations[n]
		for _, k := range r.Grants {
			keys[k] = true
		}
		stack = append(stack, r.Includes...)
	}
	return keys
}

// Catalog is the component's resource catalogue: the resource_types member of the catalogue JSON
// (catalog.schema.json; P6.10, P6.12). Other members (keys, dimensions) are ignored here.
type Catalog struct {
	types map[string]*ResourceType
	names []string
}

// ParseCatalog reads a catalogue JSON object and its resource_types. A missing resource_types member
// is an empty catalogue; a duplicate type is an error.
func ParseCatalog(raw []byte) (*Catalog, error) {
	var doc struct {
		ResourceTypes []json.RawMessage `json:"resource_types"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCatalogInvalid, err)
	}
	c := &Catalog{types: map[string]*ResourceType{}}
	for _, r := range doc.ResourceTypes {
		rt, err := ParseResourceType(r)
		if err != nil {
			return nil, err
		}
		if _, dup := c.types[rt.Type]; dup {
			return nil, fmt.Errorf("%w: resource type %s declared twice", ErrCatalogInvalid, rt.Type)
		}
		c.types[rt.Type] = rt
		c.names = append(c.names, rt.Type)
	}
	sort.Strings(c.names)
	return c, nil
}

// Lookup returns the resource type called name.
func (c *Catalog) Lookup(name string) (*ResourceType, bool) {
	rt, ok := c.types[name]
	return rt, ok
}

// Types are the catalogue's resource type names, sorted.
func (c *Catalog) Types() []string { return slices.Clone(c.names) }

// PulledTypes is the type set the ACL projection pulls (P6.12): the catalogue's own types and every
// type they inherit from, sorted and deduplicated.
func (c *Catalog) PulledTypes() []string {
	out := slices.Clone(c.names)
	for _, n := range c.names {
		for _, inh := range c.types[n].Inherits {
			out = append(out, inh.From)
		}
	}
	return sortedUnique(out)
}

// sortedUnique sorts by byte order and removes duplicates; it never returns nil (EVALUATION.md
// "Ordering").
func sortedUnique(in []string) []string {
	out := slices.Clone(in)
	if out == nil {
		out = []string{}
	}
	sort.Strings(out)
	return slices.Compact(out)
}
