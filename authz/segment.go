package authz

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
)

// TreeItem is one item of an org-tree atom.
type TreeItem struct {
	Path string `json:"path"`
	Mode string `json:"mode"` // exact, subtree
}

// Atom is a segment's condition on one dimension (security 07, 09a).
type Atom struct {
	Kind     string     `json:"kind"` // all, in, tree, self, interval
	Values   []string   `json:"values,omitempty"`
	Items    []TreeItem `json:"items,omitempty"`
	Currency string     `json:"currency,omitempty"`
	Lo       *string    `json:"lo,omitempty"`
	LoClosed *bool      `json:"lo_closed,omitempty"`
	Hi       *string    `json:"hi,omitempty"`
	HiClosed *bool      `json:"hi_closed,omitempty"`
}

// Window is a validity window, left-closed, right-open; no Until means no fixed end.
type Window struct {
	From  string `json:"from"`
	Until string `json:"until,omitempty"`
}

// Segment is one complete segment: one resource type, one operation, one atom per declared dimension.
type Segment struct {
	ResourceType string          `json:"resource_type,omitempty"`
	Operation    string          `json:"operation"`
	Atoms        map[string]Atom `json:"atoms"`
	ReadFields   []string        `json:"read_fields"`
	WriteFields  []string        `json:"write_fields,omitempty"`
	Valid        Window          `json:"valid"`
	Binding      string          `json:"binding,omitempty"`
	Source       string          `json:"source,omitempty"`
	Definition   json.RawMessage `json:"definition,omitempty"`
}

// ActiveAt reports whether the segment's window contains t.
func (s Segment) ActiveAt(t time.Time) bool {
	from, err := time.Parse(time.RFC3339Nano, s.Valid.From)
	if err != nil || t.Before(from) {
		return false
	}
	if s.Valid.Until == "" {
		return true
	}
	until, err := time.Parse(time.RFC3339Nano, s.Valid.Until)
	return err == nil && t.Before(until)
}

// Check verifies the segment against its resource: exactly one atom per declared dimension, atoms well formed,
// written fields readable (security 09a, EN08-EN10).
func (s Segment) Check(r *Resource) error {
	if len(s.Atoms) != len(r.Dimensions) {
		return fmt.Errorf("segment for %s: %d atoms for %d dimensions", s.Operation, len(s.Atoms), len(r.Dimensions))
	}
	for dim, a := range s.Atoms {
		d, ok := r.Dimensions[dim]
		if !ok {
			return fmt.Errorf("segment for %s: dimension %s is not declared", s.Operation, dim)
		}
		switch a.Kind {
		case "all", "in":
		case "tree":
			if d.Kind != OrgTree {
				return fmt.Errorf("segment for %s: a tree atom on %s", s.Operation, dim)
			}
		case "self":
			if d.Kind != Principal {
				return fmt.Errorf("segment for %s: self on %s", s.Operation, dim)
			}
		case "interval":
			if d.Kind != Amount || (a.Lo == nil && a.Hi == nil) || (a.Lo != nil) != (a.LoClosed != nil) || (a.Hi != nil) != (a.HiClosed != nil) {
				return fmt.Errorf("segment for %s: interval on %s", s.Operation, dim)
			}
		default:
			return fmt.Errorf("segment for %s: atom kind %q is not understood", s.Operation, a.Kind)
		}
	}
	readable := map[string]bool{}
	for _, f := range s.ReadFields {
		readable[f] = true
	}
	for _, f := range s.WriteFields {
		if !readable[f] {
			return fmt.Errorf("segment for %s: field %s writable but not readable", s.Operation, f)
		}
	}
	return nil
}

// Canonical returns the normalized segment as the encoder's types (security 07's normalization, 09a's encoding).
func (s Segment) Canonical(resourceType string) (map[string]any, error) {
	atoms := map[string]any{}
	for dim, a := range s.Atoms {
		m := map[string]any{"kind": a.Kind}
		switch a.Kind {
		case "in":
			vals := make([]any, len(a.Values))
			for i, v := range a.Values {
				vals[i] = canon.Text(v)
			}
			set, err := canon.SortedSet(vals)
			if err != nil {
				return nil, err
			}
			m["values"] = set
		case "tree":
			items := make([]any, len(a.Items))
			for i, it := range a.Items {
				items[i] = map[string]any{"path": it.Path, "mode": it.Mode}
			}
			set, err := canon.SortedSet(items)
			if err != nil {
				return nil, err
			}
			m["items"] = set
		case "interval":
			m["currency"] = a.Currency
			for _, b := range []struct {
				name   string
				v      *string
				closed *bool
			}{{"lo", a.Lo, a.LoClosed}, {"hi", a.Hi, a.HiClosed}} {
				if b.v != nil {
					d, err := canon.Decimal(*b.v)
					if err != nil {
						return nil, err
					}
					m[b.name], m[b.name+"_closed"] = d, *b.closed
				}
			}
		}
		atoms[dim] = m
	}
	set := func(xs []string) ([]any, error) {
		in := make([]any, len(xs))
		for i, x := range xs {
			in[i] = x
		}
		return canon.SortedSet(in)
	}
	read, err := set(s.ReadFields)
	if err != nil {
		return nil, err
	}
	write, err := set(s.WriteFields)
	if err != nil {
		return nil, err
	}
	valid := map[string]any{}
	if valid["from"], err = canon.Instant(s.Valid.From); err != nil {
		return nil, err
	}
	if s.Valid.Until != "" {
		if valid["until"], err = canon.Instant(s.Valid.Until); err != nil {
			return nil, err
		}
	}
	out := map[string]any{"operation": s.Operation, "atoms": atoms, "read_fields": read, "write_fields": write, "valid": valid}
	if resourceType != "" {
		out["resource_type"] = resourceType
	}
	if s.Binding != "" {
		out["binding"] = s.Binding
	}
	if s.Source != "" {
		out["source"] = s.Source
	}
	if len(s.Definition) > 0 {
		var def map[string]any
		if err := json.Unmarshal(s.Definition, &def); err != nil {
			return nil, err
		}
		out["definition"] = def
	}
	return out, nil
}

// Digest is the segment's identity (security 09a: the digest of its normalized form).
func (s Segment) Digest() (string, error) {
	c, err := s.Canonical(s.ResourceType)
	if err != nil {
		return "", err
	}
	return canon.Digest("be.security.segment", 1, c)
}
