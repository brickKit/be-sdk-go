// Package tiering is hot and cold data (be-protocol chapter 15; the assembly project's protocol 11): the owner's unit
// catalog, the classes a component declares, candidates and sealing behind a write barrier, the transfer to a route
// (the warm rows in the same database, or Parquet with a manifest on the object store) under the claim's generation
// and epoch, the release of the online rows, reads by catalog snapshot, holds, destruction and erasure.
package tiering

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Categories (P15.2): only completed history and audit evidence are tiered.
const (
	CategoryOnlineMutable = "online_mutable"
	CategoryEvidence      = "long_term_evidence"
	CategoryCompleted     = "completed_history"
	CategoryAudit         = "audit_evidence"
	CategoryRebuildable   = "rebuildable"
)

// Unit states (P15.7).
const (
	StateOpen      = "open"
	StateSealed    = "sealed"
	StatePublished = "published"
	StateReleased  = "released"
	StateDestroyed = "destroyed"
)

// Routes (A150).
const (
	RouteOnline   = "online"
	RoutePostgres = "postgres"
	RouteObject   = "object-parquet"
)

// Table is a class's table: its name, its single-column primary key, and its columns' types (P15.8).
type Table struct {
	Name    string
	PK      string
	Columns map[string]string
}

// Child is a table whose rows belong to a root: FK is its column holding its parent's primary key, the parent being
// the root (Parent empty) or another child (its name): an aggregate may have several levels (P15.1).
type Child struct {
	Table
	FK     string
	Parent string
}

// Retention is a class's retention (P15.4): the shortest and longest, in days from the unit's latest anchor, and
// the basis; Pending when no law or contract gives one (never destroyed).
type Retention struct {
	MinDays, MaxDays int
	Basis            string
	Pending          bool
}

// Class is a data class a component declares (P15.1).
type Class struct {
	Name     string
	Category string
	Root     Table
	Children []Child
	// Group are the root's columns a unit groups by; with the anchor's UTC month (P15.6).
	Group []string
	// Anchor is the root's timestamptz column the residence and the retention count from; Created its creation.
	Anchor, Created string
	// Terminal is a SQL boolean expression over the root row (alias r) that holds for an aggregate in its final state.
	Terminal      string
	ResidenceDays int // Never: stays online
	Retention     Retention
	// Locate are the columns reads need to find and authorize a record (every authorization dimension).
	Locate   []string
	Erasable []string
	Routes   []string
	// SchemaVersion is the version of the class's tables' structure, written in each manifest (P15.33).
	SchemaVersion string
	// HoldOperation and ApproveOperation are the component's operations that set or lift a hold and give or withdraw
	// a destruction approval (P15.28, P15.29): the hold one when the class can be destroyed or erased, the approval
	// one when it can be destroyed (its retention is not pending).
	HoldOperation, ApproveOperation string
}

var plainIdent = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Tierable says whether the class may leave the online tables.
func (c Class) Tierable() bool { return c.Category == CategoryCompleted || c.Category == CategoryAudit }

// parent is a child's parent table: the root, or the child it names.
func (c Class) parent(ch Child) (Table, bool) {
	if ch.Parent == "" || ch.Parent == c.Root.Name {
		return c.Root, true
	}
	for _, o := range c.Children {
		if o.Name == ch.Parent && o.Name != ch.Name {
			return o.Table, true
		}
	}
	return Table{}, false
}

// child is a child by its table's name.
func (c Class) child(name string) (Child, bool) {
	for _, ch := range c.Children {
		if ch.Name == name {
			return ch, true
		}
	}
	return Child{}, false
}

// depth is a child's level: 1 under the root; -1 for a parent chain that does not end at the root.
func (c Class) depth(ch Child) int {
	for d := 1; d <= len(c.Children); d++ {
		if ch.Parent == "" || ch.Parent == c.Root.Name {
			return d
		}
		next, ok := c.child(ch.Parent)
		if !ok {
			return -1
		}
		ch = next
	}
	return -1
}

// byDepth are the children, the shallowest first (reads and copies) or the deepest first (deletes).
func (c Class) byDepth(deepestFirst bool) []Child {
	out := append([]Child(nil), c.Children...)
	sort.SliceStable(out, func(i, j int) bool {
		if deepestFirst {
			return c.depth(out[i]) > c.depth(out[j])
		}
		return c.depth(out[i]) < c.depth(out[j])
	})
	return out
}

// rootExpr is the SQL expression (text) of the root key of a child's row under alias: its key column under the root,
// a lookup up the parent chain below it.
func (c Class) rootExpr(ch Child, alias string) string {
	if ch.Parent == "" || ch.Parent == c.Root.Name {
		return alias + "." + ident(ch.FK) + "::text"
	}
	parent, _ := c.child(ch.Parent)
	p := alias + "p"
	return fmt.Sprintf("(SELECT %s FROM %s %s WHERE %s.%s = %s.%s)", c.rootExpr(parent, p), ident(parent.Name), p, p,
		ident(parent.PK), alias, ident(ch.FK))
}

// Tables are the root and the children.
func (c Class) Tables() []Table {
	out := []Table{c.Root}
	for _, ch := range c.Children {
		out = append(out, ch.Table)
	}
	return out
}

// Check checks a declaration against the protocol's rules that need no database (P15.1-P15.5); unique registries are
// the tables the component names as such. n is the list's default range in days.
func (c Class) Check(n int, registries []string) error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(c.Name+": "+format, args...)) }
	if c.Name == "" || strings.ContainsAny(c.Name, " /") {
		bad("a class needs a name without spaces or slashes")
	}
	if !slices.Contains([]string{CategoryOnlineMutable, CategoryEvidence, CategoryCompleted, CategoryAudit,
		CategoryRebuildable}, c.Category) {
		bad("unknown category %q", c.Category)
	}
	if !c.Tierable() && (c.ResidenceDays != 0 || len(c.Routes) > 0) {
		bad("category %s is never tiered: no residence or route (P15.2)", c.Category)
	}
	if c.Tierable() {
		if c.ResidenceDays != Never && c.ResidenceDays < n {
			bad("residence %d days is below N = %d (P15.3)", c.ResidenceDays, n)
		}
		if len(c.Routes) == 0 {
			bad("a tierable class names its routes")
		}
		for _, r := range c.Routes {
			if r != RoutePostgres && r != RouteObject {
				bad("unknown route %q", r)
			}
		}
		if c.Terminal == "" {
			bad("a tierable class needs its terminal condition")
		}
	}
	if !c.Retention.Pending && (c.Retention.MinDays <= 0 || c.Retention.MaxDays < c.Retention.MinDays || c.Retention.Basis == "") {
		bad("a retention needs its shortest and longest days and its basis, or pending (P15.4)")
	}
	if c.SchemaVersion == "" {
		bad("a class needs its schema version")
	}
	if c.HoldOperation == "" && (!c.Retention.Pending || len(c.Erasable) > 0) {
		bad("a class that can be destroyed or erased declares its hold operation (P15.28)")
	}
	if c.ApproveOperation == "" && !c.Retention.Pending {
		bad("a class that can be destroyed declares its destruction-approval operation (P15.29)")
	}
	for _, t := range c.Tables() {
		if slices.Contains(registries, t.Name) {
			bad("%s is a unique registry: never a class's table (P15.5)", t.Name)
		}
		if !plainIdent.MatchString(t.Name) || !plainIdent.MatchString(t.PK) {
			bad("table %q or its key is not a plain identifier", t.Name)
		}
		if _, ok := t.Columns[t.PK]; !ok {
			bad("table %s: its key %s has no type", t.Name, t.PK)
		}
		for col, typ := range t.Columns {
			if !plainIdent.MatchString(col) || !slices.Contains(Types, typ) {
				bad("table %s: column %q of type %q", t.Name, col, typ)
			}
		}
	}
	for _, ch := range c.Children {
		if _, ok := ch.Columns[ch.FK]; !ok {
			bad("child %s: its key to its parent %s has no type", ch.Name, ch.FK)
		}
		if _, ok := c.parent(ch); !ok {
			bad("child %s: its parent %q is not the root or another child", ch.Name, ch.Parent)
		} else if c.depth(ch) < 0 {
			bad("child %s: its parents go round in a cycle", ch.Name)
		}
	}
	root := c.Root.Columns
	for _, col := range append(append([]string{c.Anchor, c.Created}, c.Group...), c.Locate...) {
		if _, ok := root[col]; !ok {
			bad("root %s has no column %q", c.Root.Name, col)
		}
	}
	if root[c.Anchor] != TypeTimestamptz || root[c.Created] != TypeTimestamptz {
		bad("the anchor and the creation are timestamptz columns")
	}
	for _, col := range c.Erasable {
		found := false
		for _, t := range c.Tables() {
			if _, ok := t.Columns[col]; ok {
				found = true
			}
		}
		if !found {
			bad("erasable column %q is in no table", col)
		}
	}
	return errors.Join(errs...)
}
