package authz

import (
	"slices"
	"time"
)

// Reasons of a single-record decision (P6.6, E10).
const (
	// ReasonNotFound: the record is not visible; reads and commands answer 404 (R62).
	ReasonNotFound = "NOT_FOUND"
	// ReasonOutOfScope: the caller holds the key but this record is outside its scope (403).
	ReasonOutOfScope = "OUT_OF_SCOPE"
)

// Row is a record's facts for a single-record decision (E10): its id, owner, department path and the
// value of each resource dimension.
type Row struct {
	ID       string
	Owner    string
	DeptPath string
	Values   map[string]string
}

// ACLRow is one projection row of besdk_authz_acl (P6.12, E3, E10). ExpiresAt nil never expires.
type ACLRow struct {
	RType     string
	RID       string
	Relation  string
	Subject   string
	ExpiresAt *time.Time
}

// RecordDecision is Can(key, row) (P6.6, E10). Reason is empty when Allowed.
type RecordDecision struct {
	Visible bool   `json:"visible"`
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

// branches is which of E10's three branches hold for one key on one record.
type branches struct {
	rule     bool
	identity bool     // the identity part alone (E12's level fact)
	failing  []string // resource dimensions that do not hold, sorted (E12)
	acl      []ACLRow // matching ACL rows (E12)
	graph    bool
}

func (b branches) visible() bool { return b.rule || len(b.acl) > 0 || b.graph }

// Vis is vis(key, row) of E10 for an already evaluated key.
func (e *Evaluator) Vis(rt *ResourceType, ka KeyAccess, row Row, acl []ACLRow) bool {
	return e.branchesOf(rt, ka, row, acl).visible()
}

// branchesOf evaluates the rule, ACL and graph branches (E10).
func (e *Evaluator) branchesOf(rt *ResourceType, ka KeyAccess, row Row, acl []ACLRow) branches {
	p := ka.Params
	var b branches
	b.identity = identityHolds(rt, p, row)
	for _, d := range rt.ResourceDimensions() {
		dp := p.Dims[d]
		if !dp.All && !slices.Contains(dp.IDs, row.Values[d]) {
			b.failing = append(b.failing, d)
		}
	}
	b.rule = ka.Has && b.identity && len(b.failing) == 0
	if p.ACL {
		for _, a := range acl {
			if a.RType == rt.Type && a.RID == row.ID && slices.Contains(p.Relations, a.Relation) &&
				slices.Contains(p.Subjects, a.Subject) && (a.ExpiresAt == nil || a.ExpiresAt.After(e.now)) {
				b.acl = append(b.acl, a)
			}
		}
	}
	b.graph = slices.Contains(p.GraphIDs, row.ID)
	return b
}

// identityHolds is E10's identity part.
func identityHolds(rt *ResourceType, p ScopeParams, row Row) bool {
	owner, org := rt.HasDimension(DimOwner), rt.HasDimension(DimOrg)
	switch {
	case !owner && !org, p.All:
		return true
	case owner && slices.Contains(p.Owners, row.Owner):
		return true
	case org && slices.Contains(p.DeptExact, row.DeptPath):
		return true
	case org:
		for _, pat := range p.DeptPrefix {
			if likeMatch(row.DeptPath, pat) {
				return true
			}
		}
	}
	return false
}

// Decide is the single-record decision Can(key, row) (P6.6, E10): visibility by rt.ViewKey, the action
// by key.
func (e *Evaluator) Decide(rt *ResourceType, key string, row Row, acl []ACLRow, graph GraphIDs) RecordDecision {
	view := e.Key(rt, rt.ViewKey, graph)
	if !e.Vis(rt, view, row, acl) {
		return RecordDecision{Reason: ReasonNotFound}
	}
	act := view
	if key != rt.ViewKey {
		act = e.Key(rt, key, graph)
	}
	switch {
	case e.Vis(rt, act, row, acl):
		return RecordDecision{Visible: true, Allowed: true}
	case act.Has:
		return RecordDecision{Visible: true, Reason: ReasonOutOfScope}
	default:
		return RecordDecision{Visible: true, Reason: ReasonMissingPermission}
	}
}

// FieldAccess lists the columns a caller may not read (Masked) and may read but not write (ReadOnly),
// sorted (P6.8, E11).
type FieldAccess struct {
	Masked   []string `json:"masked"`
	ReadOnly []string `json:"read_only"`
}

// IsMasked reports whether column is masked: null in responses, listed in _masked, never sorted,
// filtered or aggregated by (SORT_FORBIDDEN) (P6.8, E11).
func (f FieldAccess) IsMasked(column string) bool { return slices.Contains(f.Masked, column) }

// IsWritable reports whether a write may change column: neither masked nor read-only (P6.8, E11).
func (f FieldAccess) IsWritable(column string) bool {
	return !f.IsMasked(column) && !slices.Contains(f.ReadOnly, column)
}

// FirstUnwritable is the first of changed that may not be written (FIELD_FORBIDDEN), or "".
func (f FieldAccess) FirstUnwritable(changed []string) string {
	for _, c := range changed {
		if !f.IsWritable(c) {
			return c
		}
	}
	return ""
}

// Fields evaluates rt's field sets (E11): without has(read) a set's columns are masked; with read but
// without edit (absent or not held) they are read-only.
func (e *Evaluator) Fields(rt *ResourceType) FieldAccess {
	var masked, readOnly []string
	for _, fs := range rt.Fields {
		switch {
		case !e.HasKey(fs.Read):
			masked = append(masked, fs.Columns...)
		case fs.Edit == "" || !e.HasKey(fs.Edit):
			readOnly = append(readOnly, fs.Columns...)
		}
	}
	return FieldAccess{Masked: sortedUnique(masked), ReadOnly: sortedUnique(readOnly)}
}
