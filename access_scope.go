package besdk

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/authz/acl"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
)

// Ref names one record of a resource type.
type Ref struct {
	Type ResourceType
	ID   string
}

// Attrs are a record's facts for authorization (E10): its owner (owner dimension), its department
// path (org dimension) and the value of each resource dimension.
type Attrs struct {
	Owner, DeptPath string
	Values          map[string]string
}

// Resource is a record as authorization sees it; a component's row type implements it.
type Resource interface {
	AuthzRef() Ref
	AuthzAttrs() Attrs
}

// Decision is a single-record decision (P6.6, E10).
type Decision struct {
	Visible, Allowed bool
	Reason           string // "" when allowed; NOT_FOUND, OUT_OF_SCOPE or MISSING_PERMISSION
}

// Err is the answer for a decision that does not allow: 404 NOT_FOUND for an invisible record, reads
// and commands alike (R62, A3), 403 OUT_OF_SCOPE or MISSING_PERMISSION otherwise; nil when allowed.
func (d Decision) Err() error {
	switch {
	case d.Allowed:
		return nil
	case !d.Visible:
		return problem.Be("NOT_FOUND", nil)
	}
	return problem.Be(d.Reason, nil)
}

// ScopeParams are the canonical predicate's parameters (P6.5): s_all, s_owners, s_dept_exact,
// s_dept_prefix, s_<dim>_all / _ids, s_acl, s_relations, s_subjects, s_graph_ids.
type ScopeParams = authz.ScopeParams

// Columns names the columns of a list query the canonical predicate reads (P6.5).
type Columns = authz.Columns

// Scope is the caller's data scope for one resource type and the route's key (P6.3–P6.5).
type Scope struct {
	rt       *authz.ResourceType
	params   ScopeParams
	degraded []string
	now      func() time.Time
}

// Params are the canonical predicate's parameters, one field per @s_* name.
func (s Scope) Params() ScopeParams { return s.params }

// Branches are the predicate's three OR branches separately, for a UNION (not UNION ALL) rewrite.
func (s Scope) Branches() []ScopeParams { return s.params.Branches() }

// Degraded lists what the scope could not evaluate ("graph": answer X-Authz-Degraded, P6.15).
func (s Scope) Degraded() []string { return s.degraded }

// Where renders the canonical predicate for a list query (P6.5): static SQL with positional
// parameters starting at $first and their arguments; nothing is concatenated from input. An unknown
// resource type renders a predicate that matches nothing.
func (s Scope) Where(c Columns, first int) (string, []any) {
	if s.rt == nil {
		return "FALSE", nil
	}
	p, err := authz.NewPredicate(s.rt, c)
	if err != nil {
		panic(fmt.Sprintf("besdk: Scope.Where: %v", err)) // a programming error of the component
	}
	return p.SQL(s.params, s.now(), first)
}

// evaluator is the request's evaluator, built on first use (P6.14: never cached across requests).
func (a *Access) evaluator() *authz.Evaluator {
	if a.eval == nil {
		a.eval = authz.NewEvaluator(a.bundle, a.token, a.now())
	}
	return a.eval
}

func (a *Access) resourceType(t ResourceType) (*authz.ResourceType, bool) {
	if a.rt == nil || a.rt.authzCatalog == nil || a.bundle == nil {
		return nil, false
	}
	return a.rt.authzCatalog.Lookup(string(t))
}

// Scope evaluates the route's key on t (P6.3–P6.5, E6–E9). A graph type without the provider's graph
// capability, or before this runtime calls ListObjects, is degraded: the graph branch is empty.
func (a *Access) Scope(t ResourceType) Scope {
	rt, ok := a.resourceType(t)
	if !ok {
		return Scope{now: a.now}
	}
	ka := a.evaluator().Key(rt, string(a.key), nil)
	deg := ka.Degraded
	if a.evaluator().NeedsGraph(rt, string(a.key)) {
		deg = append(deg, "graph")
	}
	return Scope{rt: rt, params: ka.Params, degraded: deg, now: a.now}
}

// Can decides key k on one record (P6.6, E10): visibility by the type's view key, the action by k.
// The record's ACL rows are read in tx, or in a short read-only transaction of its own when tx is nil
// (so call it with the command's transaction once one is open).
func (a *Access) Can(ctx context.Context, tx *Tx, k PermKey, r Resource) (Decision, error) {
	ref := r.AuthzRef()
	rt, ok := a.resourceType(ref.Type)
	if !ok {
		return Decision{Reason: authz.ReasonNotFound}, nil
	}
	rows, err := a.loadACL(ctx, tx, string(ref.Type), []string{ref.ID})
	if err != nil {
		return Decision{}, err
	}
	d := a.evaluator().Decide(rt, string(k), rowOf(r), rows[ref.ID], nil)
	return Decision{Visible: d.Visible, Allowed: d.Allowed, Reason: d.Reason}, nil
}

// RowActions decides each of ks on each record (P6.9): id → key → allowed, for the row buttons.
func (a *Access) RowActions(ctx context.Context, tx *Tx, t ResourceType, rows []Resource, ks ...PermKey) (map[string]map[string]bool, error) {
	out := make(map[string]map[string]bool, len(rows))
	rt, ok := a.resourceType(t)
	if !ok || len(rows) == 0 {
		return out, nil
	}
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.AuthzRef().ID
	}
	acls, err := a.loadACL(ctx, tx, string(t), ids)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		id := r.AuthzRef().ID
		out[id] = make(map[string]bool, len(ks))
		for _, k := range ks {
			out[id][string(k)] = a.evaluator().Decide(rt, string(k), rowOf(r), acls[id], nil).Allowed
		}
	}
	return out, nil
}

func (a *Access) loadACL(ctx context.Context, tx *Tx, rtype string, ids []string) (map[string][]authz.ACLRow, error) {
	if tx != nil {
		return acl.LoadMany(ctx, tx.Tx, rtype, ids)
	}
	if a.rt == nil || a.rt.deps.store == nil {
		return map[string][]authz.ACLRow{}, nil
	}
	var out map[string][]authz.ACLRow
	err := a.rt.deps.store.Run(ctx, pg.TxOptions{ReadOnly: true}, func(ctx context.Context, t *pg.Tx) error {
		var err error
		out, err = acl.LoadMany(ctx, t, rtype, ids)
		return err
	})
	return out, err
}

func rowOf(r Resource) authz.Row {
	at := r.AuthzAttrs()
	return authz.Row{ID: r.AuthzRef().ID, Owner: at.Owner, DeptPath: at.DeptPath, Values: at.Values}
}

// fields is the caller's field access on t (P6.8, E11).
func (a *Access) fields(t ResourceType) authz.FieldAccess {
	rt, ok := a.resourceType(t)
	if !ok {
		return authz.FieldAccess{}
	}
	return a.evaluator().Fields(rt)
}

// Mask sets the fields of v (a pointer to a struct, or to a slice of structs) whose column the caller
// may not read to their zero value (nil for a pointer) and returns the masked columns, for the
// response's _masked (P6.8). A field's column is its json tag name, else its be:"column=<name>" tag.
func (a *Access) Mask(t ResourceType, v any) []string {
	masked := a.fields(t).Masked
	if len(masked) == 0 {
		return []string{}
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return masked
	}
	maskValue(rv.Elem(), masked)
	return masked
}

func maskValue(v reflect.Value, masked []string) {
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			maskValue(v.Index(i), masked)
		}
	case reflect.Pointer:
		if !v.IsNil() {
			maskValue(v.Elem(), masked)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if f := v.Type().Field(i); f.IsExported() && slices.Contains(masked, columnOf(f)) {
				v.Field(i).Set(reflect.Zero(f.Type))
			}
		}
	}
}

func columnOf(f reflect.StructField) string {
	if c, ok := strings.CutPrefix(f.Tag.Get("be"), "column="); ok {
		return c
	}
	name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
	if name == "" || name == "-" {
		return f.Name
	}
	return name
}

// CheckWritable refuses a write that changes a column the caller may not edit: 403 FIELD_FORBIDDEN
// naming the field (P6.8).
func (a *Access) CheckWritable(t ResourceType, changed []string) error {
	if col := a.fields(t).FirstUnwritable(changed); col != "" {
		return problem.Be("FIELD_FORBIDDEN", map[string]string{"field": col})
	}
	return nil
}

// CheckSortable refuses sorting, filtering or aggregating by a masked column: 400 SORT_FORBIDDEN
// (P6.8).
func (a *Access) CheckSortable(t ResourceType, column string) error {
	if a.fields(t).IsMasked(column) {
		return problem.Be("SORT_FORBIDDEN", map[string]string{"field": column})
	}
	return nil
}
