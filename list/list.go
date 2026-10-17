// Package list builds component lists (protocol P9.1-P9.8): keyset pages with the record ID as the last key, opaque
// cursors bound to the filter and plan digests, declared filters and sorts, list search on normalized search
// columns, all in one SQL statement with the subject's authorization predicate.
package list

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/search"
	"github.com/jackc/pgx/v5"
)

// Page sizes (P9.2).
const (
	DefaultLimit = 50
	MaxLimit     = 200
)

// Kinds of columns: how cursor values and filter values are cast.
const (
	Text    = "text"
	Decimal = "numeric"
	Date    = "date"
	Time    = "timestamptz"
	UUID    = "uuid"
)

// Column is a field a list may filter or sort by. Sortable columns are not nullable.
type Column struct {
	Field      string
	Column     string
	Kind       string
	Sortable   bool
	Filterable bool
}

// SearchColumn is a stored search column made of some fields of the same read permission (P9.8).
type SearchColumn struct {
	Name   string
	Column string
	Fields []string
}

// Spec describes one list of a resource.
type Spec struct {
	Operation string // the list's operation (the view operation for ordinary lists)
	Alias     string // the table alias the SQL uses
	IDColumn  string
	IDKind    string
	Columns   []Column
	Search    []SearchColumn
}

// Order is one sort key.
type Order struct {
	Field string `json:"field"`
	Desc  bool   `json:"desc,omitempty"`
}

// Filter is one condition: eq, in, gte, gt, lte, lt.
type Filter struct {
	Field  string   `json:"field"`
	Op     string   `json:"op"`
	Values []string `json:"values"`
}

// Request is a list request.
type Request struct {
	Sort    []Order
	Filters []Filter
	Q       string
	Cursor  string
	Limit   int
}

// Query is a compiled list page: SELECT the columns you need plus Hits, FROM your table AS the spec's alias,
// WHERE Where, ORDER BY OrderBy, LIMIT Limit+1 (one more row tells whether there is a next page).
type Query struct {
	Where   string
	Args    []any
	OrderBy string
	Limit   int
	Hits    []string

	spec       Spec
	plan       *authz.Plan
	sorts      []Order
	cols       map[string]Column
	filterHash string
	planHash   string
}

type cursor struct {
	V    int      `json:"v"`
	Keys []string `json:"k"`
	ID   string   `json:"id"`
	F    string   `json:"f"`
	P    string   `json:"p"`
}

func bad(field, why string) error {
	return errs.New(errs.ReasonRequestInvalid, why).WithViolations(errs.Violation{Field: field, Reason: "INVALID", Description: why})
}

func digest(v any) string {
	b, _ := json.Marshal(v)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:16])
}

// ParseLimit reads a page size given by a caller: absent is the default; 0 or more than MaxLimit is refused (P9.2).
func ParseLimit(v string) (int, error) {
	if v == "" {
		return DefaultLimit, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > MaxLimit {
		return 0, bad("limit", fmt.Sprintf("the page size must be 1-%d", MaxLimit))
	}
	return n, nil
}

// Build compiles one page for the request's subject.
func Build(ctx context.Context, subj *authz.Subject, spec Spec, req Request) (context.Context, *Query, error) {
	if req.Limit == 0 {
		req.Limit = DefaultLimit // not given
	}
	if req.Limit < 1 || req.Limit > MaxLimit {
		return ctx, nil, bad("limit", fmt.Sprintf("the page size must be 1-%d", MaxLimit))
	}
	ctx, plan, err := subj.List(ctx, spec.Operation, spec.Alias, 0)
	if err != nil {
		return ctx, nil, err
	}
	q := &Query{spec: spec, plan: plan, Limit: req.Limit, Hits: plan.Hits, cols: map[string]Column{}}
	for _, c := range spec.Columns {
		q.cols[c.Field] = c
	}
	q.Args = append(q.Args, plan.Args...)
	where := []string{plan.Where}
	restricted := func(field string) bool {
		r, _ := subj.Defs.Resource(resourceOf(subj, spec.Operation))
		return r != nil && r.Fields[field].Class != authz.Ordinary
	}
	for _, f := range req.Filters {
		c, ok := q.cols[f.Field]
		if !ok || !c.Filterable {
			return ctx, nil, bad("filter."+f.Field, "not a filterable field")
		}
		if restricted(f.Field) && !plan.ReadableEverywhere(f.Field) {
			return ctx, nil, errs.New(errs.ReasonFieldNotQueryable, "this field cannot be used to filter here").With("field", f.Field)
		}
		cond, err := q.filter(c, f)
		if err != nil {
			return ctx, nil, err
		}
		where = append(where, cond)
	}
	nq := search.Query(req.Q)
	if nq != "" {
		var ors []string
		for _, sc := range spec.Search {
			usable := true
			for _, f := range sc.Fields {
				usable = usable && (!restricted(f) || plan.ReadableEverywhere(f))
			}
			if usable {
				ors = append(ors, fmt.Sprintf("%s LIKE %s ESCAPE '\\'", q.col(sc.Column), q.arg(search.Like(nq))))
			}
		}
		if len(ors) == 0 {
			ors = []string{"FALSE"} // no column this caller may search: nothing matches, nothing leaks
		}
		where = append(where, "("+strings.Join(ors, " OR ")+")")
	}
	for _, o := range req.Sort {
		c, ok := q.cols[o.Field]
		if !ok || !c.Sortable {
			return ctx, nil, bad("sort."+o.Field, "not a sortable field")
		}
		if restricted(o.Field) && !plan.ReadableEverywhere(o.Field) {
			return ctx, nil, errs.New(errs.ReasonFieldNotQueryable, "this field cannot be used to sort here").With("field", o.Field)
		}
	}
	q.sorts = req.Sort
	q.filterHash = digest(map[string]any{"filters": req.Filters, "q": nq, "sort": req.Sort})
	q.planHash = plan.Digest
	if req.Cursor != "" {
		cond, limit, err := q.resume(req.Cursor, req.Limit)
		if err != nil {
			return ctx, nil, err
		}
		where = append(where, cond)
		q.Limit = limit
	}
	q.Where = strings.Join(where, " AND ")
	var order []string
	for _, o := range q.sorts {
		dir := "ASC"
		if o.Desc {
			dir = "DESC"
		}
		order = append(order, q.col(q.cols[o.Field].Column)+" "+dir)
	}
	q.OrderBy = strings.Join(append(order, q.col(spec.IDColumn)+" ASC"), ", ")
	return ctx, q, nil
}

func resourceOf(subj *authz.Subject, op string) string {
	if o, ok := subj.Defs.Operation(op); ok {
		return o.ResourceType
	}
	return ""
}

func (q *Query) arg(v any) string {
	q.Args = append(q.Args, v)
	return "$" + strconv.Itoa(len(q.Args))
}

func (q *Query) col(name string) string {
	if q.spec.Alias == "" {
		return pgx.Identifier{name}.Sanitize()
	}
	return pgx.Identifier{q.spec.Alias, name}.Sanitize()
}

func (q *Query) typed(c Column, v string) string { return q.arg(v) + "::" + c.Kind }

func (q *Query) filter(c Column, f Filter) (string, error) {
	col := q.col(c.Column)
	ops := map[string]string{"eq": "=", "gte": ">=", "gt": ">", "lte": "<=", "lt": "<"}
	if f.Op == "in" {
		if len(f.Values) == 0 {
			return "FALSE", nil
		}
		return fmt.Sprintf("%s = ANY(%s::%s[])", col, q.arg(f.Values), c.Kind), nil
	}
	op, ok := ops[f.Op]
	if !ok || len(f.Values) != 1 {
		return "", bad("filter."+f.Field, "operator must be eq, in, gte, gt, lte or lt with one value (in: a list)")
	}
	return fmt.Sprintf("%s %s %s", col, op, q.typed(c, f.Values[0])), nil
}

// resume decodes a cursor and returns the keyset condition: rows strictly after the last row in the order.
func (q *Query) resume(raw string, limit int) (string, int, error) {
	b, err := base64.RawURLEncoding.DecodeString(raw)
	var c cursor
	if err != nil || json.Unmarshal(b, &c) != nil || c.V != 1 || len(c.Keys) != len(q.sorts) {
		return "", 0, errs.New(errs.ReasonCursorInvalid, "the cursor cannot be read")
	}
	if c.F != q.filterHash {
		return "", 0, errs.New(errs.ReasonCursorInvalid, "the cursor belongs to another filter, search or sort")
	}
	if c.P != q.planHash {
		return "", 0, errs.New(errs.ReasonQueryPlanChanged, "the authorization changed; query again")
	}
	// (k1, k2, ..., id) after (v1, v2, ..., id0), each key in its own direction:
	// k1 >1 v1 OR (k1 = v1 AND k2 >2 v2) OR ... OR (all keys equal AND id > id0)
	var ors []string
	for i := 0; i <= len(q.sorts); i++ {
		var ands []string
		for j := 0; j < i; j++ {
			c2 := q.cols[q.sorts[j].Field]
			ands = append(ands, fmt.Sprintf("%s = %s", q.col(c2.Column), q.typed(c2, c.Keys[j])))
		}
		if i < len(q.sorts) {
			c2 := q.cols[q.sorts[i].Field]
			op := ">"
			if q.sorts[i].Desc {
				op = "<"
			}
			ands = append(ands, fmt.Sprintf("%s %s %s", q.col(c2.Column), op, q.typed(c2, c.Keys[i])))
		} else {
			ands = append(ands, fmt.Sprintf("%s > %s::%s", q.col(q.spec.IDColumn), q.arg(c.ID), q.spec.IDKind))
		}
		ors = append(ors, "("+strings.Join(ands, " AND ")+")")
	}
	return "(" + strings.Join(ors, " OR ") + ")", limit, nil
}

// Next returns the cursor that continues after a row, given its sort values (as text, in sort order) and ID.
func (q *Query) Next(keys []string, id string) string {
	b, _ := json.Marshal(cursor{V: 1, Keys: keys, ID: id, F: q.filterHash, P: q.planHash})
	return base64.RawURLEncoding.EncodeToString(b)
}

// SortFields lists the fields of the sort keys in order (their values make the next cursor).
func (q *Query) SortFields() []string {
	out := make([]string, len(q.sorts))
	for i, o := range q.sorts {
		out[i] = o.Field
	}
	return out
}

// Fields returns a row's readable fields from its hit bitmap.
func (q *Query) Fields(hits []bool) []string { return q.plan.Fields(hits) }
