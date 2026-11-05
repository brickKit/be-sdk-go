//go:build integration

package list

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/internal/dbtest"
	"github.com/brickKit/be-sdk-go/v3/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const alice = "0190f3a1-0000-7000-8000-0000000000a1"

type grants struct{ segs []authz.Segment }

func (g *grants) Verify(_ context.Context, req authz.VerifyRequest) (authz.VerifyAnswer, error) {
	s := authz.Shard{Shard: "s", Applicability: []string{"demo.item"}, Digest: "d",
		Subjects: map[string]authz.SubjectEntry{alice: {Segments: g.segs}}}
	return authz.VerifyAnswer{Nonce: req.Nonce, RecoveryGeneration: authz.Revision{Value: "1"},
		Shards: []authz.ShardAnswer{{Shard: "s", Status: "changed", View: &s}}}, nil
}

var defs = &authz.Definitions{Component: "demo/items", Resources: []authz.Resource{{Type: "demo.item", ViewOperation: "demo.item.read",
	Dimensions: map[string]authz.Dimension{},
	Fields: map[string]authz.Field{"id": {Class: authz.Ordinary}, "name": {Class: authz.Ordinary},
		"amount": {Class: authz.Ordinary}, "day": {Class: authz.Ordinary},
		"cost": {Class: authz.Restricted, ReadKey: "demo.item.cost"}}}},
	Operations: []authz.Operation{{Key: "demo.item.read", ResourceType: "demo.item", Purpose: "read", RiskClass: "low"}}}

func seg(fields ...string) authz.Segment {
	return authz.Segment{ResourceType: "demo.item", Operation: "demo.item.read", Atoms: map[string]authz.Atom{},
		ReadFields: fields, Valid: authz.Window{From: "2026-01-01T00:00:00Z"}}
}

var spec = Spec{Operation: "demo.item.read", Alias: "i", IDColumn: "id", IDKind: UUID,
	Columns: []Column{{Field: "name", Column: "name", Kind: Text, Sortable: true, Filterable: true},
		{Field: "amount", Column: "amount", Kind: Decimal, Sortable: true, Filterable: true},
		{Field: "day", Column: "day", Kind: Date, Sortable: true, Filterable: true},
		{Field: "cost", Column: "cost", Kind: Decimal, Sortable: true, Filterable: true}},
	Search: []SearchColumn{{Name: "names", Column: "search_names", Fields: []string{"name"}}}}

func setup(t *testing.T) (*db.DB, *authz.Subject, *grants) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/list-pages.json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	var fx struct{ Rows []map[string]string }
	require.NoError(t, json.Unmarshal(v.Cases[0], &fx))
	d, _ := dbtest.Schema(t, "listing", fstest.MapFS{"0001_items.up.sql": {Data: []byte(`CREATE TABLE items (
		id uuid PRIMARY KEY, name text NOT NULL, amount numeric NOT NULL, day date NOT NULL, cost numeric NOT NULL,
		search_names text NOT NULL, search_v integer NOT NULL);`)}})
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		for _, r := range fx.Rows {
			if _, err := tx.Exec(ctx, "INSERT INTO items VALUES ($1, $2, $3, $4, 1, $5, $6)", r["id"], r["name"], r["amount"],
				r["day"], search.Text(r["name"]), search.Version); err != nil {
				return err
			}
		}
		return nil
	}))
	g := &grants{segs: []authz.Segment{seg("id", "name", "amount", "day", "cost")}}
	h := authz.NewHolder("demo/items", g, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, h.Verify(context.Background()))
	require.NoError(t, defs.Validate())
	return d, &authz.Subject{Principal: alice, Defs: defs, Holder: h, At: time.Now()}, g
}

// page runs one page and returns its IDs and the next cursor ("" at the end).
func page(t *testing.T, d *db.DB, s *authz.Subject, req Request) ([]string, string, error) {
	ctx, q, err := Build(context.Background(), s, spec, req)
	if err != nil {
		return nil, "", err
	}
	var ids []string
	var last []string
	err = d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		sel := []string{"i.id::text"}
		for _, f := range q.SortFields() {
			sel = append(sel, "i."+f+"::text")
		}
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT %s FROM items i WHERE %s ORDER BY %s LIMIT %d",
			strings.Join(sel, ", "), q.Where, q.OrderBy, q.Limit+1), q.Args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			vals := make([]string, len(sel))
			ptrs := make([]any, len(vals))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			if len(ids) < q.Limit {
				ids, last = append(ids, vals[0]), vals
			} else {
				return rows.Err()
			}
		}
		last = nil // no extra row: this is the last page
		return rows.Err()
	})
	if err != nil || last == nil {
		return ids, "", err
	}
	return ids, q.Next(last[1:], last[0]), nil
}

func TestListPagesVectors(t *testing.T) { // P9.1-P9.3
	d, s, _ := setup(t)
	raw, _ := fs.ReadFile(protocol.Vectors, "vectors/list-pages.json")
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	for _, r := range v.Cases[1:] {
		var c struct {
			ID, Kind string
			Sort     []struct{ Field, Dir string }
			PageSize int `json:"page_size"`
			Expect   json.RawMessage
		}
		require.NoError(t, json.Unmarshal(r, &c))
		req := Request{Limit: c.PageSize}
		for _, o := range c.Sort {
			req.Sort = append(req.Sort, Order{Field: o.Field, Desc: o.Dir == "desc"})
		}
		if c.Kind == "refused" {
			_, err := ParseLimit(fmt.Sprint(c.PageSize))
			assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), c.ID)
			continue
		}
		var want [][]string
		require.NoError(t, json.Unmarshal(c.Expect, &want))
		var got [][]string
		for {
			ids, next, err := page(t, d, s, req)
			require.NoError(t, err, c.ID)
			got = append(got, ids)
			if next == "" {
				break
			}
			req.Cursor = next
		}
		assert.Equal(t, want, got, c.ID)
	}
}

func TestCursorsBindTheirFilterAndPlan(t *testing.T) { // P9.3, CK-LS-01
	d, s, g := setup(t)
	req := Request{Limit: 3, Sort: []Order{{Field: "name"}}}
	_, next, err := page(t, d, s, req)
	require.NoError(t, err)
	_, _, err = page(t, d, s, Request{Limit: 3, Sort: []Order{{Field: "amount"}}, Cursor: next})
	assert.True(t, errs.Is(err, "be", errs.ReasonCursorInvalid), "another sort: %v", err)
	_, _, err = page(t, d, s, Request{Limit: 3, Sort: req.Sort, Q: "alpha", Cursor: next})
	assert.True(t, errs.Is(err, "be", errs.ReasonCursorInvalid), "another search")
	_, _, err = page(t, d, s, Request{Limit: 3, Sort: req.Sort, Cursor: "not-a-cursor"})
	assert.True(t, errs.Is(err, "be", errs.ReasonCursorInvalid))

	g.segs = []authz.Segment{seg("id", "name")} // the grants changed
	require.NoError(t, s.Holder.Verify(context.Background()))
	_, _, err = page(t, d, s, Request{Limit: 3, Sort: req.Sort, Cursor: next})
	assert.True(t, errs.Is(err, "be", errs.ReasonQueryPlanChanged), "%v", err)
}

func TestRestrictedFieldsAndSearch(t *testing.T) { // P9.4, P9.7, P9.8, CK-LS-02
	d, s, g := setup(t)
	ids, _, err := page(t, d, s, Request{Sort: []Order{{Field: "cost"}}})
	require.NoError(t, err, "cost is readable in every segment")
	assert.Len(t, ids, 8)

	g.segs = []authz.Segment{seg("id", "name", "amount", "day")}
	require.NoError(t, s.Holder.Verify(context.Background()))
	_, _, err = page(t, d, s, Request{Sort: []Order{{Field: "cost"}}})
	assert.True(t, errs.Is(err, "be", errs.ReasonFieldNotQueryable), "%v", err)
	_, _, err = page(t, d, s, Request{Filters: []Filter{{Field: "cost", Op: "gte", Values: []string{"1"}}}})
	assert.True(t, errs.Is(err, "be", errs.ReasonFieldNotQueryable))

	for q, n := range map[string]int{"alpha": 3, "ALP": 3, "eps": 1, "a%": 0, "": 8} {
		ids, _, err := page(t, d, s, Request{Q: q})
		require.NoError(t, err)
		assert.Len(t, ids, n, "q=%q", q)
	}
	ids, _, err = page(t, d, s, Request{Filters: []Filter{{Field: "amount", Op: "in", Values: []string{"10", "5"}}}})
	require.NoError(t, err)
	assert.Len(t, ids, 5)
	ids, _, err = page(t, d, s, Request{Filters: []Filter{{Field: "day", Op: "gte", Values: []string{"2026-10-03"}}}})
	require.NoError(t, err)
	assert.Len(t, ids, 2)
}

func TestChineseSearchByPinyin(t *testing.T) { // P9.6
	d, s, _ := setup(t)
	require.NoError(t, d.Tx(context.Background(), func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO items VALUES ('0190f3a1-0000-7000-8000-0000000000ff', '中国银行', 1, '2026-10-05', 1, $1, 1)",
			search.Text("中国银行"))
		return err
	}))
	for _, q := range []string{"银行", "zhongguo", "zgyx", "ZHONG"} {
		ids, _, err := page(t, d, s, Request{Q: q})
		require.NoError(t, err)
		assert.Equal(t, []string{"0190f3a1-0000-7000-8000-0000000000ff"}, ids, q)
	}
}

// TestReadableOnlyNarrowsToTheRowsThatShowAField: cost is readable only on the alpha rows; an explicit readable_only
// narrows the list to them, lets cost sort and filter there, and changes the filter digest (P9.4, security 07's FX12).
// Without it nothing narrows.
func TestReadableOnlyNarrowsToTheRowsThatShowAField(t *testing.T) {
	d, _, _ := setup(t)
	byName := &authz.Definitions{Component: "demo/items", Resources: []authz.Resource{{Type: "demo.item", ViewOperation: "demo.item.read",
		Dimensions: map[string]authz.Dimension{"name": {Kind: authz.ValueSet, Column: "name"}},
		Fields:     defs.Resources[0].Fields}}, Operations: defs.Operations}
	require.NoError(t, byName.Validate())
	alphaCost := seg("id", "name", "cost")
	alphaCost.Atoms = map[string]authz.Atom{"name": {Kind: "in", Values: []string{"alpha"}}}
	others := seg("id", "name")
	others.Atoms = map[string]authz.Atom{"name": {Kind: "all"}}
	g := &grants{segs: []authz.Segment{alphaCost, others}}
	h := authz.NewHolder("demo/items", g, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, h.Verify(context.Background()))
	s := &authz.Subject{Principal: alice, Defs: byName, Holder: h, At: time.Now()}

	ids, _, err := page(t, d, s, Request{})
	require.NoError(t, err)
	assert.Len(t, ids, 8, "no narrowing unless asked: the rows without a readable cost are listed (masked)")
	_, _, err = page(t, d, s, Request{Sort: []Order{{Field: "cost"}}})
	assert.True(t, errs.Is(err, "be", errs.ReasonFieldNotQueryable), "precondition: cost is not readable everywhere: %v", err)

	narrowed := Request{ReadableOnly: []string{"cost", "cost"}, Sort: []Order{{Field: "cost"}},
		Filters: []Filter{{Field: "cost", Op: "gte", Values: []string{"1"}}}, Limit: 2}
	ids, next, err := page(t, d, s, narrowed)
	require.NoError(t, err, "narrowed to the alpha rows, cost sorts and filters")
	rest, _, err := page(t, d, s, Request{ReadableOnly: narrowed.ReadableOnly, Sort: narrowed.Sort, Filters: narrowed.Filters,
		Limit: 2, Cursor: next})
	require.NoError(t, err)
	assert.Len(t, append(ids, rest...), 3, "the three alpha rows")
	_, q, err := Build(context.Background(), s, spec, narrowed)
	require.NoError(t, err)
	assert.Equal(t, []string{"cost"}, q.ReadableOnly, "the answer says how it narrowed")

	// the same sort, only the narrowing differs: a cursor of the narrowed list does not continue the whole one
	sorted := Request{ReadableOnly: []string{"cost"}, Sort: []Order{{Field: "name"}}, Limit: 2}
	_, next, err = page(t, d, s, sorted)
	require.NoError(t, err)
	require.NotEmpty(t, next, "precondition: a second page")
	_, _, err = page(t, d, s, Request{Sort: sorted.Sort, Limit: 2, Cursor: next})
	assert.True(t, errs.Is(err, "be", errs.ReasonCursorInvalid), "the narrowing is part of the filter digest: %v", err)
	_, _, err = page(t, d, s, Request{ReadableOnly: []string{"nonexistent"}})
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "%v", err)
}
