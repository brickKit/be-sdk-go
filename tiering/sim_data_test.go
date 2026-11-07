//go:build simulation

package tiering_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/tiering"
)

// The long simulation's data (assembly protocol 11's replacement checks; foundations item 5, T11a): one class of three
// levels and every column type, one erasable column at two levels, its route chosen by the deployment.

const simClass = "sim.docs"

const simSchemaSQL = `CREATE TABLE docs (
  id uuid PRIMARY KEY, grp text NOT NULL, rev integer NOT NULL, code text NOT NULL, note text,
  amount numeric(19,4) NOT NULL, qty bigint NOT NULL, small smallint NOT NULL, flag boolean NOT NULL, day date,
  attrs jsonb, blob bytea, tags text[], closed_at timestamptz, created_at timestamptz NOT NULL);
CREATE TABLE doc_lines (
  id uuid PRIMARY KEY, doc_id uuid NOT NULL REFERENCES docs (id), line integer NOT NULL, label text, person text);
CREATE TABLE line_marks (
  line_id uuid NOT NULL REFERENCES doc_lines (id), seq integer NOT NULL, mark text, PRIMARY KEY (line_id, seq));
CREATE TABLE sim_backup (finished_at timestamptz NOT NULL);`

// simOrphanAge is the simulation's verification window before an orphan goes (P15.20). It must exceed the longest
// transfer, or the cleanup takes a transfer's objects before it verifies them (20 s did, under the simulation's load).
const simOrphanAge = 90 * time.Second

// simCleanupEvery is the replicas' cleanup interval: each run lists every object of the class (tens of thousands by the
// end of a run).
const simCleanupEvery = 30 * time.Second

// simParts makes a unit's tables span several Parquet objects (R7: the data volume reaches the boundaries).
const simParts = 40

func simClassDef() tiering.Class {
	return tiering.Class{Name: simClass, Category: tiering.CategoryCompleted,
		Root: tiering.Table{Name: "docs", PK: "id", Columns: map[string]string{"id": tiering.TypeUUID,
			"grp": tiering.TypeText, "rev": tiering.TypeInt4, "code": tiering.TypeText, "note": tiering.TypeText,
			"amount": tiering.TypeNumeric, "qty": tiering.TypeInt8, "small": tiering.TypeInt2, "flag": tiering.TypeBool,
			"day": tiering.TypeDate, "attrs": tiering.TypeJSONB, "blob": tiering.TypeBytea, "tags": tiering.TypeTextArray,
			"closed_at": tiering.TypeTimestamptz, "created_at": tiering.TypeTimestamptz}},
		Children: []tiering.Child{
			{Table: tiering.Table{Name: "doc_lines", PK: "id", Columns: map[string]string{"id": tiering.TypeUUID,
				"doc_id": tiering.TypeUUID, "line": tiering.TypeInt4, "label": tiering.TypeText, "person": tiering.TypeText}},
				FK: "doc_id"},
			{Table: tiering.Table{Name: "line_marks", Key: []string{"line_id", "seq"}, Columns: map[string]string{
				"line_id": tiering.TypeUUID, "seq": tiering.TypeInt4, "mark": tiering.TypeText}}, FK: "line_id",
				Parent: "doc_lines"}},
		Group: []string{"grp"}, Anchor: "closed_at", Created: "created_at", Terminal: "r.closed_at IS NOT NULL",
		ResidenceDays: 365, Retention: tiering.Retention{MinDays: 3650, MaxDays: 4000, Basis: "the simulation"},
		Locate: []string{"grp"}, Erasable: []string{"note", "person"},
		Routes: []string{tiering.RoutePostgres, tiering.RouteObject}, SchemaVersion: "1",
		HoldOperation: "sim.hold", ApproveOperation: "sim.approve"}
}

// simBackups is the deployment's latest backup: the newest row of sim_backup (the parent records one every few seconds).
func simBackups(d *db.DB) tiering.BackupSource {
	return func(ctx context.Context) (time.Time, bool, error) {
		var at *time.Time
		err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, "SELECT max(finished_at) FROM sim_backup").Scan(&at)
		})
		if err != nil || at == nil {
			return time.Time{}, false, err
		}
		return *at, true, nil
	}
}

// canonRows reads rows of a class table as canonical rows (P15.8), as the copy holds them.
func canonRows(ctx context.Context, tx db.Tx, t tiering.Table, where string, args ...any) ([]map[string]any, error) {
	names := make([]string, 0, len(t.Columns))
	for n := range t.Columns {
		names = append(names, n)
	}
	sort.Strings(names)
	exprs := make([]string, len(names))
	for i, n := range names {
		exprs[i] = "t." + n
		if typ := t.Columns[n]; typ == tiering.TypeNumeric || typ == tiering.TypeJSONB {
			exprs[i] += "::text"
		}
	}
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT %s FROM %s t WHERE %s", strings.Join(exprs, ", "), t.Name, where), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		m := map[string]any{}
		for i, n := range names {
			m[n] = vals[i]
		}
		b, err := tiering.Row(t.Columns, m)
		if err != nil {
			return nil, err
		}
		var obj map[string]any
		if err := json.Unmarshal(b, &obj); err != nil {
			return nil, err
		}
		out = append(out, obj)
	}
	return out, rows.Err()
}

// simAggregate is one document as JSON: its root and every child table's rows in a fixed order (empty tables left out).
func simAggregate(root map[string]any, children map[string][]map[string]any) string {
	kids := map[string][]string{}
	for name, rows := range children {
		for _, r := range rows {
			b, _ := json.Marshal(r)
			kids[name] = append(kids[name], string(b))
		}
		sort.Strings(kids[name])
		if len(kids[name]) == 0 {
			delete(kids, name)
		}
	}
	b, _ := json.Marshal(map[string]any{"root": root, "children": kids})
	return string(b)
}

// readOnline reads a document from the online tables; found=false: not there.
func readOnline(ctx context.Context, d *db.DB, id string) (string, bool, error) {
	cl := simClassDef()
	var out string
	found := false
	err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		roots, err := canonRows(ctx, tx, cl.Root, "t.id = $1::uuid", id)
		if err != nil || len(roots) == 0 {
			return err
		}
		lines, err := canonRows(ctx, tx, cl.Children[0].Table, "t.doc_id = $1::uuid", id)
		if err != nil {
			return err
		}
		marks, err := canonRows(ctx, tx, cl.Children[1].Table,
			"t.line_id IN (SELECT id FROM doc_lines WHERE doc_id = $1::uuid)", id)
		if err != nil {
			return err
		}
		out, found = simAggregate(roots[0], map[string][]map[string]any{"doc_lines": lines, "line_marks": marks}), true
		return nil
	})
	return out, found, err
}

// erased is a document's expected form after its erasure: the erasable columns null (a canonical row leaves a null
// out), at both levels.
func erased(agg string) string {
	var a struct {
		Root     map[string]any      `json:"root"`
		Children map[string][]string `json:"children"`
	}
	_ = json.Unmarshal([]byte(agg), &a)
	delete(a.Root, "note")
	children := map[string][]map[string]any{}
	for name, rows := range a.Children {
		for _, raw := range rows {
			var r map[string]any
			_ = json.Unmarshal([]byte(raw), &r)
			if name == "doc_lines" {
				delete(r, "person")
			}
			children[name] = append(children[name], r)
		}
	}
	return simAggregate(a.Root, children)
}
