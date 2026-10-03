package authz

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// The property of P6.7 / E10 "List/Can consistency" over generated bundles, tokens, rows, grants,
// delegations, ceilings, shares and graph ids: for every resource type shape, the canonical predicate
// run by PostgreSQL lists exactly the rows whose single-record vis(K, row) holds, and the three
// disjoint branches together list the same rows once each.

var propertyTypes = []string{
	`{"type":"t.k.order","owner_component":"t/k","view_key":"t.k.view","dimensions":["owner","org","warehouse"],"derivation":"direct",
	  "relations":{"viewer":{"grants":["t.k.view"]},"editor":{"grants":["t.k.act"],"includes":["viewer"]},
	               "member":{"grants":["t.k.act"],"includes":["viewer"],"owned_by":"component"}}}`,
	`{"type":"t.k.balance","owner_component":"t/k","view_key":"t.k.view","dimensions":["warehouse","legal_entity"],"derivation":"direct","relations":{}}`,
	`{"type":"t.k.plain","owner_component":"t/k","view_key":"t.k.view","derivation":"direct","relations":{"viewer":{"grants":["t.k.view"]}}}`,
	`{"type":"t.k.orgonly","owner_component":"t/k","view_key":"t.k.view","dimensions":["org","legal_entity"],"derivation":"direct",
	  "relations":{"editor":{"grants":["t.k.view","t.k.act"]}}}`,
	`{"type":"t.k.folder","owner_component":"t/k","view_key":"t.k.view","dimensions":["owner"],"derivation":"graph",
	  "relations":{"viewer":{"grants":["t.k.view"]}}}`,
}

var (
	propDepts    = []string{"/", "/1/", "/1/12/", "/1/12/5/", "/1/123/", "/a_b/", "/a%b/", "/aXb/", "", "1/12", `/a\b/`}
	propUsers    = []string{"u_me", "u_a", "u_b"}
	propWH       = []string{"S", "N", "Z"}
	propLE       = []string{"le1", "le2"}
	propKeys     = []string{"t.k.view", "t.k.act"}
	propLevels   = []string{"own", "dept", "subtree", "all", ""}
	propSubjects = []string{"user:u_me", "user:u_a", "role:r1", "role:r2", "dept:/1/12/", "dept:/1/", "dept_tree:/1/", "dept_tree:/", "dept_tree:/2/"}
)

func drawOpt[T any](t *rapid.T, gen *rapid.Generator[T], label string) *T {
	if !rapid.Bool().Draw(t, label+"?") {
		return nil
	}
	v := gen.Draw(t, label)
	return &v
}

func drawWindow(t *rapid.T, label string) map[string]any {
	out := map[string]any{}
	if v := drawOpt(t, rapid.IntRange(-50, 50), label+".from"); v != nil {
		out["from_ts"] = listNow.Unix() + int64(*v)
	}
	if v := drawOpt(t, rapid.IntRange(-50, 50), label+".until"); v != nil {
		out["until"] = listNow.Unix() + int64(*v)
	}
	return out
}

// drawBundle generates roles r1..r3, their grants, an optional ceiling profile p1 and delegations.
func drawBundle(t *rapid.T) map[string]any {
	roles, grants := map[string]any{}, map[string]any{}
	for _, r := range []string{"r1", "r2", "r3"} {
		roles[r] = rapid.SliceOfDistinct(rapid.SampledFrom(propKeys), rapid.ID[string]).Draw(t, r+".keys")
		g := drawWindow(t, r)
		levels := map[string]string{}
		for _, k := range propKeys {
			if l := rapid.SampledFrom(propLevels).Draw(t, r+".level."+k); l != "" {
				levels[k] = l
			}
		}
		g["levels"] = levels
		if l := rapid.SampledFrom(propLevels).Draw(t, r+".default"); l != "" {
			g["default_level"] = l
		}
		g["values"] = map[string][]string{
			"org":          rapid.SliceOfDistinct(rapid.SampledFrom(append([]string{"*"}, propDepts...)), rapid.ID[string]).Filter(noEmpty).Draw(t, r+".org"),
			"warehouse":    rapid.SliceOfDistinct(rapid.SampledFrom(append([]string{"*"}, propWH...)), rapid.ID[string]).Draw(t, r+".wh"),
			"legal_entity": rapid.SliceOfDistinct(rapid.SampledFrom(append([]string{"*"}, propLE...)), rapid.ID[string]).Draw(t, r+".le"),
		}
		grants[r] = g
	}
	var delegations []any
	for i := range rapid.IntRange(0, 2).Draw(t, "delegations") {
		d := drawWindow(t, fmt.Sprintf("dg%d", i))
		d["id"], d["mode"], d["to"] = fmt.Sprintf("dg_%d", i), "on_behalf", "u_me"
		d["from"] = rapid.SampledFrom([]string{"u_a", "u_b"}).Draw(t, "from")
		d["keys"] = rapid.SliceOfDistinct(rapid.SampledFrom(propKeys), rapid.ID[string]).Draw(t, "dg.keys")
		delegations = append(delegations, d)
	}
	return map[string]any{
		"contract": "authz/2.0", "revision": "1", "catalog_digest": "sha256:" + zeros64, "stale_since": map[string]any{},
		"capabilities": map[string]any{"core": true, "sharing": rapid.Bool().Draw(t, "sharing"),
			"relation_sync": rapid.Bool().Draw(t, "relation_sync"), "delegation": rapid.Bool().Draw(t, "delegation"),
			"graph": rapid.Bool().Draw(t, "graph")},
		"roles": roles, "grants": grants, "delegations": delegations,
		"profiles": map[string]any{"p1": map[string]any{
			"keys":      rapid.SliceOfDistinct(rapid.SampledFrom(propKeys), rapid.ID[string]).Draw(t, "p1.keys"),
			"max_level": rapid.SampledFrom(propLevels[:4]).Draw(t, "p1.max"),
			"relations": rapid.SliceOfDistinct(rapid.SampledFrom([]string{"viewer", "editor", "member"}), rapid.ID[string]).Draw(t, "p1.rel")}},
	}
}

func noEmpty(s []string) bool {
	for _, v := range s {
		if v == "" {
			return false
		}
	}
	return true
}

func drawRows(t *rapid.T) []dbRow {
	n := rapid.IntRange(0, 10).Draw(t, "rows")
	rows := make([]dbRow, n)
	for i := range rows {
		rows[i] = dbRow{id: fmt.Sprintf("r%02d", i),
			owner:    drawOpt(t, rapid.SampledFrom(propUsers), "owner"),
			dept:     drawOpt(t, rapid.SampledFrom(propDepts), "dept"),
			wh:       drawOpt(t, rapid.SampledFrom(propWH), "wh"),
			legalEnt: drawOpt(t, rapid.SampledFrom(propLE), "le")}
	}
	return rows
}

func drawACL(t *rapid.T, rt *ResourceType, rows []dbRow) []ACLRow {
	if len(rows) == 0 {
		return nil
	}
	relations := []string{"viewer", "editor", "member", "ghost"}
	var out []ACLRow
	for range rapid.IntRange(0, 8).Draw(t, "acl") {
		a := ACLRow{RType: rapid.SampledFrom([]string{rt.Type, "t.k.elsewhere"}).Draw(t, "rtype"),
			RID: rows[rapid.IntRange(0, len(rows)-1).Draw(t, "rid")].id, Relation: rapid.SampledFrom(relations).Draw(t, "relation"),
			Subject: rapid.SampledFrom(propSubjects).Draw(t, "subject")}
		if d := drawOpt(t, rapid.IntRange(-3, 3), "expires"); d != nil {
			at := listNow.Add(time.Duration(*d) * time.Second)
			a.ExpiresAt = &at
		}
		out = append(out, a)
	}
	return out
}

func TestListCanConsistencyProperty(t *testing.T) {
	env := newDBEnv(t)
	types := make([]*ResourceType, len(propertyTypes))
	for i, raw := range propertyTypes {
		types[i] = mustType(t, raw)
	}
	rapid.Check(t, func(rt *rapid.T) {
		typ := rapid.SampledFrom(types).Draw(rt, "type")
		raw, err := json.Marshal(drawBundle(rt))
		require.NoError(rt, err)
		b, err := ParseBundle(raw)
		require.NoError(rt, err)
		tok := Token{Sub: "u_me", DeptPath: rapid.SampledFrom(propDepts).Draw(rt, "token.dept"),
			Roles: rapid.SliceOfDistinct(rapid.SampledFrom([]string{"r1", "r2", "r3", "ghost"}), rapid.ID[string]).Draw(rt, "token.roles")}
		if rapid.Bool().Draw(rt, "ceiling") {
			tok.Ceil = []string{rapid.SampledFrom([]string{"p1", "unknown"}).Draw(rt, "ceil")}
		}
		rows := drawRows(rt)
		acl := drawACL(rt, typ, rows)
		graphIDs := rapid.SliceOfDistinct(rapid.SampledFrom([]string{"r00", "r01", "r03", "r07", "nope"}), rapid.ID[string]).Draw(rt, "graph")
		key := rapid.SampledFrom(propKeys).Draw(rt, "key")

		e := NewEvaluator(b, tok, listNow)
		ka := e.Key(typ, key, func(string) []string { return graphIDs })
		env.reset(rt, rows, acl)
		p, err := NewPredicate(typ, itemColumns)
		require.NoError(rt, err)
		got := env.list(rt, p, ka.Params, listNow)
		require.Equal(rt, visibleIDs(e, typ, ka, rows, acl), got, "List(K) = {r | vis(K, r)} (P6.7, E10)")
		require.Equal(rt, got, env.listBranches(rt, p, ka.Params, listNow), "UNION ALL of the branches")
	})
}
