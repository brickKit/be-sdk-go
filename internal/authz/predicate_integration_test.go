package authz

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var listNow = time.Unix(1760000000, 0).UTC()

// testBundle is a bundle with every capability on, one role rep holding t.k.view at the given level,
// and the given extra grant values.
func testBundle(t *testing.T, level string, values string) *Bundle {
	t.Helper()
	b, err := ParseBundle([]byte(`{"contract":"authz/2.0","revision":"1","catalog_digest":"sha256:` + zeros64 + `",
		"capabilities":{"core":true,"sharing":true,"relation_sync":true,"delegation":true,"graph":true},
		"roles":{"rep":["t.k.view"]},"grants":{"rep":{"default_level":"` + level + `","values":{` + values + `}}},
		"stale_since":{}}`))
	require.NoError(t, err)
	return b
}

const zeros64 = "0000000000000000000000000000000000000000000000000000000000000000"

const ownerOrgType = `{"type":"t.k.item","owner_component":"t/k","view_key":"t.k.view","dimensions":["owner","org"],
	"derivation":"direct","relations":{"viewer":{"grants":["t.k.view"]}}}`

// assertListMatchesCan checks P6.7 for one evaluation and returns the listed ids.
func assertListMatchesCan(t *testing.T, env *dbEnv, e *Evaluator, rt *ResourceType, key string, rows []dbRow, acl []ACLRow) []string {
	t.Helper()
	env.reset(t, rows, acl)
	ka := e.Key(rt, key, nil)
	p, err := NewPredicate(rt, itemColumns)
	require.NoError(t, err)
	got := env.list(t, p, ka.Params, listNow)
	require.Equal(t, visibleIDs(e, rt, ka, rows, acl), got, "List(K) = {r | vis(K, r)} (P6.7, E10)")
	require.Equal(t, got, env.listBranches(t, p, ka.Params, listNow), "UNION ALL of the branches = the predicate")
	return got
}

func TestListEscapesLikeInDepartmentPaths(t *testing.T) {
	env := newDBEnv(t)
	rt := mustType(t, ownerOrgType)
	rows := []dbRow{
		{id: "in-self", owner: sp("u_x"), dept: sp("/a_b/c%d/")},
		{id: "in-child", owner: sp("u_x"), dept: sp("/a_b/c%d/e/")},
		{id: "out-underscore", owner: sp("u_x"), dept: sp("/aXb/c%d/")},
		{id: "out-percent", owner: sp("u_x"), dept: sp("/a_b/cZZd/")},
		{id: "out-backslash", owner: sp("u_x"), dept: sp(`/a_b/c%d\/`)},
	}
	e := NewEvaluator(testBundle(t, "subtree", ""), Token{Sub: "u_me", Roles: []string{"rep"}, DeptPath: "/a_b/c%d/"}, listNow)
	require.Equal(t, []string{"in-child", "in-self"}, assertListMatchesCan(t, env, e, rt, "t.k.view", rows, nil))

	org := NewEvaluator(testBundle(t, "own", `"org":["/x_y/"]`), Token{Sub: "u_me", Roles: []string{"rep"}}, listNow)
	rows = []dbRow{{id: "custom", owner: sp("u_x"), dept: sp("/x_y/z/")}, {id: "wild", owner: sp("u_x"), dept: sp("/xQy/z/")}}
	require.Equal(t, []string{"custom"}, assertListMatchesCan(t, env, org, rt, "t.k.view", rows, nil), "org values are escaped too")
}

func TestListHonoursACLExpiry(t *testing.T) {
	env := newDBEnv(t)
	rt := mustType(t, ownerOrgType)
	e := NewEvaluator(testBundle(t, "own", ""), Token{Sub: "u_me", DeptPath: "/1/"}, listNow)
	at := func(d time.Duration) *time.Time { v := listNow.Add(d); return &v }
	rows := []dbRow{{id: "expired"}, {id: "at-now"}, {id: "later"}, {id: "never"}, {id: "other-subject"}}
	acl := []ACLRow{
		{RType: rt.Type, RID: "expired", Relation: "viewer", Subject: "user:u_me", ExpiresAt: at(-time.Second)},
		{RType: rt.Type, RID: "at-now", Relation: "viewer", Subject: "user:u_me", ExpiresAt: at(0)},
		{RType: rt.Type, RID: "later", Relation: "viewer", Subject: "dept_tree:/", ExpiresAt: at(time.Second)},
		{RType: rt.Type, RID: "never", Relation: "viewer", Subject: "dept:/1/"},
		{RType: rt.Type, RID: "other-subject", Relation: "viewer", Subject: "user:u_other"},
		{RType: "t.k.other", RID: "never", Relation: "viewer", Subject: "user:u_me"},
	}
	require.Equal(t, []string{"later", "never"}, assertListMatchesCan(t, env, e, rt, "t.k.view", rows, acl))
}

func TestListWithoutDepartmentSeesNoDepartmentRows(t *testing.T) {
	env := newDBEnv(t)
	rt := mustType(t, ownerOrgType)
	rows := []dbRow{
		{id: "mine", owner: sp("u_me"), dept: sp("/1/")},
		{id: "root-child", owner: sp("u_x"), dept: sp("/1/")},
		{id: "empty-dept", owner: sp("u_x"), dept: sp("")},
		{id: "null-dept", owner: sp("u_x")},
		{id: "null-owner", dept: sp("/2/")},
	}
	for _, dept := range []string{"", "1/", "/1"} {
		e := NewEvaluator(testBundle(t, "subtree", ""), Token{Sub: "u_me", Roles: []string{"rep"}, DeptPath: dept}, listNow)
		require.Equal(t, []string{"mine"}, assertListMatchesCan(t, env, e, rt, "t.k.view", rows, nil), "dept %q is no department (R60)", dept)
	}
	root := NewEvaluator(testBundle(t, "subtree", ""), Token{Sub: "u_me", Roles: []string{"rep"}, DeptPath: "/"}, listNow)
	require.Equal(t, []string{"mine", "null-owner", "root-child"}, assertListMatchesCan(t, env, root, rt, "t.k.view", rows, nil),
		"/ is the whole tree, never a row without a department")
	dept := NewEvaluator(testBundle(t, "dept", ""), Token{Sub: "u_me", Roles: []string{"rep"}, DeptPath: "/1/"}, listNow)
	require.Equal(t, []string{"mine", "root-child"}, assertListMatchesCan(t, env, dept, rt, "t.k.view", rows, nil))
}

func TestListTypeWithoutDimensionsNeedsTheKey(t *testing.T) {
	env := newDBEnv(t)
	rt := mustType(t, `{"type":"t.k.plain","owner_component":"t/k","view_key":"t.k.view","derivation":"direct",
		"relations":{"viewer":{"grants":["t.k.view"]}}}`)
	rows := []dbRow{{id: "a"}, {id: "b"}}
	holder := NewEvaluator(testBundle(t, "own", ""), Token{Sub: "u_me", Roles: []string{"rep"}}, listNow)
	require.Equal(t, []string{"a", "b"}, assertListMatchesCan(t, env, holder, rt, "t.k.view", rows, nil))
	stranger := NewEvaluator(testBundle(t, "own", ""), Token{Sub: "u_me"}, listNow)
	acl := []ACLRow{{RType: rt.Type, RID: "b", Relation: "viewer", Subject: "user:u_me"}}
	require.Equal(t, []string{"b"}, assertListMatchesCan(t, env, stranger, rt, "t.k.view", rows, acl))
}

func TestListBranchesAreDisjoint(t *testing.T) {
	env := newDBEnv(t)
	rt := mustType(t, `{"type":"t.k.folder","owner_component":"t/k","view_key":"t.k.view","dimensions":["owner"],"derivation":"graph",
		"relations":{"viewer":{"grants":["t.k.view"]}}}`)
	e := NewEvaluator(testBundle(t, "own", ""), Token{Sub: "u_me", Roles: []string{"rep"}}, listNow)
	rows := []dbRow{{id: "all-three", owner: sp("u_me")}, {id: "acl-and-graph", owner: sp("u_x")}, {id: "graph-only"}, {id: "none"}}
	acl := []ACLRow{{RType: rt.Type, RID: "all-three", Relation: "viewer", Subject: "user:u_me"},
		{RType: rt.Type, RID: "acl-and-graph", Relation: "viewer", Subject: "role:rep"}}
	env.reset(t, rows, acl)
	ka := e.Key(rt, "t.k.view", func(string) []string { return []string{"all-three", "acl-and-graph", "graph-only"} })
	p, err := NewPredicate(rt, itemColumns)
	require.NoError(t, err)
	want := []string{"acl-and-graph", "all-three", "graph-only"}
	require.Equal(t, want, env.list(t, p, ka.Params, listNow))
	require.Equal(t, want, env.listBranches(t, p, ka.Params, listNow), "a row matching several branches is listed once")
	require.Equal(t, want, visibleIDs(e, rt, ka, rows, acl))
}
