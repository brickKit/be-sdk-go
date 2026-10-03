package authz

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	placeholder     = regexp.MustCompile(`\$[0-9]+`)
	castPlaceholder = regexp.MustCompile(`\$[0-9]+::`)
)

func mustType(t *testing.T, raw string) *ResourceType {
	t.Helper()
	rt, err := ParseResourceType([]byte(raw))
	require.NoError(t, err)
	return rt
}

const orderType = `{"type":"erp.sales.order","owner_component":"erp/sales","view_key":"erp.sales.view",
	"dimensions":["owner","org","warehouse"],"derivation":"direct",
	"relations":{"viewer":{"grants":["erp.sales.view"]},"editor":{"grants":["erp.sales.ship"],"includes":["viewer"]}}}`

func orderColumns() Columns {
	return Columns{Alias: "o", ID: "id", Owner: "owner_id", DeptPath: "dept_path", Dims: map[string]string{"warehouse": "warehouse_id"}}
}

func TestPredicateIsStaticAndParameterised(t *testing.T) {
	p, err := NewPredicate(mustType(t, orderType), orderColumns())
	require.NoError(t, err)
	now := time.Unix(1760000000, 0).UTC()
	hostile := ScopeParams{Owners: []string{"x'); DROP TABLE o; --"}, DeptExact: []string{"/'/"}, DeptPrefix: []string{"/1/%"},
		Dims: map[string]DimParam{"warehouse": {IDs: []string{"' OR 1=1 --"}}}, ACL: true, Relations: []string{"viewer"},
		Subjects: []string{"user:'"}, GraphIDs: []string{"'"}}
	sql1, args1 := p.SQL(hostile, now, 1)
	sql2, args2 := p.SQL(ScopeParams{All: true, Dims: map[string]DimParam{}}, now, 1)
	require.Equal(t, sql1, sql2, "the text never depends on the values (P6.5)")
	require.Len(t, args1, len(args2))
	require.NotContains(t, sql1, "DROP")
	require.NotContains(t, sql1, "'", "no literal at all: the resource type is a parameter too")
	require.Contains(t, sql1, "o.owner_id::text = ANY($2::text[])")
	require.Contains(t, sql1, "o.dept_path LIKE ANY($4::text[])")
	require.Contains(t, sql1, "o.warehouse_id::text = ANY($6::text[])")
	require.Contains(t, sql1, "FROM besdk_authz_acl")
	require.Equal(t, len(placeholder.FindAllString(sql1, -1)), len(castPlaceholder.FindAllString(sql1, -1)), "every placeholder carries a cast")
	require.Equal(t, []string{"' OR 1=1 --"}, args1[5])
	require.Equal(t, false, args2[4], "a missing dimension entry means nothing matches")
	require.Equal(t, []string{}, args2[5])
}

func TestPredicatePlaceholdersStartAtFirst(t *testing.T) {
	p, err := NewPredicate(mustType(t, orderType), orderColumns())
	require.NoError(t, err)
	sql, args := p.SQL(ScopeParams{}, time.Now(), 3)
	require.NotContains(t, sql, "$1:")
	require.NotContains(t, sql, "$2:")
	require.Contains(t, sql, "$3::boolean")
	require.Contains(t, sql, "$"+strconv.Itoa(3+len(args)-1)+"::")
	require.NotContains(t, sql, "$"+strconv.Itoa(3+len(args))+"::")
}

func TestPredicateWithoutIdentityDimensionsRequiresTheKey(t *testing.T) {
	rt := mustType(t, `{"type":"a.b.c","owner_component":"a/b","view_key":"a.b.view","relations":{},"derivation":"direct"}`)
	p, err := NewPredicate(rt, Columns{Alias: "t", ID: "id"})
	require.NoError(t, err)
	sql, _ := p.SQL(ScopeParams{}, time.Now(), 1)
	require.Contains(t, sql, "cardinality($", "has(K) is s_owners non-empty for a type with neither owner nor org (E10)")
}

func TestPredicateRejectsBadColumns(t *testing.T) {
	rt := mustType(t, orderType)
	for name, c := range map[string]Columns{
		"no id":                     {Alias: "o", Owner: "owner_id", DeptPath: "dept_path", Dims: map[string]string{"warehouse": "w"}},
		"owner declared, no column": {Alias: "o", ID: "id", DeptPath: "dept_path", Dims: map[string]string{"warehouse": "w"}},
		"org declared, no column":   {Alias: "o", ID: "id", Owner: "owner_id", Dims: map[string]string{"warehouse": "w"}},
		"dimension without column":  {Alias: "o", ID: "id", Owner: "owner_id", DeptPath: "dept_path"},
		"injected identifier":       {Alias: "o", ID: "id; DROP TABLE x", Owner: "owner_id", DeptPath: "dept_path", Dims: map[string]string{"warehouse": "w"}},
		"bad alias":                 {Alias: "o o", ID: "id", Owner: "owner_id", DeptPath: "dept_path", Dims: map[string]string{"warehouse": "w"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewPredicate(rt, c)
			require.Error(t, err)
		})
	}
}

func TestPredicateBranchesAreThree(t *testing.T) {
	p, err := NewPredicate(mustType(t, orderType), orderColumns())
	require.NoError(t, err)
	frs := p.Branches(ScopeParams{}, time.Now(), 1)
	require.Len(t, frs, 3)
	require.Contains(t, frs[1].SQL, "NOT COALESCE(", "the ACL branch excludes rows of the rule branch (UNION ALL)")
	require.Equal(t, 2, strings.Count(frs[2].SQL, "NOT COALESCE("), "the graph branch excludes both others")
	for _, f := range frs {
		require.Equal(t, len(placeholder.FindAllString(f.SQL, -1)), len(castPlaceholder.FindAllString(f.SQL, -1)))
	}
}

func TestScopeParamsBranches(t *testing.T) {
	p := ScopeParams{All: true, Owners: []string{"u"}, DeptExact: []string{"/1/"}, DeptPrefix: []string{"/1/%"},
		Dims: map[string]DimParam{"w": {IDs: []string{"S"}}}, ACL: true, Relations: []string{"viewer"},
		Subjects: []string{"user:u"}, GraphIDs: []string{"g1"}}
	b := p.Branches()
	require.Len(t, b, 3)
	require.True(t, b[0].All)
	require.False(t, b[0].ACL)
	require.Empty(t, b[0].GraphIDs)
	require.False(t, b[1].All)
	require.Empty(t, b[1].Owners)
	require.True(t, b[1].ACL)
	require.Empty(t, b[1].GraphIDs)
	require.False(t, b[2].All)
	require.False(t, b[2].ACL)
	require.Equal(t, []string{"g1"}, b[2].GraphIDs)
	b[0].Dims["w"].IDs[0] = "X"
	require.Equal(t, "S", p.Dims["w"].IDs[0], "branches never alias the original")
}

func TestLikeMatch(t *testing.T) {
	for _, c := range []struct {
		s, pat string
		want   bool
	}{
		{"/1/12/5/", DeptPrefix("/1/12/"), true},
		{"/1/12/", DeptPrefix("/1/12/"), true},
		{"/1/123/", DeptPrefix("/1/12/"), false},
		{"/a_b/", DeptPrefix("/a_b/"), true},
		{"/aXb/", DeptPrefix("/a_b/"), false},
		{"/c%d/x/", DeptPrefix("/c%d/"), true},
		{"/cZZd/", DeptPrefix("/c%d/"), false},
		{`/a\b/`, DeptPrefix(`/a\b/`), true},
		{"", DeptPrefix("/"), false},
		{"/x/", DeptPrefix("/"), true},
		{"abc", "a_c", true},
		{"abc", "a%", true},
		{"ab", "a_c", false},
	} {
		require.Equal(t, c.want, likeMatch(c.s, c.pat), "%q LIKE %q", c.s, c.pat)
	}
	require.Equal(t, `/a\_b/c\%d/\\/%`, DeptPrefix(`/a_b/c%d/\/`))
}

func TestDeptHelpers(t *testing.T) {
	require.Equal(t, []string{"/", "/1/", "/1/12/"}, DeptAncestors("/1/12/"))
	require.Equal(t, []string{"/"}, DeptAncestors("/"))
	require.True(t, ValidDept("/"))
	require.True(t, ValidDept("/1/12/"))
	for _, bad := range []string{"", "1/12", "/1/12", "//", "/1//"} {
		require.False(t, ValidDept(bad), bad)
	}
}

func TestLevelNames(t *testing.T) {
	for _, n := range []string{"own", "dept", "subtree", "all"} {
		l, ok := ParseLevel(n)
		require.True(t, ok)
		require.Equal(t, n, l.String())
	}
	_, ok := ParseLevel("none")
	require.False(t, ok, "none is not a grantable level")
	require.Equal(t, "none", LevelNone.String())
	require.True(t, LevelOwn < LevelDept && LevelDept < LevelSubtree && LevelSubtree < LevelAll)
}

func TestFieldAccessHelpers(t *testing.T) {
	f := FieldAccess{Masked: []string{"discount", "unit_price"}, ReadOnly: []string{"cost"}}
	require.True(t, f.IsMasked("unit_price"))
	require.False(t, f.IsMasked("cost"))
	require.False(t, f.IsWritable("unit_price"), "a masked field is never written (FIELD_FORBIDDEN)")
	require.False(t, f.IsWritable("cost"), "read-only")
	require.True(t, f.IsWritable("qty"))
	require.Equal(t, "discount", f.FirstUnwritable([]string{"qty", "discount", "cost"}))
	require.Equal(t, "", f.FirstUnwritable([]string{"qty"}))
}

func TestNeedsGraph(t *testing.T) {
	b, err := ParseBundle([]byte(`{"contract":"authz/2.0","revision":"1","catalog_digest":"sha256:` + zeros64 + `",
		"capabilities":{"core":true,"graph":true,"delegation":true},"roles":{},"grants":{},"stale_since":{},
		"profiles":{"p":{"keys":["t.k.view"],"max_level":"own","relations":[]}}}`))
	require.NoError(t, err)
	graphType := mustType(t, `{"type":"t.k.folder","owner_component":"t/k","view_key":"t.k.view","derivation":"graph","relations":{}}`)
	direct := mustType(t, `{"type":"t.k.item","owner_component":"t/k","view_key":"t.k.view","derivation":"direct","relations":{}}`)
	e := NewEvaluator(b, Token{Sub: "u"}, time.Now())
	require.True(t, e.NeedsGraph(graphType, "t.k.view"))
	require.False(t, e.NeedsGraph(direct, "t.k.view"))
	ceiled := NewEvaluator(b, Token{Sub: "u", Ceil: []string{"p"}}, time.Now())
	require.False(t, ceiled.NeedsGraph(graphType, "t.k.edit"), "a key the ceilings drop has no graph branch (E4)")
	require.True(t, ceiled.NeedsGraph(graphType, "t.k.view"))
}
