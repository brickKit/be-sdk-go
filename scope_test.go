package besdk

import (
	"context"
	"strings"
	"testing"
)

func ctxWithClaims(c *Claims) context.Context {
	return context.WithValue(context.Background(), claimsCtxKey{}, c)
}

// TestScopeOf_按JWT字段填三个可选字段 是 §14.2.4 的核心断言：五档不是
// ScopeOf 自己判断出来的，Prefix/Exact/Owner 始终从同一份 Claims 填，
// 调用方（某条 .sql 查询）自己决定用哪一个。
func TestScopeOf_按JWT字段填三个可选字段(t *testing.T) {
	ctx := ctxWithClaims(&Claims{Sub: "u_zhangsan", DeptPath: "/root/china/east/sh-sales"})
	f := ScopeOf(ctx)
	if f.All {
		t.Fatalf("dept_path 非空时不该是 All，实际 %+v", f)
	}
	if f.Prefix != "/root/china/east/sh-sales" {
		t.Fatalf("Prefix 应该等于 dept_path，实际 %+v", f)
	}
	if f.Exact != "/root/china/east/sh-sales" {
		t.Fatalf("Exact 应该等于 dept_path，实际 %+v", f)
	}
	if f.Owner != "u_zhangsan" {
		t.Fatalf("Owner 应该等于 sub，实际 %+v", f)
	}
}

// TestScopeOf_dept_path为空时org维落空只剩本人 替换了旧用例
// TestScopeOf_部门树根节点自然得到All——旧用例锁定的是 fail-open：authz
// 签发的真实路径恒为 /<id>/…/，根部门也是 /<根id>/，dept_path 为空只有
// "这个人没分部门"一种来源，旧语义把它当成"不限"，于是 Prefix == "" 在
// LIKE ” || '%' 和 strings.HasPrefix 里都匹配一切。现在断言：空路径不是
// All，Prefix 不命中任何真实路径、也不命中 dept_path = ” 的行，Owner 照旧。
func TestScopeOf_dept_path为空时org维落空只剩本人(t *testing.T) {
	ctx := ctxWithClaims(&Claims{Sub: "u_x", DeptPath: ""})
	f := ScopeOf(ctx)
	if f.All {
		t.Fatalf("dept_path 为空不该得到 All，实际 %+v", f)
	}
	if strings.HasPrefix("/1/12/", f.Prefix) {
		t.Fatalf("dept_path 为空时 Prefix 不该命中真实路径 /1/12/，实际 %+v", f)
	}
	if strings.HasPrefix("", f.Prefix) {
		t.Fatalf("dept_path 为空时 Prefix 不该命中 dept_path = '' 的行，实际 %+v", f)
	}
	if f.Exact == "" {
		t.Fatalf("dept_path 为空时 Exact 不该是空串（会命中 dept_path = '' 的行），实际 %+v", f)
	}
	if f.Owner != "u_x" {
		t.Fatalf("Owner 应该照旧等于 sub，实际 %+v", f)
	}
	if f.HasDept {
		t.Fatalf("dept_path 为空时 HasDept 应该为假，实际 %+v", f)
	}
	if f.Prefix != NoDeptPath || f.Exact != NoDeptPath {
		t.Fatalf("dept_path 为空时 Prefix/Exact 应该都是哨兵 %q，实际 %+v", NoDeptPath, f)
	}
}

// TestScopeOf_斜杠是整棵树的显式根标记："/" 是所有真实路径的公共前缀，
// 作为普通前缀天然匹配整片森林（多个顶层部门），是"看全部"唯一的写法。
func TestScopeOf_斜杠是整棵树的显式根标记(t *testing.T) {
	f := ScopeOf(ctxWithClaims(&Claims{Sub: "u_hq", DeptPath: "/"}))
	if !f.All || !f.HasDept || f.Prefix != "/" || f.Exact != "/" {
		t.Fatalf(`dept_path 为 "/" 应该得到 All、HasDept、Prefix=Exact="/"，实际 %+v`, f)
	}
	if !strings.HasPrefix("/1/12/", f.Prefix) {
		t.Fatalf(`"/" 应该前缀命中真实路径 /1/12/，实际 %+v`, f)
	}
	if strings.HasPrefix("", f.Prefix) {
		t.Fatalf(`"/" 不该命中 dept_path = '' 的行（没有部门的行只对 owner 可见），实际 %+v`, f)
	}
	if f.Owner != "u_hq" {
		t.Fatalf("Owner 应该等于 sub，实际 %+v", f)
	}
}

// TestScopeOf_不以斜杠开头的dept_path按无部门处理：格式异常的路径前缀
// 语义不可预期（"1" 会前缀命中 "12/…"），一律当成没有部门，fail-closed。
func TestScopeOf_不以斜杠开头的dept_path按无部门处理(t *testing.T) {
	for _, dp := range []string{"1/12/", "!no-dept", "%", " /1/"} {
		f := ScopeOf(ctxWithClaims(&Claims{Sub: "u_y", DeptPath: dp}))
		if f.All || f.HasDept || f.Prefix != NoDeptPath || f.Exact != NoDeptPath || f.Owner != "u_y" {
			t.Fatalf("dept_path=%q 应该按无部门处理（!All、!HasDept、Prefix=Exact=哨兵、Owner 照旧），实际 %+v", dp, f)
		}
	}
}

// TestScopeOf_真实部门路径HasDept为真：普通路径 HasDept 为真、All 为假。
func TestScopeOf_真实部门路径HasDept为真(t *testing.T) {
	f := ScopeOf(ctxWithClaims(&Claims{Sub: "u_z", DeptPath: "/1/12/"}))
	if !f.HasDept || f.All || f.Prefix != "/1/12/" || f.Exact != "/1/12/" {
		t.Fatalf("真实路径应该 HasDept、!All、Prefix=Exact=原值，实际 %+v", f)
	}
}

// TestNoDeptPath_不以斜杠开头且不含LIKE通配符：哨兵必须在
// `dept_path LIKE prefix || '%'`、`dept_path = exact` 与 strings.HasPrefix
// 里对任何真实路径（恒以 "/" 开头）和空串都落空——它不能以 "/" 开头，
// 不能含 LIKE 的通配符 % _ 和默认转义符 \，也不能是空串。
func TestNoDeptPath_不以斜杠开头且不含LIKE通配符(t *testing.T) {
	if NoDeptPath == "" {
		t.Fatal("哨兵不能是空串：空串在 LIKE '' || '%' 里匹配一切")
	}
	if strings.HasPrefix(NoDeptPath, "/") {
		t.Fatalf("哨兵 %q 不能以 / 开头：那会和真实路径的前缀重叠", NoDeptPath)
	}
	if strings.ContainsAny(NoDeptPath, `%_\`) {
		t.Fatalf("哨兵 %q 不能含 LIKE 通配符 %% _ 或转义符 \\", NoDeptPath)
	}
	for _, real := range []string{"/", "/1/", "/1/12/", ""} {
		if strings.HasPrefix(real, NoDeptPath) {
			t.Fatalf("哨兵 %q 不该前缀命中 %q", NoDeptPath, real)
		}
	}
}

// TestScopeOf_ctx里没有Claims时panic 是本次实现最容易被反过来搞错方向
// 的一处：§14.2.4 的 SQL 约定是"空字符串表示不限"，如果这里改成返回
// 零值 ScopeFilter{}，等于把"取不到身份"解读成"放行一切"——方向反了，
// 是 fail-open 不是 fail-closed。必须 panic，不能安静地返回一个看似
// 收紧、实际在下游 SQL 里被解读成不限的值。
func TestScopeOf_ctx里没有Claims时panic(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("ctx 里没有 Claims 时 ScopeOf 应该 panic，不该安静地返回一个值")
		}
	}()
	ScopeOf(context.Background())
}

// TestContextWithClaims_供业务组件单元测试构造ctx 是阶段三 Task 6 写
// erp-inventory 的 service 层测试时发现的真实缺口的直接测试：claimsCtxKey
// 是包内私有类型，在此之前业务组件的 _test.go 没有任何公开 API 能构造出
// ScopeOf 认得的 ctx。ContextWithClaims 就是补的这条口子——断言它产出的
// ctx 喂给 ScopeOf 得到的结果，与包内私有的 ctxWithClaims 完全一致
// （两条路径必须是同一件事，不能有第二套"稍微不一样"的语义）。
func TestContextWithClaims_供业务组件单元测试构造ctx(t *testing.T) {
	claims := Claims{Sub: "u_lisi", DeptPath: "/root/china/south"}
	ctx := ContextWithClaims(context.Background(), claims)

	f := ScopeOf(ctx)
	want := ScopeOf(ctxWithClaims(&claims))
	if f.All != want.All || f.Prefix != want.Prefix || f.Exact != want.Exact || f.Owner != want.Owner {
		t.Fatalf("ContextWithClaims 产出的 ctx 喂给 ScopeOf 应该与包内 ctxWithClaims 结果一致，"+
			"实际 %+v，期望 %+v", f, want)
	}
	if f.Owner != "u_lisi" || f.Prefix != "/root/china/south" {
		t.Fatalf("ScopeOf(ContextWithClaims(...)) 应该真的按 Claims 字段填，实际 %+v", f)
	}
}
