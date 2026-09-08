package besdk

import (
	"context"
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

// TestScopeOf_部门树根节点自然得到All 是"五档退化成纯函数"这条设计的
// 直接验证：坐在根部门（dept_path 为空）的人，前缀匹配天然覆盖全部，
// 不需要任何特判分支。
func TestScopeOf_部门树根节点自然得到All(t *testing.T) {
	ctx := ctxWithClaims(&Claims{Sub: "u_ceo", DeptPath: ""})
	f := ScopeOf(ctx)
	if !f.All {
		t.Fatalf("dept_path 为空应该得到 All:true，实际 %+v", f)
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
