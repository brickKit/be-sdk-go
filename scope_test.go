package besdk

import (
	"context"
	"testing"
)

// TestScopeOf_阶段二恒返回不限：签名先定型，真实的按 dept_path/owner_id
// 过滤要等阶段三 infra-authz 上线、JWT 里的身份字段能被解出来才生效
// （§14.2.3）。调用方现在就按这个签名接线，阶段三只改这一个函数的实现。
func TestScopeOf_阶段二恒返回不限(t *testing.T) {
	f := ScopeOf(context.Background())
	if !f.All {
		t.Fatalf("阶段二 ScopeOf 应该恒返回 All:true，实际 %+v", f)
	}
}
