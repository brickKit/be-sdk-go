package besdk

import "context"

// ScopeFilter 是查仓储时用的数据范围过滤条件——设计书 §14.2.3 的五档
// 求解（all / dept_and_below / self_dept / self / custom）压平成一个
// 结构体，仓储方法按哪个字段非零决定怎么拼 WHERE。
//
// ⚠️ 这是一个"纯函数"的输出（§14.2.4 明文）：五档不是 ScopeOf 自己判断
// 出来的，Prefix/Exact/Owner 三个字段**始终**从同一份 JWT 的
// dept_path/sub 填，"这次查询该用哪一档"是调用方（具体某个 .sql 查询）
// 的静态选择——它只取自己关心的那个字段，其余字段的存在与否不影响它。
// 比如 erp-sales 的"我的订单"只绑 @scope_owner，"本部门及下级"只绑
// @scope_prefix，两条查询各自只用一半的返回值。
// In 字段本次（阶段三 Task 5）不填：它对应 mode: in 的"自定义列表"档
// （如 erp-inventory 的仓库维度），列表从哪来是业务组件自己的数据
// （不是 JWT 字段），要由业务组件自己的仓储层查出来后再组装，不归
// ScopeOf 管——be-sdk 没有、也不该有 erp-inventory 的表结构知识。
type ScopeFilter struct {
	All    bool     // all：不限。dept_path 为空（如坐在部门树根节点）时天然成立，不是特判出来的
	Prefix string   // dept_and_below：dept_path LIKE prefix || '%'
	Exact  string   // self_dept：dept_path = exact
	Owner  string   // self：owner_id = owner
	In     []string // custom：调用方自己填，ScopeOf 不填（见上）
}

// ScopeOf 从 ctx 取当前请求的数据范围。ctx 必须是经过
// RequirePermission 处理过的请求的 context.Context（它把验签后的
// Claims 塞了进去）——除 Public/Authenticated 外的路由，这个前提总是
// 成立。
//
// ⚠️ 取不到 Claims 时**不能**返回零值 ScopeFilter{}：§14.2.4 的 SQL
// 约定是"空字符串表示不限"（`@scope_prefix = ''` 表示不限），零值的
// Prefix/Owner 都是空字符串——那会被下游 SQL 解读成"放行一切"，是
// fail-**open**，方向反了。这种调用只可能是编程错误（在 Start()/事件
// handler 里调 ScopeOf，那些地方应该用 SystemClient 且不经过
// RequirePermission，或者一个标了 Public/Authenticated 的路由却去查
// 了需要数据权限的仓储方法）——**panic**，让它在测试/联调阶段就现形，
// 而不是安静地多返回几行数据（这是全项目第三条"悄悄读到别人数据"路径
// 的同一类风险，§14.2.6）。NewGinEngine 的 recovery 中间件会把它接成
// 一次 500，不会带崩整个进程。
func ScopeOf(ctx context.Context) ScopeFilter {
	claims, ok := claimsFromContext(ctx)
	if !ok {
		panic("besdk.ScopeOf: ctx 里没有 Claims——只能在 RequirePermission 已经验过签的" +
			"请求路径上调用；Start()/事件 handler 里查数据请用 SystemClient，不经过这里")
	}
	return ScopeFilter{
		All:    claims.DeptPath == "",
		Prefix: claims.DeptPath,
		Exact:  claims.DeptPath,
		Owner:  claims.Sub,
	}
}
