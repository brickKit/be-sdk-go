package besdk

import (
	"context"
	"strings"
)

// NoDeptPath 是"调用者没有部门"时 ScopeFilter.Prefix/Exact 取的哨兵值（R60）。
//
// authz 签发的真实 dept_path 恒为 /<id>/…/（根部门也是 /<根id>/），所以
// dept_path 为空只有一种来源：这个人还没被分到任何部门。v0.4.0 及以前
// 把空串当成"不限"，Prefix == "" 在 `dept_path LIKE ” || '%'` 和
// strings.HasPrefix(x, "") 里都匹配一切——没分部门的新账号看得到全部，
// 是 fail-open。
//
// 选一个哨兵值而不只是加 HasDept 字段，是为了值层面 fail-closed：它不以
// "/" 开头、不含 LIKE 的通配符 % _ 和转义符 \，于是现有的
// `LIKE prefix || '%'`、`= exact`、strings.HasPrefix 不改一行就对任何真实
// 路径和空串都落空——没来得及改代码的消费者、在外壳里被 MVS 抬到新 SDK
// 的旧成员也自动收紧。⚠️ 它只用来比较，绝不能写进行里（建单快照之类要先
// 看 HasDept，没有部门就写空串）。
const NoDeptPath = "!no-dept"

// ScopeFilter 是查仓储时用的数据范围过滤条件——设计书 §14.2.3 的五档
// 求解（all / dept_and_below / self_dept / self / custom）压平成一个
// 结构体，仓储方法按需要取其中的字段拼 WHERE。
//
// ⚠️ 这是一个"纯函数"的输出（§14.2.4 明文）：五档不是 ScopeOf 自己判断
// 出来的，Prefix/Exact/Owner 三个字段**始终**从同一份 JWT 的
// dept_path/sub 填，"这次查询该用哪一档"是调用方（具体某个 .sql 查询）
// 的静态选择——它只取自己关心的那个字段，其余字段的存在与否不影响它。
// 比如 erp-sales 的"我的订单"只绑 @scope_owner，"本部门及下级"只绑
// @scope_prefix，两条查询各自只用一半的返回值。
//
// dept_path 的三种取值（v0.5.0 起，R60）：
//
//	token 里的 dept_path     HasDept  All    Prefix / Exact
//	"" 或不以 "/" 开头         false    false  NoDeptPath（什么都不命中）
//	"/"                       true     true   "/"（整棵树的显式根标记）
//	"/1/12/"                  true     false  原值
//
// SDK 保证 Prefix/Exact 永不为空串：仓储层收到空串的 prefix 只可能是
// 编程错误（漏填），必须当成错误处理，不能当成"不限"。没有部门的人在
// owner OR org 判据里只剩 owner 一侧（看得到自己的行），"本部门及下级"
// 视图是空列表；看全公司是"分到根部门"这个身份，不是空串。
//
// In 字段本次（阶段三 Task 5）不填：它对应 mode: in 的"自定义列表"档
// （如 erp-inventory 的仓库维度），列表从哪来是业务组件自己的数据
// （不是 JWT 字段），要由业务组件自己的仓储层查出来后再组装，不归
// ScopeOf 管——be-sdk 没有、也不该有 erp-inventory 的表结构知识。
type ScopeFilter struct {
	All     bool     // all：dept_path 为 "/"（整棵树的显式根标记）时为真；空串不是 All
	HasDept bool     // 调用者有没有部门：dept_path 以 "/" 开头为真；为假时 Prefix/Exact 是 NoDeptPath
	Prefix  string   // dept_and_below：dept_path LIKE prefix || '%'；永不为空串
	Exact   string   // self_dept：dept_path = exact；永不为空串
	Owner   string   // self：owner_id = owner
	In      []string // custom：调用方自己填，ScopeOf 不填（见上）
}

// ScopeOf 从 ctx 取当前请求的数据范围。ctx 必须是经过
// RequirePermission 处理过的请求的 context.Context（它把验签后的
// Claims 塞了进去）——除 Public/Authenticated 外的路由，这个前提总是
// 成立。
//
// ⚠️ 取不到 Claims 时**不能**返回零值 ScopeFilter{}：零值的 Prefix/Owner
// 都是空字符串，`LIKE ” || '%'` 匹配一切，下游会解读成"放行一切"，是
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
	return scopeFromClaims(claims)
}

// scopeFromClaims 按上面的三行表求解，纯函数。
func scopeFromClaims(c *Claims) ScopeFilter {
	if !strings.HasPrefix(c.DeptPath, "/") {
		// 空串（没分部门、claim 缺失）或格式异常：org 维什么都不命中。
		return ScopeFilter{Prefix: NoDeptPath, Exact: NoDeptPath, Owner: c.Sub}
	}
	return ScopeFilter{
		All:     c.DeptPath == "/",
		HasDept: true,
		Prefix:  c.DeptPath,
		Exact:   c.DeptPath,
		Owner:   c.Sub,
	}
}
