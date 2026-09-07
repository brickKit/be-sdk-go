package besdk

import "context"

// ScopeFilter 是查仓储时用的数据范围过滤条件——设计书 §14.2.3 的五档
// 求解（all / dept_and_below / self_dept / self / custom）压平成一个
// 结构体，仓储方法按哪个字段非零决定怎么拼 WHERE。
type ScopeFilter struct {
	All    bool     // all：不限。阶段二恒为 true
	Prefix string   // dept_and_below：dept_path LIKE prefix || '%'
	Exact  string   // self_dept：dept_path = exact
	Owner  string   // self：owner_id = owner
	In     []string // custom：dept_path LIKE ANY(...)
}

// ScopeOf 从 ctx 取当前请求的数据范围。阶段二恒返回不限——JWT 里的
// dept_path/sub 要等阶段三 infra-authz 上线才有真实的身份链路可解。
// 签名先定型，调用方现在就按它接线；阶段三只改这一个函数的实现，
// 不改调用点。
func ScopeOf(ctx context.Context) ScopeFilter {
	return ScopeFilter{All: true}
}
