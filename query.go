package besdk

// Query 是 List 类查询的参数信封，字段留给 Task 7 实现时按需扩（时间窗口、
// cursor、排序、过滤）。现在只钉签名，不钉字段——字段属于「横切函数的输入
// 形状」，不属于「结构三件套」，改起来代价小得多。
type Query struct {
	// 留空，Task 7 补
}

// ListWindow 给 List 查询自动注入时间窗口（默认最近 90 天）与 Cursor 分页
// （§11.4）。
//
// ⚠️ 守的是决策 53：List 契约里没有 offset 字段，深分页在契约层面就不可
// 表达；业务代码里永远只写 `SELECT * FROM sales_orders`，时间窗口与游标
// 由这一层注入，不许业务代码自己拼。
//
// 实现放 Task 7 用 TDD 补。
func ListWindow(q Query) Query {
	panic("未实现：Task 7 补")
}
