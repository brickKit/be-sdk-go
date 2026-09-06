package besdk

import "log/slog"

// NewLogger 构造已注入 component_id 与 trace 上下文的结构化 JSON 日志根
// （§7.3）。Bootstrap 调用它填 Runtime.Logger，恰好一次——模块自己不许
// `logging.basicConfig()` 式的重新初始化（§12.5.2：最后一个 init 的赢，
// 22 个模块的日志格式被某个模块顶掉，而一路全绿）。
//
// ⚠️ 守三条：① 输出是合法 JSON；② 每条日志能拿到当前 span 就自动带
// trace_id/span_id；③ 大字段（payload 一类）截断到 2KB，已知敏感字段
// （手机号/身份证/银行卡/密码…）脱敏。这份敏感字段清单要跟业务一起长，
// 不是写死几个字符串就完事，所以放这里用 TDD 补，不在 Task 6 猜。
//
// 实现放 Task 7 用 TDD 补。
func NewLogger(componentID string) *slog.Logger {
	panic("未实现：Task 7 补")
}
