package besdk

import "time"

const (
	defaultWindow = 90 * 24 * time.Hour // §11.4：List 默认最近 90 天
	defaultLimit  = 50
	maxLimit      = 500 // 上限——不许业务代码传天文数字把整张表读出来
)

// Query 是 List 类查询的参数信封。
//
// ⚠️ 刻意没有 Offset 字段——决策 53：List 契约里没有 offset，深分页在
// 契约层面就不可表达。要往后翻页，走 Cursor，不是数字偏移量。
type Query struct {
	From   time.Time // 时间窗口起点，零值表示未指定
	To     time.Time // 时间窗口终点，零值表示未指定
	Cursor string    // 游标分页，空表示第一页
	Limit  int       // 每页条数，<=0 用默认值；超过上限会被夹住
}

// ListWindow 给 List 查询自动注入时间窗口（默认最近 90 天）与合理的分页
// 上限（§11.4）。业务代码里永远只写 `SELECT * FROM sales_orders`，时间
// 窗口与游标由这一层注入，不许业务代码自己拼。
func ListWindow(q Query) Query {
	now := time.Now()
	if q.From.IsZero() {
		q.From = now.Add(-defaultWindow)
	}
	if q.To.IsZero() {
		q.To = now
	}
	switch {
	case q.Limit <= 0:
		q.Limit = defaultLimit
	case q.Limit > maxLimit:
		q.Limit = maxLimit
	}
	return q
}
