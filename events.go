package besdk

import (
	"context"
	"database/sql"

	"github.com/nats-io/nats.go"
)

// Event 是事件的统一信封。Header 里的三个字段由 SDK 自动填/校验（§3.10）。
type Event struct {
	Subject     string            // {domain}.{aggregate}.{action}.v{n}
	AggregateID string
	Version     int64             // 消费侧只允许严格大于本地当前值才更新（§3.10）
	TraceID     string
	CausationID string
	HopCount    int               // > 5 直接丢弃进 DLQ
	Payload     []byte
	Headers     map[string]string
}

// Consume 注册幂等消费者：自动做 inbox 去重、hop_count 防环、version 单调校验。
//
// ⚠️ 守的是 §3.10 事件纪律与 §4.6 幂等性铁律——业务代码里只写 fn 的内容，
// 去重/防环/单调校验全部由这一层负责，不许业务代码自己再判一遍。
//
// 实现放 Task 7 用 TDD 补（SOP-W W-1：契约先于测试）。
func Consume(ctx context.Context, nc *nats.Conn, db *sql.DB, schema, subject string,
	fn func(context.Context, *sql.Tx, Event) error) error {
	panic("未实现：Task 7 补")
}
