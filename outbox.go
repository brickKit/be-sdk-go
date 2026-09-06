package besdk

import (
	"context"
	"database/sql"

	"github.com/nats-io/nats.go"
)

// PublishOutbox 在同一事务里把事件写进 event_outbox（§3.10 Outbox Pattern）。
//
// ⚠️ 生产者必须走这条路，不许直接往 NATS publish——那样业务变更与事件发布
// 不在同一事务里，进程崩在两者之间就是「改了但没发」或「发了但没改」。
//
// 实现放 Task 7 用 TDD 补。
func PublishOutbox(tx *sql.Tx, schema string, ev Event) error {
	panic("未实现：Task 7 补")
}

// StartOutboxPump 起后台推送线程，轮询 outbox 发往 NATS。
//
// ⚠️ 这是 Module.Start 的典型用法——必须接 ctx，cancel 时返回，不许自己装
// 信号处理器（§13.3 铁律七）。
//
// 实现放 Task 7 用 TDD 补。
func StartOutboxPump(ctx context.Context, db *sql.DB, schema string, nc *nats.Conn) error {
	panic("未实现：Task 7 补")
}
