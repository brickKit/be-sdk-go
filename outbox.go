package besdk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// PublishOutbox 在同一事务里把事件写进 event_outbox（§3.10 Outbox Pattern）。
//
// ⚠️ 生产者必须走这条路，不许直接往 NATS publish——那样业务变更与事件发布
// 不在同一事务里，进程崩在两者之间就是「改了但没发」或「发了但没改」。
// 表结构见 §11.2.2（本函数假定表已经建好，不负责建表）。
func PublishOutbox(tx *sql.Tx, schema string, ev Event) error {
	if !identRe.MatchString(schema) {
		return fmt.Errorf("非法 schema 名：%q", schema)
	}
	_, err := tx.Exec(fmt.Sprintf(`
		INSERT INTO %s.event_outbox
			(subject, aggregate_id, version, trace_id, causation_id, hop_count, payload)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, schema),
		ev.Subject, ev.AggregateID, ev.Version, ev.TraceID, ev.CausationID, ev.HopCount, ev.Payload)
	if err != nil {
		return fmt.Errorf("写 event_outbox: %w", err)
	}
	return nil
}

const outboxPollInterval = 200 * time.Millisecond

// StartOutboxPump 起后台推送线程，轮询 outbox 发往 NATS。
//
// ⚠️ 这是 Module.Start 的典型用法——必须接 ctx，cancel 时返回，不许自己装
// 信号处理器（§13.3 铁律七）。
//
// 发送成功立刻标记 PUBLISHED；发送失败只累加 attempts、状态留在 PENDING
// 等下一轮重试——NATS 抖动不该让事件永久丢失，也不该让 pump 自己崩掉。
func StartOutboxPump(ctx context.Context, db *sql.DB, schema string, nc *nats.Conn) error {
	if !identRe.MatchString(schema) {
		return fmt.Errorf("非法 schema 名：%q", schema)
	}
	ticker := time.NewTicker(outboxPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := pumpOnce(ctx, db, schema, nc); err != nil {
				// ⚠️ 实测发现：ctx 到期的时刻可能恰好撞上 ticker 触发，
				// select 语句对多个已就绪的 case 是随机挑选的，不保证
				// 优先选 ctx.Done()。这种情况下 pumpOnce 会带着一个已经
				// / 即将过期的 ctx 去查库，返回 context 相关错误——这是
				// 正常关停的一种表现形式，不是 pump 真的坏了，不能当
				// 硬错误往上抛。
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("outbox pump: %w", err)
			}
		}
	}
}

// pumpOnce 处理一批 PENDING 事件。失败的那一条只累加 attempts，不中断
// 整批——一条坏数据不该卡住同一批里的其他事件。
func pumpOnce(ctx context.Context, db *sql.DB, schema string, nc *nats.Conn) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, subject, aggregate_id, version, trace_id, causation_id, hop_count, payload
		FROM %s.event_outbox
		WHERE status = 'PENDING' ORDER BY id LIMIT 100`, schema))
	if err != nil {
		return err
	}
	type pending struct {
		id          int64
		subject     string
		aggregateID string
		version     int64
		traceID     string
		causationID string
		hopCount    int
		payload     []byte
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.subject, &p.aggregateID, &p.version,
			&p.traceID, &p.causationID, &p.hopCount, &p.payload); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, p := range batch {
		// ⚠️ trace_id/causation_id/hop_count 走 NATS Header，不折进
		// payload——消费侧的因果链防环靠的就是这几个 Header（§3.10、
		// 决策 42），折进 payload 就等于让每个消费者自己去解析信封，
		// 违背"业务代码只关心业务字段"这条。
		msg := &nats.Msg{
			Subject: p.subject,
			Data:    p.payload,
			Header:  nats.Header{},
		}
		msg.Header.Set(headerAggregateID, p.aggregateID)
		msg.Header.Set(headerVersion, fmt.Sprint(p.version))
		msg.Header.Set(headerTraceID, p.traceID)
		msg.Header.Set(headerCausationID, p.causationID)
		msg.Header.Set(headerHopCount, fmt.Sprint(p.hopCount))

		if err := nc.PublishMsg(msg); err != nil {
			if _, uerr := db.ExecContext(ctx, fmt.Sprintf(
				`UPDATE %s.event_outbox SET attempts = attempts + 1, updated_at = now() WHERE id = $1`, schema),
				p.id); uerr != nil {
				return uerr
			}
			continue
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			`UPDATE %s.event_outbox SET status = 'PUBLISHED', published_at = now(), updated_at = now() WHERE id = $1`, schema),
			p.id); err != nil {
			return err
		}
	}
	return nil
}
