package besdk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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

// outboxClaimTimeout：一行认领成 SENDING 之后卡在这个状态超过这个时长，
// 下一轮当作认领它的那个实例已经不在了（进程崩溃/被杀），重新可被认领。
// 见 pumpOnce 顶部注释。
const outboxClaimTimeout = 30 * time.Second

// StartOutboxPump 起后台推送线程，轮询 outbox 发往 NATS。
//
// ⚠️ 这是 Module.Start 的典型用法——必须接 ctx，cancel 时返回，不许自己装
// 信号处理器（§13.3 铁律七）。
//
// 每一批先原子认领（转 SENDING，见 pumpOnce）再逐条发布：发送成功立刻
// 标记 PUBLISHED；发送失败转回 PENDING、累加 attempts，等下一轮重试——
// NATS 抖动不该让事件永久丢失，也不该让 pump 自己崩掉。
//
// ⚠️ 实测踩坑：pumpOnce 查询数据库失败（连接抖动/短暂不可用）曾经被当作
// 硬错误直接向上返回——StartOutboxPump 整个循环退出，Module.Start 的
// errCh 收到错误，RunStandalone 把整个进程带崩，Docker 重启容器又立刻
// 撞到同一个还没恢复的连接，陷入几百毫秒一次的重启死循环，直到数据库
// 恢复；这段时间里 HTTP/gRPC 完全没人能连，不是"降级"是整个容器反复
// 重启。这与 partition.go 的 Start 不一致（那边失败只记日志、留到下一轮
// 重试）。现在对齐同一套容错方式：一次 pumpOnce 失败只记日志，循环继续，
// 只有 ctx 取消才真正返回。
func StartOutboxPump(ctx context.Context, db *sql.DB, schema string, nc *nats.Conn, logger *slog.Logger) error {
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
				// 硬错误往上抛，也不用当成故障记日志。
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					return nil
				}
				logger.Error("outbox pump 单轮失败", "error", err)
			}
		}
	}
}

// pumpOnce 原子认领一批事件并发布。失败的那一条只累加 attempts、转回
// PENDING，不中断整批——一条坏数据不该卡住同一批里的其他事件。
//
// ⚠️ 实测踩坑：早期版本这里是一条裸 SELECT ... WHERE status='PENDING'，
// 读到就直接发，读和"认领"之间没有任何互斥手段。只要同一张 event_outbox
// 物理表在某个时间窗口内被两个 pump 实例同时轮询到——K8s 滚动重启新旧
// 两个副本重叠、或者同一个组件将来被扩到多副本——两边会读到同一批
// PENDING 行，各自真的调一次 nc.PublishMsg，把同一个事件真实发布两遍到
// NATS：这一步双方都不会报错，等两边各自把状态改成 PUBLISHED 时 SQL 层
// 面同样不会报任何约束冲突，是完全静默的重复投递。这颗雷在测试套件全部
// 串行跑、每个组件永远只有一个 pump 实例的现状下从未被真正触发过，是在
// 评估"给测试开 t.Parallel()"是否安全时顺带查出来的。
//
// 现在改成 UPDATE ... FOR UPDATE SKIP LOCKED 的原子认领：子查询里锁住这
// 一批行，另一个并发实例对同一行的 FOR UPDATE 会直接跳过去抢下一行，不
// 会等锁也不会抢到重复的行。认领成功先转 SENDING 而不是直接发布完才改
// 状态，是为了让"认领"这个动作本身在数据库侧一次性原子完成。
//
// 认领之后、发布完成之前如果进程崩溃（例如认领的那个实例被杀），这一批
// 会卡在 SENDING——所以认领条件里同时接纳"超过 outboxClaimTimeout 还没
// 结束的 SENDING"，靠这个超时兜底重新认领，不会让事件永久卡住（Outbox
// 的核心承诺是至少一次投递，卡住等于悄悄丢事件，比重复投递更违背这个
// 承诺）。
func pumpOnce(ctx context.Context, db *sql.DB, schema string, nc *nats.Conn) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
		UPDATE %[1]s.event_outbox
		SET status = 'SENDING', updated_at = now()
		WHERE id IN (
			SELECT id FROM %[1]s.event_outbox
			WHERE status = 'PENDING'
			   OR (status = 'SENDING' AND updated_at < now() - interval '%[2]d seconds')
			ORDER BY id
			LIMIT 100
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, subject, aggregate_id, version, trace_id, causation_id, hop_count, payload`,
		schema, int(outboxClaimTimeout.Seconds())))
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
			// 转回 PENDING，不是留在 SENDING——已经认领过一轮，留在
			// SENDING 只能靠 outboxClaimTimeout 超时才能重新被认领，
			// 白白多等一轮；发布失败是已知结果，没有理由不立刻还回去。
			if _, uerr := db.ExecContext(ctx, fmt.Sprintf(
				`UPDATE %s.event_outbox SET status = 'PENDING', attempts = attempts + 1, updated_at = now() WHERE id = $1`, schema),
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
