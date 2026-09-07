package besdk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

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

// NATS Header 里承载信封字段的 key。⚠️ 这几个不折进 payload——消费侧的
// 因果链防环靠的就是 Header，折进 payload 等于让每个消费者自己解析信封。
const (
	headerAggregateID = "X-Aggregate-Id"
	headerVersion      = "X-Version"
	headerTraceID      = "X-Trace-Id"
	headerCausationID  = "X-Causation-Id"
	headerHopCount     = "X-Hop-Count"
)

const maxHopCount = 5 // §3.10：> 5 直接丢弃进 DLQ

// Consume 注册幂等消费者：自动做 inbox 去重、hop_count 防环、version 单调
// 校验。业务代码只写 fn 的内容，这三件事全部由这一层负责。
//
// ⚠️ role 参数是 erp-inventory（本阶段第一个真的调用 Consume 的组件）
// 补上的：早期签名没有它，fn 拿到的 tx 只 BeginTx 过，没有切 role/
// search_path——业务代码在 fn 里写惯了的"不带 schema 前缀"的 SQL
// （同 WithTx 的约定）在这里全都会报「relation does not exist」，或者更
// 隐蔽地在错误的 role 下执行。现在 fn 收到的 tx 已经和 WithTx 给的一样
// 切过 SET LOCAL ROLE + search_path，业务代码不需要关心这里在事件消费
// 路径上还是普通请求路径上。
//
// ⚠️ 范围声明：当前实现走**普通 NATS 核心订阅**，不接 JetStream 的手动
// ack/重投——fn 返回 error 时只记日志，不会让消息重新投递。这已经满足
// 本任务列的三条断言（幂等/防环/version 单调），但完整的「at-least-once
// 送达」语义（进程崩溃时不丢消息）需要 JetStream durable consumer，
// 那是一个独立、更大的决定，留作待决问题（见 docs/design/infra-authz.md
// 同类档案的做法，回头在 be-sdk-go 自己的设计计划里补一条）。
func Consume(ctx context.Context, nc *nats.Conn, db *sql.DB, role, schema, subject string,
	fn func(context.Context, *sql.Tx, Event) error) error {

	if !identRe.MatchString(role) {
		return fmt.Errorf("非法 role 名：%q", role)
	}
	if !identRe.MatchString(schema) {
		return fmt.Errorf("非法 schema 名：%q", schema)
	}

	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		ev := eventFromMsg(msg)
		if err := handleOne(ctx, nc, db, role, schema, ev, fn); err != nil {
			// ⚠️ 只记日志，不重投（见上方范围声明）。
			slog.Default().Error("消费事件失败", "subject", ev.Subject,
				"aggregate_id", ev.AggregateID, "error", err)
		}
	})
	if err != nil {
		return fmt.Errorf("订阅 %s: %w", subject, err)
	}
	defer sub.Unsubscribe()

	<-ctx.Done()
	return nil
}

func eventFromMsg(msg *nats.Msg) Event {
	version, _ := strconv.ParseInt(msg.Header.Get(headerVersion), 10, 64)
	hopCount, _ := strconv.Atoi(msg.Header.Get(headerHopCount))
	return Event{
		Subject:     msg.Subject,
		AggregateID: msg.Header.Get(headerAggregateID),
		Version:     version,
		TraceID:     msg.Header.Get(headerTraceID),
		CausationID: msg.Header.Get(headerCausationID),
		HopCount:    hopCount,
		Payload:     msg.Data,
	}
}

func handleOne(ctx context.Context, nc *nats.Conn, db *sql.DB, role, schema string, ev Event,
	fn func(context.Context, *sql.Tx, Event) error) error {

	// §3.10：hop_count > 5 直接丢弃进 DLQ，物理斩断无限循环。
	if ev.HopCount > maxHopCount {
		return deadLetter(nc, ev, "hop_count 超过上限")
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// ⚠️ 同 WithTx 的理由：SET 必须带 LOCAL，且必须在 fn 拿到 tx 之前切好——
	// fn 里的业务查询按 WithTx 的约定写"不带 schema 前缀"的 SQL（product_
	// tracking_snapshots 而不是 erp_inventory.product_tracking_snapshots），
	// 不切好 search_path 这些查询会直接报表不存在。
	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+role); err != nil {
		return fmt.Errorf("SET LOCAL ROLE %s: %w", role, err)
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL search_path TO "+schema+", "+schema+"_archive"); err != nil {
		return fmt.Errorf("SET LOCAL search_path TO %s: %w", schema, err)
	}

	// version 单调：本地已经有一条 >= 传入 version 的记录，跳过更新
	// （免疫乱序——事件总线不保证顺序送达）。
	var maxSeen sql.NullInt64
	err = tx.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT max(version) FROM %s.event_inbox WHERE subject = $1 AND aggregate_id = $2`, schema),
		ev.Subject, ev.AggregateID).Scan(&maxSeen)
	if err != nil {
		return err
	}
	if maxSeen.Valid && ev.Version <= maxSeen.Int64 {
		return tx.Commit() // 静默跳过，不算错误
	}

	// 消费幂等：同一个 (subject, aggregate_id, version) 只处理一次。
	idempotencyKey := ev.Subject + "|" + ev.AggregateID + "|" + strconv.FormatInt(ev.Version, 10)
	_, err = tx.ExecContext(ctx, fmt.Sprintf(
		`INSERT INTO %s.event_inbox (idempotency_key, subject, aggregate_id, version) VALUES ($1, $2, $3, $4)`, schema),
		idempotencyKey, ev.Subject, ev.AggregateID, ev.Version)
	if err != nil {
		if isUniqueViolation(err) {
			return tx.Commit() // 已经处理过这个 (subject, aggregate_id, version)，静默跳过
		}
		return err
	}

	if err := fn(ctx, tx, ev); err != nil {
		return err // defer 里的 Rollback 会执行；inbox 记录也跟着回滚
	}
	return tx.Commit()
}

// deadLetter 把消息原样转发到 dlq.<原 subject>，供 infra-dlq-monitor 消费。
// be-sdk-go 只负责转发，不负责监控与人工干预——那是独立组件的职责。
func deadLetter(nc *nats.Conn, ev Event, reason string) error {
	msg := &nats.Msg{Subject: "dlq." + ev.Subject, Data: ev.Payload, Header: nats.Header{}}
	msg.Header.Set(headerAggregateID, ev.AggregateID)
	msg.Header.Set(headerVersion, strconv.FormatInt(ev.Version, 10))
	msg.Header.Set(headerTraceID, ev.TraceID)
	msg.Header.Set(headerCausationID, ev.CausationID)
	msg.Header.Set(headerHopCount, strconv.Itoa(ev.HopCount))
	msg.Header.Set("X-Dlq-Reason", reason)
	if err := nc.PublishMsg(msg); err != nil {
		return fmt.Errorf("转发死信到 dlq.%s: %w", ev.Subject, err)
	}
	slog.Default().Warn("事件进入死信队列", "subject", ev.Subject,
		"aggregate_id", ev.AggregateID, "hop_count", ev.HopCount, "reason", reason)
	return nil
}

// isUniqueViolation 判断错误是不是唯一约束冲突。pgx 的错误类型带 SQLSTATE，
// 23505 是 PostgreSQL 的 unique_violation。
func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return false
}
