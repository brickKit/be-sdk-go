package besdk

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
)

func setupOutboxProbeDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("未设置 TEST_PG_DSN，跳过（CI 里必须设）")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	ctx := context.Background()
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS besdk_outbox_probe`,
		// ⚠️ 测试用简化版：不分区。真实组件的迁移要按 §11.2.2 做周分区，
		// 但分区是物理存储细节，与 SDK 本身的读写逻辑正交——SDK 的单测
		// 不需要真分区表也能验证 PublishOutbox/StartOutboxPump 的行为。
		`CREATE TABLE IF NOT EXISTS besdk_outbox_probe.event_outbox (
			id           BIGSERIAL PRIMARY KEY,
			subject      TEXT        NOT NULL,
			aggregate_id TEXT        NOT NULL,
			version      BIGINT      NOT NULL,
			trace_id     TEXT        NOT NULL DEFAULT '',
			causation_id TEXT        NOT NULL DEFAULT '',
			hop_count    INT         NOT NULL DEFAULT 0,
			payload      JSONB       NOT NULL,
			published_at TIMESTAMPTZ,
			attempts     INT         NOT NULL DEFAULT 0,
			created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			status       TEXT        NOT NULL DEFAULT 'PENDING'
		)`,
		`CREATE TABLE IF NOT EXISTS besdk_outbox_probe.probe_business (
			id  TEXT PRIMARY KEY,
			val TEXT NOT NULL
		)`,
		`TRUNCATE besdk_outbox_probe.event_outbox`,
		`TRUNCATE besdk_outbox_probe.probe_business`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("准备测试表失败：%v\nSQL: %s", err, s)
		}
	}
	return db
}

func TestPublishOutbox_与业务数据同一事务_回滚都不留(t *testing.T) {
	db := setupOutboxProbeDB(t)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO besdk_outbox_probe.probe_business (id, val) VALUES ('biz-1', 'x')`); err != nil {
		t.Fatal(err)
	}
	ev := Event{Subject: "test.probe.created.v1", AggregateID: "biz-1", Version: 1, Payload: []byte(`{"k":"v"}`)}
	if err := PublishOutbox(tx, "besdk_outbox_probe", ev); err != nil {
		t.Fatalf("PublishOutbox 不该报错：%v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	var bizCount, outboxCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM besdk_outbox_probe.probe_business`).Scan(&bizCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM besdk_outbox_probe.event_outbox`).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if bizCount != 0 || outboxCount != 0 {
		t.Fatalf("业务回滚后两张表都该是空的，实际 业务=%d outbox=%d", bizCount, outboxCount)
	}
}

func TestPublishOutbox_提交后写入且默认PENDING(t *testing.T) {
	db := setupOutboxProbeDB(t)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ev := Event{Subject: "test.probe.created.v1", AggregateID: "biz-2", Version: 1, Payload: []byte(`{"k":"v"}`)}
	if err := PublishOutbox(tx, "besdk_outbox_probe", ev); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var status string
	var payload []byte
	err = db.QueryRowContext(ctx,
		`SELECT status, payload FROM besdk_outbox_probe.event_outbox WHERE aggregate_id = 'biz-2'`).
		Scan(&status, &payload)
	if err != nil {
		t.Fatalf("提交后应该能查到这一行：%v", err)
	}
	if status != "PENDING" {
		t.Fatalf("默认状态应为 PENDING，得到 %q", status)
	}
	var got map[string]any
	if err := json.Unmarshal(payload, &got); err != nil || got["k"] != "v" {
		t.Fatalf("payload 没有正确落库：%s（err=%v）", payload, err)
	}
}

func TestStartOutboxPump_发送成功后标记为PUBLISHED(t *testing.T) {
	db := setupOutboxProbeDB(t)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	ev := Event{Subject: "test.probe.pumped.v1", AggregateID: "biz-3", Version: 1, Payload: []byte(`{}`)}
	if err := PublishOutbox(tx, "besdk_outbox_probe", ev); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(natsURLForTest(t))
	if err != nil {
		t.Fatalf("连接 NATS 失败：%v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("test.probe.pumped.v1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	pumpCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	pumpDone := make(chan error, 1)
	go func() { pumpDone <- StartOutboxPump(pumpCtx, db, "besdk_outbox_probe", nc) }()

	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("pump 应该把事件发到 NATS，等消息超时：%v", err)
	}

	// pump 应该在 ctx 到期后干净返回（Module.Start 的 ctx-cancel 契约）
	select {
	case err := <-pumpDone:
		if err != nil {
			t.Fatalf("pump 结束时不该报错：%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("pump 在 ctx 到期后没有及时返回")
	}

	var status string
	var attempts int
	if err := db.QueryRowContext(ctx,
		`SELECT status, attempts FROM besdk_outbox_probe.event_outbox WHERE aggregate_id = 'biz-3'`).
		Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "PUBLISHED" {
		t.Fatalf("发送成功后状态应为 PUBLISHED，得到 %q（attempts=%d）", status, attempts)
	}
}

func natsURLForTest(t *testing.T) string {
	t.Helper()
	if u := os.Getenv("TEST_NATS_URL"); u != "" {
		return u
	}
	return nats.DefaultURL
}
