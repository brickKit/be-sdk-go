package besdk

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"
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
	go func() { pumpDone <- StartOutboxPump(pumpCtx, db, "besdk_outbox_probe", nc, discardLoggerForTest()) }()

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

// ⚠️ 实测踩坑：mdm-customer 真的停掉 postgres 后，pumpOnce 查询失败被当作
// 硬错误往上抛，StartOutboxPump 整个循环退出 → Module.Start 的 errCh 收到
// 错误 → RunStandalone 把整个进程带崩 → Docker 又把容器拉起来 → 拉起来立刻
// 重新连接又立刻失败——容器陷入几百毫秒一次的重启死循环，直到 postgres
// 恢复，且这段时间内 HTTP/gRPC 也完全没人能连（不是"降级"，是整个容器
// 反复重启)。这与 partition.go 的 Start 形成对照——分区维护失败只记日志
// 不退出循环，outbox pump 这里当时没有对齐同一套容错方式。
func TestStartOutboxPump_数据库查询失败不让循环退出(t *testing.T) {
	db := setupOutboxProbeDB(t)

	nc, err := nats.Connect(natsURLForTest(t))
	if err != nil {
		t.Fatalf("连接 NATS 失败：%v", err)
	}
	defer nc.Close()

	// schema 名格式合法但实际不存在——每一次 pumpOnce 都会查询失败，
	// 模拟"数据库连不上/表不存在"这类瞬时故障，且不会自愈。
	pumpCtx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	pumpDone := make(chan error, 1)
	go func() {
		pumpDone <- StartOutboxPump(pumpCtx, db, "besdk_outbox_probe_missing_schema", nc, discardLoggerForTest())
	}()

	select {
	case err := <-pumpDone:
		t.Fatalf("查询持续失败不该让 pump 提前退出（应该只记日志、等下一轮 ticker），得到：%v", err)
	case <-time.After(500 * time.Millisecond):
		// 500ms > 2 个 outboxPollInterval（200ms），扛住了至少两次失败的 tick。
	}

	select {
	case err := <-pumpDone:
		if err != nil {
			t.Fatalf("ctx 到期后应该干净返回 nil（不是把查询错误当成关停错误），得到：%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pump 在 ctx 到期后没有及时返回")
	}
}

// ⚠️ 实测踩坑：这是给"要不要给测试套件开 t.Parallel()"做可行性评估时，
// 顺带查出来的一个真实生产级 bug——早期的 pumpOnce 是裸 SELECT，读和
// "认领"之间没有互斥手段，两个 pump 实例（K8s 滚动重启新旧副本重叠、或
// 同一组件将来被扩到多副本）同时轮询同一张表会各自把同一批事件真实发布
// 一遍，静默重复投递。这条测试就是这个 bug 的回归用例：真的起两个
// goroutine 并发调用 pumpOnce，抢的是同一张物理表，断言每条事件只会被
// NATS 收到恰好一次。
func TestPumpOnce_两个实例并发认领同一批事件_每条只发一次(t *testing.T) {
	db := setupOutboxProbeDB(t)
	ctx := context.Background()

	const n = 40
	subject := "test.probe.concurrent.v1"
	for i := 0; i < n; i++ {
		aggID := fmt.Sprintf("concurrent-%d", i)
		if _, err := db.ExecContext(ctx, `
			INSERT INTO besdk_outbox_probe.event_outbox
				(subject, aggregate_id, version, payload)
			VALUES ($1, $2, 1, '{}')`, subject, aggID); err != nil {
			t.Fatal(err)
		}
	}

	nc, err := nats.Connect(natsURLForTest(t))
	if err != nil {
		t.Fatalf("连接 NATS 失败：%v", err)
	}
	defer nc.Close()

	var mu sync.Mutex
	seen := map[string]int{}
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		aggID := m.Header.Get(headerAggregateID)
		mu.Lock()
		seen[aggID]++
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 两个 goroutine 卡在同一个 channel 上，尽量让两次 pumpOnce 真的在
	// 同一个时间窗口里去抢同一批行，而不是先后错开各拿各的。
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- pumpOnce(ctx, db, "besdk_outbox_probe", nc)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("pumpOnce 不该报错：%v", err)
		}
	}

	// NATS 投递是异步的，给回调一点时间落地。
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		total := 0
		for _, c := range seen {
			total += c
		}
		mu.Unlock()
		if total >= n || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Fatalf("应该收到 %d 个不同的 aggregate_id，实际收到 %d 个", n, len(seen))
	}
	for aggID, count := range seen {
		if count != 1 {
			t.Fatalf("aggregate_id=%s 被投递了 %d 次，应该恰好 1 次——两个 pump 实例抢到了同一行", aggID, count)
		}
	}

	var stillClaimable int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM besdk_outbox_probe.event_outbox
		WHERE subject = $1 AND status != 'PUBLISHED'`, subject).Scan(&stillClaimable); err != nil {
		t.Fatal(err)
	}
	if stillClaimable != 0 {
		t.Fatalf("两次 pumpOnce 加起来应该认领完全部 %d 条，还剩 %d 条没转成 PUBLISHED", n, stillClaimable)
	}
}

func TestPumpOnce_认领超时的SENDING行会被重新认领(t *testing.T) {
	db := setupOutboxProbeDB(t)
	ctx := context.Background()

	subject := "test.probe.stuck.v1"
	staleSeconds := int(outboxClaimTimeout.Seconds()) + 5
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO besdk_outbox_probe.event_outbox
			(subject, aggregate_id, version, payload, status, updated_at)
		VALUES ($1, 'stuck-1', 1, '{}', 'SENDING', now() - interval '%d seconds')`, staleSeconds),
		subject); err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(natsURLForTest(t))
	if err != nil {
		t.Fatalf("连接 NATS 失败：%v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	if err := pumpOnce(ctx, db, "besdk_outbox_probe", nc); err != nil {
		t.Fatalf("pumpOnce 不该报错：%v", err)
	}

	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("卡住超时的 SENDING 行应该被重新认领并发出去，等消息超时：%v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM besdk_outbox_probe.event_outbox WHERE aggregate_id = 'stuck-1'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "PUBLISHED" {
		t.Fatalf("重新认领并发布成功后应该是 PUBLISHED，得到 %q", status)
	}
}

func TestPumpOnce_未超时的SENDING行不会被重新认领(t *testing.T) {
	db := setupOutboxProbeDB(t)
	ctx := context.Background()

	subject := "test.probe.inflight.v1"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO besdk_outbox_probe.event_outbox
			(subject, aggregate_id, version, payload, status)
		VALUES ($1, 'inflight-1', 1, '{}', 'SENDING')`, subject); err != nil {
		t.Fatal(err)
	}

	nc, err := nats.Connect(natsURLForTest(t))
	if err != nil {
		t.Fatalf("连接 NATS 失败：%v", err)
	}
	defer nc.Close()

	if err := pumpOnce(ctx, db, "besdk_outbox_probe", nc); err != nil {
		t.Fatalf("pumpOnce 不该报错：%v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM besdk_outbox_probe.event_outbox WHERE aggregate_id = 'inflight-1'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SENDING" {
		t.Fatalf("刚认领不久（未超时）的 SENDING 行不该被别的实例重新认领，状态应仍是 SENDING，得到 %q", status)
	}
}

func natsURLForTest(t *testing.T) string {
	t.Helper()
	if u := os.Getenv("TEST_NATS_URL"); u != "" {
		return u
	}
	return nats.DefaultURL
}

// natsHostPortForTest 把 natsURLForTest 拆成 MQ_HOST/MQ_PORT 两片——平台
// 注入的从来是分开的两个变量，不是一个完整 URL（§标准化环境变量约定同
// buildNATSURL）。
func natsHostPortForTest(t *testing.T) (host, port string) {
	t.Helper()
	u, err := url.Parse(natsURLForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	return u.Hostname(), u.Port()
}
