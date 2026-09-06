package besdk

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
)

func setupEventsProbeDB(t *testing.T) *sql.DB {
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
		`CREATE SCHEMA IF NOT EXISTS besdk_events_probe`,
		`CREATE TABLE IF NOT EXISTS besdk_events_probe.event_inbox (
			idempotency_key TEXT PRIMARY KEY,
			subject         TEXT   NOT NULL,
			aggregate_id    TEXT   NOT NULL,
			version         BIGINT NOT NULL,
			created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`TRUNCATE besdk_events_probe.event_inbox`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("准备测试表失败：%v\nSQL: %s", err, s)
		}
	}
	return db
}

func publishTestEvent(t *testing.T, nc *nats.Conn, subject, aggregateID string, version int64, hopCount int) {
	t.Helper()
	msg := &nats.Msg{Subject: subject, Data: []byte(`{}`), Header: nats.Header{}}
	msg.Header.Set(headerAggregateID, aggregateID)
	msg.Header.Set(headerVersion, strconv.FormatInt(version, 10))
	msg.Header.Set(headerHopCount, strconv.Itoa(hopCount))
	if err := nc.PublishMsg(msg); err != nil {
		t.Fatal(err)
	}
}

func TestConsume_hop_count超过5丢弃进DLQ(t *testing.T) {
	db := setupEventsProbeDB(t)
	nc, err := nats.Connect(natsURLForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	dlqSub, err := nc.SubscribeSync("dlq.test.events.hopcount.v1")
	if err != nil {
		t.Fatal(err)
	}
	defer dlqSub.Unsubscribe()

	var called atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		_ = Consume(ctx, nc, db, "besdk_events_probe", "test.events.hopcount.v1",
			func(context.Context, *sql.Tx, Event) error {
				called.Store(true)
				return nil
			})
	}()
	time.Sleep(100 * time.Millisecond) // 等订阅建立

	publishTestEvent(t, nc, "test.events.hopcount.v1", "agg-1", 1, 6) // hop_count=6 > 5

	if _, err := dlqSub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("hop_count 超限应该被转发到 dlq，等待超时：%v", err)
	}
	if called.Load() {
		t.Fatal("hop_count 超限时不该调用业务 fn")
	}
}

func TestConsume_消费幂等_同idempotency投两次只落一次(t *testing.T) {
	db := setupEventsProbeDB(t)
	nc, err := nats.Connect(natsURLForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	var callCount atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = Consume(ctx, nc, db, "besdk_events_probe", "test.events.idem.v1",
			func(context.Context, *sql.Tx, Event) error {
				callCount.Add(1)
				return nil
			})
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)

	publishTestEvent(t, nc, "test.events.idem.v1", "agg-2", 1, 0)
	publishTestEvent(t, nc, "test.events.idem.v1", "agg-2", 1, 0) // 同一个 (subject, aggregate_id, version)
	nc.Flush()
	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done

	if got := callCount.Load(); got != 1 {
		t.Fatalf("同一个事件投两次，业务 fn 应该只被调用 1 次，实际 %d 次", got)
	}
}

func TestConsume_version不大于本地当前值时跳过(t *testing.T) {
	db := setupEventsProbeDB(t)
	nc, err := nats.Connect(natsURLForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	var applied []int64
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = Consume(ctx, nc, db, "besdk_events_probe", "test.events.version.v1",
			func(_ context.Context, _ *sql.Tx, ev Event) error {
				applied = append(applied, ev.Version)
				return nil
			})
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)

	// 乱序到达：先 version=3，再 version=2（旧的）
	publishTestEvent(t, nc, "test.events.version.v1", "agg-3", 3, 0)
	nc.Flush()
	time.Sleep(300 * time.Millisecond)
	publishTestEvent(t, nc, "test.events.version.v1", "agg-3", 2, 0)
	nc.Flush()
	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done

	if len(applied) != 1 || applied[0] != 3 {
		t.Fatalf("旧版本(2)应该被跳过，只有 version=3 该被应用，实际调用序列 %v", applied)
	}
}
