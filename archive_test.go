package besdk

import (
	"context"
	"database/sql"
	"os"
	"sort"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type probeRow struct {
	ID  string
	Val string
}

func scanProbeRow(rows *sql.Rows) (string, probeRow, error) {
	var r probeRow
	err := rows.Scan(&r.ID, &r.Val)
	return r.ID, r, err
}

func setupArchiveProbeDB(t *testing.T) *sql.DB {
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
		`CREATE SCHEMA IF NOT EXISTS besdk_archive_probe`,
		`CREATE SCHEMA IF NOT EXISTS besdk_archive_probe_archive`,
		`CREATE TABLE IF NOT EXISTS besdk_archive_probe.items (id text PRIMARY KEY, val text NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS besdk_archive_probe_archive.items (id text PRIMARY KEY, val text NOT NULL)`,
		`TRUNCATE besdk_archive_probe.items`,
		`TRUNCATE besdk_archive_probe_archive.items`,
		`INSERT INTO besdk_archive_probe.items (id, val) VALUES ('hot-1', 'from-hot')`,
		`INSERT INTO besdk_archive_probe_archive.items (id, val) VALUES ('cold-1', 'from-archive')`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("准备测试数据失败：%v\nSQL: %s", err, s)
		}
	}
	return db
}

func TestBatchGetRouted_热表命中_归档缺失都覆盖(t *testing.T) {
	db := setupArchiveProbeDB(t)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	got, err := BatchGetRouted(ctx, tx, "besdk_archive_probe", "items",
		[]string{"hot-1", "cold-1", "missing-1"}, scanProbeRow)
	if err != nil {
		t.Fatalf("BatchGetRouted 不该报错：%v", err)
	}

	sort.Slice(got, func(i, j int) bool { return got[i].ID < got[j].ID })
	want := []probeRow{{ID: "cold-1", Val: "from-archive"}, {ID: "hot-1", Val: "from-hot"}}
	if len(got) != len(want) {
		t.Fatalf("期望 %d 条（hot-1 与 cold-1），实际 %d 条：%+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 条不符：期望 %+v，得到 %+v", i, want[i], got[i])
		}
	}
	// missing-1 两处都没有——不报错，静默从结果里缺席
}

// ⚠️ 实测踩坑：mdm-customer 用真实 PostgreSQL 跑 BatchGet 时，一个真的
// 缺失的 id（热表没有）会让 BatchGetRouted 去查 {schema}_archive.{table}，
// 而 mdm-customer 设计上永远不归档主数据（设计计划 §7）——那个 schema
// 建了，但从来不会有任何表在里面。上一条测试只覆盖了"热表全命中、归档表
// 不存在也不报错"（间接证明不强制查归档），没覆盖"热表有缺失、归档表
// 不存在"这条更常见的路径——BatchGetRouted 原样把 "relation does not
// exist" 抛出来了，而"两处都没有的 id 静默缺席，不报错"是这个函数自己
// 文档注释写的承诺（archive.go 顶部）。
func TestBatchGetRouted_归档表整个不存在时缺失id也不报错(t *testing.T) {
	db := setupArchiveProbeDB(t)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DROP TABLE besdk_archive_probe_archive.items`); err != nil {
		t.Fatal(err)
	}

	got, err := BatchGetRouted(ctx, tx, "besdk_archive_probe", "items",
		[]string{"hot-1", "missing-1"}, scanProbeRow)
	if err != nil {
		t.Fatalf("归档表不存在时，缺失的 id 也不该报错（只是查不到归档而已）：%v", err)
	}
	if len(got) != 1 || got[0].ID != "hot-1" {
		t.Fatalf("期望恰好 1 条 hot-1，得到 %+v", got)
	}
}

func TestBatchGetRouted_热表全命中不查归档(t *testing.T) {
	db := setupArchiveProbeDB(t)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 把归档表清空后再查——如果实现漏了"热表全命中就跳过归档查询"这个
	// 优化，结果不会错（归档本来就没有），所以这条测的不是正确性而是
	// "归档表不存在时热表全命中也不该报错"，间接确认没有强制查询归档表。
	if _, err := tx.ExecContext(ctx, `DROP TABLE besdk_archive_probe_archive.items`); err != nil {
		t.Fatal(err)
	}

	got, err := BatchGetRouted(ctx, tx, "besdk_archive_probe", "items",
		[]string{"hot-1"}, scanProbeRow)
	if err != nil {
		t.Fatalf("热表全命中时不该因为归档表不存在而报错：%v", err)
	}
	if len(got) != 1 || got[0].ID != "hot-1" {
		t.Fatalf("期望恰好 1 条 hot-1，得到 %+v", got)
	}
}
