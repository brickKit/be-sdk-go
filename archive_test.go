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
