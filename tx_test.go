package besdk

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // ⚠️ 不是计划原文的 lib/pq——那个驱动
	// 已被 §I 门禁 9 / 决策 108 明确禁掉（不再维护，且不支持我们要的一些
	// PG 类型）。全项目统一用 pgx/v5/stdlib，驱动名注册为 "pgx"。
)

// 这个用例守的是设计书里点名「最难查的一个」的雷：
// 用不带 LOCAL 的 SET 之后把连接还回池，下一个借用者会原样继承它。
// 断言方式：把池限制成 1 条连接，WithTx 跑完之后再借出同一条连接，
// 它的 search_path 必须已经还原，不能还是组件的 schema。
func TestWithTx_连接还池后search_path必须还原(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("未设置 TEST_PG_DSN，跳过（CI 里必须设）")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // 关键：强制复用同一条物理连接

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS besdk_probe`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS besdk_probe_archive`); err != nil {
		t.Fatal(err)
	}

	var before string
	if err := db.QueryRowContext(ctx, `SHOW search_path`).Scan(&before); err != nil {
		t.Fatal(err)
	}

	// "postgres" 是这条测试连接本身登录用的角色，在任何测试 PG 实例上都
	// 保证存在——不能用 current_user 这个占位符，SET LOCAL ROLE 不接受它
	// （PostgreSQL 语法层面就拒绝，"syntax error at or near current_user"）。
	err = WithTx(ctx, db, "postgres", "besdk_probe", func(tx *sql.Tx) error {
		var inside string
		if err := tx.QueryRow(`SHOW search_path`).Scan(&inside); err != nil {
			return err
		}
		if inside != "besdk_probe, besdk_probe_archive" {
			t.Fatalf("事务内 search_path 应为 besdk_probe, besdk_probe_archive，实际 %q", inside)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var after string
	if err := db.QueryRowContext(ctx, `SHOW search_path`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("连接还池后 search_path 未还原：还池前 %q，还池后 %q —— "+
			"说明用了不带 LOCAL 的 SET，会跨组件串数据", before, after)
	}
}

func TestWithTx_拒绝非法标识符(t *testing.T) {
	for _, bad := range []string{"erp_sales; DROP SCHEMA public", "ERP_Sales", "1erp", ""} {
		if err := WithTx(context.Background(), nil, "r", bad, nil); err == nil {
			t.Fatalf("schema=%q 应该被拒绝", bad)
		}
	}
}
