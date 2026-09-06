package besdk

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
)

var identRe = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// WithTx 开事务，切到本组件的 role 与 schema，跑 fn，然后 COMMIT。
//
// ⚠️⚠️ 绝不能用不带 LOCAL 的 SET。SET search_path / SET ROLE 之后把连接
// 还回共享池，下一个借用者会原样继承它——A 组件的查询打在 B 组件的表上，
// 不报错、不崩，只是悄悄读写了别人的数据。这是这套写法唯一的雷，
// 也是最难查的一个（设计书决策 3、§13.3 铁律二）。
//
// SET LOCAL 在 COMMIT/ROLLBACK 时自动还原，连接干净地回到池里。
func WithTx(ctx context.Context, db *sql.DB, role, schema string,
	fn func(*sql.Tx) error) error {

	// role 与 schema 来自 registry，不是用户输入；但它们要拼进 SQL
	// （SET LOCAL ROLE 不接受占位符），所以仍然白名单校验。
	if !identRe.MatchString(role) {
		return fmt.Errorf("非法 role 名：%q", role)
	}
	if !identRe.MatchString(schema) {
		return fmt.Errorf("非法 schema 名：%q", schema)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }() // COMMIT 成功后这次 Rollback 是 no-op

	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+role); err != nil {
		return fmt.Errorf("SET LOCAL ROLE %s: %w", role, err)
	}
	// 归档 schema 也要在 search_path 里，batchGet 的冷热路由才不用写限定名
	if _, err := tx.ExecContext(ctx,
		"SET LOCAL search_path TO "+schema+", "+schema+"_archive"); err != nil {
		return fmt.Errorf("SET LOCAL search_path TO %s: %w", schema, err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
