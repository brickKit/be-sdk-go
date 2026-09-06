package besdk

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// BatchGetRouted 先查热表，缺失的再查 {schema}_archive（§11.6.1）。
//
// ⚠️ 每个聚合根必须提供的 batchGet（§3.8）走这一层实现冷热自动路由——
// 业务代码里不许出现 `if archived { ... }` 这种分支，归档与否对业务
// 完全透明。两处都没有的 id 静默从结果里缺席，不报错（调用方自己按
// 「结果集比传入的 ids 少」判断哪些不存在）。
//
// ⚠️ scan 除了行本身，还要把这一行的 id 一并返回——BatchGetRouted 需要
// 拿它去重（同一个 id 理论上不会同时出现在热表与归档表，但拿去重防一手
// 比假设"不会发生"更安全）、也需要按传入 ids 的顺序重排结果。
// schema/table 名走白名单校验，不接受占位符（与 WithTx 同一套 identRe）。
func BatchGetRouted[T any](ctx context.Context, tx *sql.Tx, schema, table string,
	ids []string, scan func(*sql.Rows) (id string, row T, err error)) ([]T, error) {

	if !identRe.MatchString(schema) {
		return nil, fmt.Errorf("非法 schema 名：%q", schema)
	}
	if !identRe.MatchString(table) {
		return nil, fmt.Errorf("非法 table 名：%q", table)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	byID, err := queryByIDs(ctx, tx, schema, table, ids, scan)
	if err != nil {
		return nil, fmt.Errorf("查热表 %s.%s: %w", schema, table, err)
	}

	missing := make([]string, 0, len(ids)-len(byID))
	for _, id := range ids {
		if _, ok := byID[id]; !ok {
			missing = append(missing, id)
		}
	}

	if len(missing) > 0 {
		archiveSchema := schema + "_archive"
		fromArchive, err := queryByIDs(ctx, tx, archiveSchema, table, missing, scan)
		if err != nil {
			return nil, fmt.Errorf("查归档表 %s.%s: %w", archiveSchema, table, err)
		}
		for id, row := range fromArchive {
			byID[id] = row
		}
	}

	// 按传入 ids 的顺序返回，两处都没有的 id 静默缺席
	out := make([]T, 0, len(byID))
	for _, id := range ids {
		if row, ok := byID[id]; ok {
			out = append(out, row)
		}
	}
	return out, nil
}

// queryByIDs 按 id 列表查一张（已限定 schema 的）表，返回 id → 那一行的
// 映射。
func queryByIDs[T any](ctx context.Context, tx *sql.Tx, schema, table string,
	ids []string, scan func(*sql.Rows) (string, T, error)) (map[string]T, error) {

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf("SELECT * FROM %s.%s WHERE id IN (%s)",
		schema, table, strings.Join(placeholders, ", "))

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]T)
	for rows.Next() {
		id, row, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out[id] = row
	}
	return out, rows.Err()
}
