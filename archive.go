package besdk

import (
	"context"
	"database/sql"
)

// BatchGetRouted 先查热表，缺失的再查 {schema}_archive（§11.6.1）。
//
// ⚠️ 每个聚合根必须提供的 batchGet（§3.8）走这一层实现冷热自动路由——
// 业务代码里不许出现 `if archived { ... }` 这种分支，归档与否对业务
// 完全透明。
//
// 实现放 Task 7 用 TDD 补：核心不变量是「传入 N 个 ID，无论各自在热表还是
// 归档表，返回集合与传入顺序无关、且不重不漏」。
func BatchGetRouted[T any](ctx context.Context, tx *sql.Tx, schema, table string,
	ids []string, scan func(*sql.Rows) (T, error)) ([]T, error) {
	panic("未实现：Task 7 补")
}
