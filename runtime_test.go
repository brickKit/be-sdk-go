package besdk

import (
	"context"
	"os"
	"testing"
	"time"
)

// 守的是 §12.5.3「配置只能注入」：rt.Config 是构造时的一份快照，不是
// 每次读都去查进程环境变量——改进程环境变量不该让已经构造好的 rt 变化。
func TestConfig_是快照不是活的环境变量查询(t *testing.T) {
	os.Setenv("BESDK_TEST_SNAPSHOT_KEY", "before")
	defer os.Unsetenv("BESDK_TEST_SNAPSHOT_KEY")

	cfg := NewConfig(envSnapshot())
	os.Setenv("BESDK_TEST_SNAPSHOT_KEY", "after")

	got, ok := cfg.String("BESDK_TEST_SNAPSHOT_KEY")
	if !ok || got != "before" {
		t.Fatalf("Config 应该是构造时的快照，期望 before/true，得到 %q/%v", got, ok)
	}
}

// 合并态的最小复现：两个不同 ComponentID 的 Runtime 各持一份 Config，
// 互不干扰——不是共用同一份 map。
//
// ⚠️ map 的 key 用 "PG_SCHEMA"（真实平台注入的环境变量名），不是
// "pgSchema"——这是修过一次的坑：旧版本这里两边都写成 "pgSchema"，
// 因为查询代码本身也是拿 camelCase 字面量直接查 map，两边"手拉手"错得
// 一致，测试就这么全绿地掩盖了 Config.String 从来没做 camelCase →
// SCREAMING_SNAKE_CASE 转换这个真实存在的 bug（见 configEnvVarName 的
// 注释）。这里改用真实的注入格式构造 map，调用方（业务代码）依然按
// component.yaml 里声明的 camelCase 名字查，这才是这条测试真正该验证
// 的接口契约。
func TestConfig_两个Runtime各持一份互不干扰(t *testing.T) {
	rt1 := &Runtime{ComponentID: "mdm/customer", Config: NewConfig(map[string]string{"PG_SCHEMA": "mdm_customer"})}
	rt2 := &Runtime{ComponentID: "erp/sales", Config: NewConfig(map[string]string{"PG_SCHEMA": "erp_sales"})}

	got1, _ := rt1.Config.String("pgSchema")
	got2, _ := rt2.Config.String("pgSchema")
	if got1 != "mdm_customer" || got2 != "erp_sales" {
		t.Fatalf("两个 Runtime 的 Config 串了：rt1=%q rt2=%q", got1, got2)
	}
}

// TestConfig_camelCase查询能找到平台真实注入的SCREAMING_SNAKE_CASE环境变量
// 是这一批 bug 修复的核心回归测试。brickKit 装配阶段（internal/inject.Build）
// 把 configSchema 的每一项都转成 SCREAMING_SNAKE_CASE 才注入进容器环境
// （实测 `brickkit up --dry-run` 生成的 docker-compose.yaml 证实：
// "pgSchema" → "PG_SCHEMA"，"otelBaseUrl" → "OTEL_BASE_URL"），而模块
// 代码一律按 component.yaml 里声明的原始 camelCase 名字查（如
// rt.Config.StringOr("pgSchema", ...)）——Config 的 getter 必须自己做
// 这一层转换，否则永远查不到真实注入的值，只会拿到调用方给的 default。
//
// 用 MustString（没有 default 的必填项）而不是 StringOr 来验证，是因为
// StringOr 在两边不匹配时会用 default 悄悄兜底，观察不出差异——这正是
// 这个 bug 在 mdm-customer/mdm-product/erp-inventory/erp-finance 四个
// 组件身上从没被发现的原因（它们的 default 恰好总等于真实值）。
// defaultWarehouseId（erp-sales 用 MustString）没有这个安全网，查不到
// 就直接 panic，是第一个会真正暴露这个 bug 的配置项。
func TestConfig_camelCase查询能找到平台真实注入的环境变量(t *testing.T) {
	cfg := NewConfig(map[string]string{
		"PG_SCHEMA":            "erp_sales",
		"OTEL_BASE_URL":        "http://otel:4318",
		"DEFAULT_WAREHOUSE_ID": "1",
	})

	if got := cfg.StringOr("pgSchema", "不该用到这个默认值"); got != "erp_sales" {
		t.Fatalf("pgSchema 应该查到 PG_SCHEMA 的真实值 erp_sales，实际 %q", got)
	}
	if got := cfg.StringOr("otelBaseUrl", "不该用到这个默认值"); got != "http://otel:4318" {
		t.Fatalf("otelBaseUrl 应该查到 OTEL_BASE_URL 的真实值，实际 %q", got)
	}
	// MustString 是关键断言：查不到会 panic，用它验证 defaultWarehouseId
	// 这种没有 default 的必填项也能正确查到，而不是"反正有 default 兜底
	// 看不出问题"。
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("defaultWarehouseId 应该查到 DEFAULT_WAREHOUSE_ID 的真实值，却 panic 了：%v", r)
			}
		}()
		if got := cfg.MustString("defaultWarehouseId"); got != "1" {
			t.Fatalf("defaultWarehouseId 期望 1，实际 %q", got)
		}
	}()
}

// Module.Start 是调用方提供的函数值，SDK 本身不做任何包装——这条测的是
// "一个写得对的 Start 长什么样"，作为文档性示例：必须接 ctx，cancel 后
// 及时返回，不是靠自己装信号处理器。
func TestModule_Start约定_ctx_cancel后及时返回(t *testing.T) {
	mod := &Module{
		Start: func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mod.Start(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancel 后 Start 不该报错：%v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Start 在 ctx cancel 后没有及时返回")
	}
}
