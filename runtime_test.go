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
func TestConfig_两个Runtime各持一份互不干扰(t *testing.T) {
	rt1 := &Runtime{ComponentID: "mdm/customer", Config: NewConfig(map[string]string{"pgSchema": "mdm_customer"})}
	rt2 := &Runtime{ComponentID: "erp/sales", Config: NewConfig(map[string]string{"pgSchema": "erp_sales"})}

	got1, _ := rt1.Config.String("pgSchema")
	got2, _ := rt2.Config.String("pgSchema")
	if got1 != "mdm_customer" || got2 != "erp_sales" {
		t.Fatalf("两个 Runtime 的 Config 串了：rt1=%q rt2=%q", got1, got2)
	}
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
