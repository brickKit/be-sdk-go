package besdk

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewRegistry_不是默认全局那个(t *testing.T) {
	reg := NewRegistry()
	if reg == nil {
		t.Fatal("Registry 不该是 nil")
	}
	if reg == prometheus.DefaultRegisterer {
		t.Fatal("不许返回默认全局 registry（§12.5.2）")
	}
}

// 守的是合并态最小复现：进程内 22 个模块各自 NewGinEngine，每个模块
// 都往自己的 Registry 注册同名指标（http_requests_total 等），互不冲突。
// 用默认全局 registry 的话第二次 MustRegister 会 panic——单跑 100% 正常，
// 进外壳第二个模块起来就崩，这正是 §12.5.2 点名的那条雷。
func TestNewRegistry_两个Runtime各自的Registry注册同名指标都成功(t *testing.T) {
	rt1 := &Runtime{Registry: NewRegistry()}
	rt2 := &Runtime{Registry: NewRegistry()}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("两个独立 Registry 各自注册同名指标不该 panic：%v", r)
		}
	}()

	_ = NewGinEngine(&Runtime{
		Registry: rt1.Registry,
		Tracer:   noopTracerForTest(),
		Logger:   discardLoggerForTest(),
	})
	_ = NewGinEngine(&Runtime{
		Registry: rt2.Registry,
		Tracer:   noopTracerForTest(),
		Logger:   discardLoggerForTest(),
	})
}
