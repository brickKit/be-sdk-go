package besdk

import "github.com/prometheus/client_golang/prometheus"

// NewRegistry 给每个模块建一个独立的 Prometheus Registry（不是默认全局
// 那个）。用默认全局 registry 的症状：Go `MustRegister` panic、Python 抛
// `Duplicated timeseries`——单跑 100% 正常，进外壳第二个模块起来就崩
// （§12.5.2）。RunStandalone 调用它填 Runtime.Registry，恰好一次。
//
// 实现放 Task 7 用 TDD 补：核心场景是「合并态下 N 个模块各自调一次，
// 互不冲突」。
func NewRegistry() *prometheus.Registry {
	panic("未实现：Task 7 补")
}
