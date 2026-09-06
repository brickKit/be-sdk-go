package besdk

import "github.com/prometheus/client_golang/prometheus"

// NewRegistry 给每个模块建一个独立的 Prometheus Registry（不是默认全局
// 那个）。用默认全局 registry 的症状：Go `MustRegister` panic、Python 抛
// `Duplicated timeseries`——单跑 100% 正常，进外壳第二个模块起来就崩
// （§12.5.2）。RunStandalone 调用它填 Runtime.Registry，恰好一次。
func NewRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}
