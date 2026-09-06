package besdk

import "context"

// InitOTel 初始化 OTel SDK（TracerProvider + MeterProvider）。
//
// ⚠️ otelBaseUrl 为空时装 Blackhole Exporter，不是报错、不是阻塞业务线程
// （设计书 §7.5：连不上必须静默丢弃）。这是 Bootstrap 唯一调用它的地方——
// 调用方（RunStandalone 或外壳）恰好调一次，模块自己永远不碰（§12.5.2）。
//
// 实现放 Task 7 用 TDD 补：最要紧的一条属性测试是「otelBaseUrl 为空时，
// 返回的 shutdown 函数必须能正常调用且不 panic、不阻塞」。
func InitOTel(ctx context.Context, serviceName, otelBaseURL string) (shutdown func(context.Context) error, err error) {
	panic("未实现：Task 7 补")
}
