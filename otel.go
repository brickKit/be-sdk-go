package besdk

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// InitOTel 初始化 OTel SDK（当前只接 TracerProvider；MeterProvider 走
// Runtime.Meter，实现同理，SOP-L 表里指标那半靠 metrics.go 的 Registry
// 单独覆盖，这里不重复）。
//
// ⚠️ otelBaseUrl 为空时装 Blackhole Exporter，不是报错、不是阻塞业务线程
// （设计书 §7.5：连不上必须静默丢弃）。这是 Bootstrap 唯一调用它的地方——
// 调用方（RunStandalone 或外壳）恰好调一次，模块自己永远不碰（§12.5.2）。
func InitOTel(ctx context.Context, serviceName, otelBaseURL string) (shutdown func(context.Context) error, err error) {
	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(semconv.ServiceName(serviceName)))
	if err != nil {
		return nil, err
	}

	if otelBaseURL == "" {
		// Blackhole：SDK TracerProvider 一个 SpanProcessor 都不挂。
		// span 该怎么创建就怎么创建（业务代码零感知），但没有任何导出器
		// 消费它们——不联网、不重试、不阻塞，创建即丢弃。
		tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res))
		otel.SetTracerProvider(tp)
		return tp.Shutdown, nil
	}

	// ⚠️ 导出器用异步批处理（BatchSpanProcessor），不是每个 span 同步发送。
	// otlptracehttp 的连接失败会在后台重试，不会让 tracer.Start()/span.End()
	// 阻塞或报错——这正是「连不上必须静默丢弃」在代码层面的落点。
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(otelBaseURL))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(5*time.Second)),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
