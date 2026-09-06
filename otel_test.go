package besdk

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
)

// §7.5：otelBaseUrl 为空时装 Blackhole Exporter——不报错、不阻塞业务线程，
// 返回的 shutdown 必须能正常调用。
func TestInitOTel_baseURL为空时不报错不阻塞(t *testing.T) {
	ctx := context.Background()
	shutdown, err := InitOTel(ctx, "test-service", "")
	if err != nil {
		t.Fatalf("otelBaseURL 为空不该报错，got %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown 函数不该是 nil")
	}

	done := make(chan error, 1)
	go func() { done <- shutdown(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown 不该报错，got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown 阻塞超过 2 秒——blackhole 模式不该有任何网络等待")
	}
}

// Collector 连不上时，业务侧创建/结束 span 必须照常返回，不能抛异常、不能挂起。
func TestInitOTel_Collector连不上业务调用照样返回(t *testing.T) {
	ctx := context.Background()
	// 一个必然连不上、也不会阻塞 DNS 解析的地址（TEST-NET-1，RFC 5737）
	shutdown, err := InitOTel(ctx, "test-service", "http://192.0.2.1:4318")
	if err != nil {
		t.Fatalf("InitOTel 本身不该因为 Collector 连不上而报错，got %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}()

	tracer := otel.Tracer("test-service")
	done := make(chan struct{})
	go func() {
		_, span := tracer.Start(ctx, "probe")
		span.End()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("创建/结束 span 不该因为 Collector 连不上而阻塞")
	}
}
