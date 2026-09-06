package besdk

import (
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// 本文件是多个 *_test.go 共用的测试夹具，不含任何断言。

func noopTracerForTest() trace.Tracer {
	return noop.NewTracerProvider().Tracer("test")
}

func discardLoggerForTest() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
