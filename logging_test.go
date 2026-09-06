package besdk

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNewLogger_输出是合法JSON且带componentID(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "mdm/customer")
	logger.Info("hello", "k", "v")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("输出不是合法 JSON：%v\n原始输出：%s", err, buf.String())
	}
	if m["component_id"] != "mdm/customer" {
		t.Fatalf("期望 component_id=mdm/customer，得到 %v", m["component_id"])
	}
	if m["msg"] != "hello" || m["k"] != "v" {
		t.Fatalf("字段丢失：%v", m)
	}
}

func TestNewLogger_有span时自动带trace_id(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(nil)

	ctx, span := tp.Tracer("test").Start(context.Background(), "probe")
	defer span.End()

	var buf bytes.Buffer
	logger := newLogger(&buf, "mdm/customer")
	logger.InfoContext(ctx, "with trace")

	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("输出不是合法 JSON：%v", err)
	}
	traceID, _ := m["trace_id"].(string)
	if traceID == "" || traceID != span.SpanContext().TraceID().String() {
		t.Fatalf("trace_id 未正确注入：got %v, want %v", m["trace_id"], span.SpanContext().TraceID().String())
	}
}

func TestRedactPII_已知敏感字段脱敏(t *testing.T) {
	in := []byte(`{"phone":"13800001111","password":"s3cr3t","name":"张三"}`)
	out := RedactPII(in)
	s := string(out)
	if strings.Contains(s, "13800001111") || strings.Contains(s, "s3cr3t") {
		t.Fatalf("敏感字段没有被脱敏：%s", s)
	}
	if !strings.Contains(s, "张三") {
		t.Fatalf("非敏感字段不该被误伤：%s", s)
	}
}

func TestRedactPII_超2KB截断(t *testing.T) {
	big := bytes.Repeat([]byte("a"), 3000)
	out := RedactPII(big)
	if len(out) > 2048 {
		t.Fatalf("期望截断到 2KB 以内，实际 %d 字节", len(out))
	}
}
