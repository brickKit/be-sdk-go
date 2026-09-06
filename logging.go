package besdk

import (
	"context"
	"io"
	"log/slog"
	"os"
	"regexp"

	"go.opentelemetry.io/otel/trace"
)

// NewLogger 构造已注入 component_id 与 trace 上下文的结构化 JSON 日志根
// （§7.3）。Bootstrap 调用它填 Runtime.Logger，恰好一次——模块自己不许
// `logging.basicConfig()` 式的重新初始化（§12.5.2：最后一个 init 的赢，
// 22 个模块的日志格式被某个模块顶掉，而一路全绿）。
func NewLogger(componentID string) *slog.Logger {
	return newLogger(os.Stdout, componentID)
}

// newLogger 是可测的核心构造——NewLogger 只是把输出钉死在 os.Stdout。
func newLogger(w io.Writer, componentID string) *slog.Logger {
	handler := &traceContextHandler{
		inner: slog.NewJSONHandler(w, &slog.HandlerOptions{}),
	}
	return slog.New(handler).With("component_id", componentID)
}

// traceContextHandler 包一层 slog.Handler：有 span 时自动把 trace_id 挂到
// 每条日志上，业务代码不用在每次调用时手写 rt.Logger.With("trace_id", ...)。
type traceContextHandler struct {
	inner slog.Handler
}

func (h *traceContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceContextHandler) Handle(ctx context.Context, r slog.Record) error {
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		sc := span.SpanContext()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceContextHandler) WithGroup(name string) slog.Handler {
	return &traceContextHandler{inner: h.inner.WithGroup(name)}
}

// piiFieldRe 匹配已知敏感字段的 JSON key。这份清单要跟业务一起长，
// 不是写死几个字符串就完事——发现新的敏感字段类型时回来加一条。
var piiFieldRe = regexp.MustCompile(`"(phone|mobile|id_card|password|bank_card|email)"\s*:\s*"[^"]*"`)

const maxLoggedPayload = 2048 // §7.3：2KB Payload 截断

// RedactPII 把已知敏感字段脱敏、payload 超过 2KB 时截断（§7.3）。
// 业务代码记录请求体/大对象之前应该调它，而不是自己判断哪些字段敏感。
func RedactPII(payload []byte) []byte {
	redacted := piiFieldRe.ReplaceAllFunc(payload, func(m []byte) []byte {
		i := indexByte(m, ':')
		return append(m[:i+1], []byte(` "[REDACTED]"`)...)
	})
	if len(redacted) > maxLoggedPayload {
		marker := []byte("...[TRUNCATED]")
		cut := maxLoggedPayload - len(marker) // 截断标记本身也算在 2KB 预算内
		redacted = append(append([]byte{}, redacted[:cut]...), marker...)
	}
	return redacted
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}
