package besdk

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestRuntime(t *testing.T) (*Runtime, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	return &Runtime{
		Registry: prometheus.NewRegistry(),
		Tracer:   tp.Tracer("test"),
		Logger:   discardLoggerForTest(),
	}, sr
}

func TestNewGinEngine_挂了healthz与metrics(t *testing.T) {
	rt, _ := newTestRuntime(t)
	engine := NewGinEngine(rt)

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/healthz 期望 200，得到 %d", w.Code)
	}

	w2 := httptest.NewRecorder()
	engine.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("/metrics 期望 200，得到 %d", w2.Code)
	}
}

// ⚠️ 实测踩坑：mdm-customer 真的用 brickkit up 起容器后一直 unhealthy，
// 但组件日志里根本没有 panic，只有一条条 404——平台生成的健康检查是
// `wget -q --spider http://.../healthz`，而 --spider 模式发的是 HEAD
// 请求，不是 GET。/healthz 只注册了 engine.GET，Gin 的路由不会让 GET
// 处理器顺带接住 HEAD（不像标准库 http.ServeMux 那样自动关联两者），
// 于是每一次健康检查探测都落进 Gin 的 NoRoute、404。be-sdk-go 自己的
// 测试从头到尾只发过 GET，从没模拟过真实健康检查用的 HEAD，这条路径
// 一次都没被走到过。
func TestNewGinEngine_healthz对HEAD请求也响应200(t *testing.T) {
	rt, _ := newTestRuntime(t)
	engine := NewGinEngine(rt)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodHead, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("wget --spider 发的是 HEAD，/healthz 对 HEAD 也应该 200，得到 %d", w.Code)
	}
}

// §12.3.6：/healthz 只查本进程存活，这里没有任何依赖可查——engine 本身
// 就没有给 /healthz 接任何 DB/NATS 探测的机会，200 恒成立。
func TestNewGinEngine_healthz不查依赖(t *testing.T) {
	rt, _ := newTestRuntime(t)
	engine := NewGinEngine(rt)
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/healthz 应恒为 200（不查依赖），得到 %d", w.Code)
	}
}

func TestNewGinEngine_每个请求产生一个span与一条RED指标(t *testing.T) {
	rt, sr := newTestRuntime(t)
	engine := NewGinEngine(rt)
	engine.GET("/ping", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ping", nil))

	if len(sr.Ended()) != 1 {
		t.Fatalf("期望产生 1 个 span，实际 %d 个", len(sr.Ended()))
	}

	mw := httptest.NewRecorder()
	engine.ServeHTTP(mw, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(mw.Body.String(), "http_requests_total") {
		t.Fatalf("/metrics 输出里应该有 http_requests_total，实际：%s", mw.Body.String())
	}
}

// TestNewGinEngine_RED指标与访问日志记录真实状态码而不是200 是真机部署
// infra-iam-casdoor 时发现的真实 bug 的回归测试：业务 handler 用
// c.Error(status.Error(...)) 上报错误（不是自己 c.JSON 显式设状态码）
// 时，Gin 的默认状态码在 recoveryAndErrorMappingMiddleware 把它改写成
// 真实值之前一直是 200——如果 RED 指标/访问日志所在的中间件排在
// recoveryAndErrorMappingMiddleware **前面**（Gin 中间件 after-Next 代码
// 按注册顺序倒序执行，越早注册的越晚才读到"最终"状态），它们会读到
// 还没被改写的默认值，把一个真实的 403 记成 200。真实响应本身是对的
// （客户端拿到的确实是 403），只有指标/日志记录不对——这是最容易被
// 忽略的一类症状，因为端到端功能测试全部会通过，只有专门去看
// Prometheus/日志才会发现。
func TestNewGinEngine_RED指标与访问日志记录真实状态码而不是200(t *testing.T) {
	rt, _ := newTestRuntime(t)
	engine := NewGinEngine(rt)
	engine.GET("/denied", func(c *gin.Context) {
		_ = c.Error(status.Error(codes.PermissionDenied, "无权限"))
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/denied", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("真实响应应该是 403，实际 %d", w.Code)
	}

	mw := httptest.NewRecorder()
	engine.ServeHTTP(mw, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := mw.Body.String()
	if strings.Contains(body, `route="/denied",status="OK"} 1`) {
		t.Fatalf("RED 指标把一个真实的 403 记成了 200：\n%s", body)
	}
	if !strings.Contains(body, `route="/denied",status="Forbidden"} 1`) {
		t.Fatalf("RED 指标应该记录真实状态码 403（Forbidden），实际：\n%s", body)
	}
}

func TestNewGinEngine_panic被recover成500而不退进程(t *testing.T) {
	rt, _ := newTestRuntime(t)
	engine := NewGinEngine(rt)
	engine.GET("/boom", func(c *gin.Context) { panic("business handler blew up") })

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("panic 之后期望 500，得到 %d", w.Code)
	}
	// 能跑到这里说明测试进程本身没有被这次 panic 带崩——这就是"不退进程"
}
