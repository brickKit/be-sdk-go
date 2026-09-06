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
