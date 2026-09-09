package besdk

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewGinEngine 发一个已挂好全部中间件的 Gin engine：OTel、request-id、
// error → gRPC status 映射、结构化访问日志、RED 指标，并已挂 /healthz
// 与 /metrics。
//
// ⚠️ 组件不许自己 gin.New()——中间件漏一条不会报错，只是那个组件从此没有
// trace、没有 RED 指标，而 Grafana 上看起来只是「这个组件流量低」。
//
// ⚠️ /healthz 只检查本进程存活，不查依赖、不查数据库（§12.3.6：一个下游
// 抖动会让所有上游同时被判不健康并重启，合并态下更狠）。
func NewGinEngine(rt *Runtime) *gin.Engine {
	reqTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP 请求总数（RED 的 Rate + Errors）",
	}, []string{"method", "route", "status"})
	reqDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP 请求耗时（RED 的 Duration）",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})
	rt.Registry.MustRegister(reqTotal, reqDuration)

	engine := gin.New()

	// ⚠️ recoveryAndErrorMappingMiddleware 必须排在 redMetricsMiddleware/
	// accessLogMiddleware 之后（真机测试踩出来的真实 bug，见
	// docs/authz-protocol.md 同一批实测记录，或直接对照下面的例子）：
	// Gin 中间件 c.Next() 之后的代码按注册顺序**倒序**执行，
	// recoveryAndErrorMappingMiddleware 是"业务 handler 调
	// c.Error(status.Error(...))、这里事后翻译成真实 HTTP 状态码"的
	// 那一层——它排在 redMetrics/accessLog **前面**时，这两个中间件的
	// after-Next 代码会先跑、读到的是 Gin 还没被改写过的默认状态码
	// （200），而不是稍后才被 recoveryAndErrorMappingMiddleware 改写
	// 出来的真实状态码；等 recoveryAndErrorMappingMiddleware 自己的
	// after-Next 代码跑到、调 c.JSON(403, ...) 时，RED 指标与访问日志
	// 早就已经记完"200"了——症状是：真实响应确实是 403（客户端收到的
	// 没问题），但 Prometheus 的 http_requests_total 与访问日志里这条
	// 请求永远显示 200，Grafana 的错误率面板因此对所有走 c.Error(...)
	// 路径的失败视而不见。现在排在最后，它的 after-Next 代码在倒序里
	// 最先执行，把状态码改对之后，RED 指标与访问日志才读到真实值。
	engine.Use(
		requestIDMiddleware(),
		tracingMiddleware(rt),
		redMetricsMiddleware(reqTotal, reqDuration),
		accessLogMiddleware(rt),
		recoveryAndErrorMappingMiddleware(rt),
	)

	// §12.3.6：/healthz 只答「进程还活着」，不做任何依赖探测。
	// ⚠️ GET 与 HEAD 都要注册：平台生成的健康检查是
	// `wget -q --spider .../healthz`，--spider 发的是 HEAD——Gin 的路由
	// 不会像标准库 http.ServeMux 那样让 GET 处理器顺带接住 HEAD，只注册
	// GET 会让每一次健康检查探测都落进 NoRoute、404（实测踩坑）。
	healthz := func(c *gin.Context) { c.Status(http.StatusOK) }
	engine.GET("/healthz", healthz)
	engine.HEAD("/healthz", healthz)
	engine.GET("/metrics", gin.WrapH(promhttp.HandlerFor(rt.Registry, promhttp.HandlerOpts{})))

	return engine
}

// requestIDMiddleware 给每个请求分配一个 ID（客户端已带则透传），供跨组件
// 排障串联日志用。
func requestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader("X-Request-Id")
		if id == "" {
			var b [16]byte
			_, _ = rand.Read(b[:])
			id = hex.EncodeToString(b[:])
		}
		c.Set("request_id", id)
		c.Header("X-Request-Id", id)
		c.Next()
	}
}

// tracingMiddleware 给每个请求开一个 span。用的是 rt.Tracer——它已经由
// Bootstrap/InitOTel 填好（otelBaseUrl 为空时是 Blackhole Exporter），
// 这里不重复判断有没有配置好，那是 InitOTel 的职责边界。
func tracingMiddleware(rt *Runtime) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, span := rt.Tracer.Start(c.Request.Context(), c.Request.Method+" "+c.FullPath())
		defer span.End()
		span.SetAttributes(
			attribute.String("http.method", c.Request.Method),
			attribute.String("http.route", c.FullPath()),
		)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		if c.Writer.Status() >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(c.Writer.Status()))
		}
	}
}

// recoveryAndErrorMappingMiddleware 兜住 panic（转成 500，不让一个请求
// 崩掉整个进程），并把业务 handler 塞进 c.Errors 的 gRPC status 错误
// 翻译成对应的 HTTP 状态码——HTTP 与 gRPC 两条对外接口共用同一套业务错误
// 类型，业务代码不用为两种协议各写一遍错误处理。
func recoveryAndErrorMappingMiddleware(rt *Runtime) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				rt.Logger.Error("请求处理 panic", "recovered", r, "path", c.FullPath())
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": "internal server error",
				})
			}
		}()
		c.Next()

		if len(c.Errors) == 0 {
			return
		}
		err := c.Errors.Last().Err
		if st, ok := status.FromError(err); ok {
			c.JSON(grpcCodeToHTTPStatus(st.Code()), gin.H{"error": st.Message()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

// grpcCodeToHTTPStatus 抄的是社区通行的 grpc-gateway 映射表。
func grpcCodeToHTTPStatus(code grpccodes.Code) int {
	switch code {
	case grpccodes.OK:
		return http.StatusOK
	case grpccodes.InvalidArgument, grpccodes.FailedPrecondition, grpccodes.OutOfRange:
		return http.StatusBadRequest
	case grpccodes.Unauthenticated:
		return http.StatusUnauthorized
	case grpccodes.PermissionDenied:
		return http.StatusForbidden
	case grpccodes.NotFound:
		return http.StatusNotFound
	case grpccodes.AlreadyExists, grpccodes.Aborted:
		return http.StatusConflict
	case grpccodes.ResourceExhausted:
		return http.StatusTooManyRequests
	case grpccodes.Unimplemented:
		return http.StatusNotImplemented
	case grpccodes.Unavailable:
		return http.StatusServiceUnavailable
	case grpccodes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

// redMetricsMiddleware 是 RED 方法的 R/E/D 三者里 Rate 与 Errors 那两个
// （由 reqTotal 的 status 标签区分 2xx/4xx/5xx），Duration 由 reqDuration
// 记录。采集间隔由 Prometheus 抓取侧的 scrape_interval 决定（§7.4：15s，
// 本地部署资源克制，这里不重复配置）。
func redMetricsMiddleware(reqTotal *prometheus.CounterVec, reqDuration *prometheus.HistogramVec) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		reqTotal.WithLabelValues(c.Request.Method, route, http.StatusText(c.Writer.Status())).Inc()
		reqDuration.WithLabelValues(c.Request.Method, route).Observe(time.Since(start).Seconds())
	}
}

// accessLogMiddleware 只记结构化字段（method/path/status/耗时/request_id/
// trace_id），不记请求体——业务代码要记 payload 自己调 RedactPII 之后
// 再记，通用访问日志不做这件事（避免整条链路每个请求都要走一次脱敏判断）。
func accessLogMiddleware(rt *Runtime) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		rt.Logger.Info("http_request",
			"method", c.Request.Method,
			"path", c.FullPath(),
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", c.GetString("request_id"),
		)
	}
}
