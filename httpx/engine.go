package httpx

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Headers of protocol chapter 04.
const (
	HeaderRequestID = "X-Request-Id"
	HeaderTimeoutMs = "Be-Timeout-Ms"
)

// Platform answers the endpoints the SDK owns (protocol chapter 01). They bypass the middleware chain and are not
// access-logged at INFO.
type Platform struct {
	// Ready reports readiness; when not ready it returns the state and, for a shell, the members still waited for.
	Ready func() (ready bool, state string, waiting []string)
	// Describe returns the self-description document.
	Describe func() any
	// Metrics serves the metrics (the member's registry, or the shell's aggregate).
	Metrics http.Handler
}

// Deps are what the engine needs from the member's runtime.
type Deps struct {
	Member   obs.Member
	Logger   *slog.Logger
	Registry *prometheus.Registry
	Tracer   trace.Tracer
	Platform Platform
}

var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// NewEngine builds the member's user-surface engine. Routes must have been validated.
func NewEngine(d Deps, routes *Routes) (*gin.Engine, error) {
	if err := routes.Validate(); err != nil {
		return nil, err
	}
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "be_http_server_duration_seconds", Help: "User-surface request duration by route and status.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})
	if err := d.Registry.Register(duration); err != nil {
		return nil, fmt.Errorf("register http metrics: %w", err)
	}

	e := gin.New()
	e.HandleMethodNotAllowed = false
	e.Use(recovery(d))
	platform(e, d)

	for _, r := range routes.List() {
		r := r
		e.Handle(r.Method, r.Path, func(g *gin.Context) { serve(g, d, r, duration) })
	}
	e.NoRoute(func(g *gin.Context) {
		errs.New(errs.ReasonNotFound, "no such route").WriteHTTP(g.Writer, traceIDOf(g.Request.Context()))
	})
	return e, nil
}

func platform(e *gin.Engine, d Deps) {
	health := func(g *gin.Context) { g.String(http.StatusOK, "ok") }
	e.GET("/healthz", health)
	e.HEAD("/healthz", health)
	e.GET("/readyz", func(g *gin.Context) {
		ready, state, waiting := d.Platform.Ready()
		if ready {
			g.String(http.StatusOK, "ok")
			return
		}
		er := errs.New(errs.ReasonNotReady, "not ready").With("state", state)
		if len(waiting) > 0 {
			er = er.With("waiting", joinComma(waiting))
		}
		er.WriteHTTP(g.Writer, "")
	})
	e.GET("/.well-known/be-component", func(g *gin.Context) { g.JSON(http.StatusOK, d.Platform.Describe()) })
	if d.Platform.Metrics != nil {
		e.GET("/metrics", gin.WrapH(d.Platform.Metrics))
	} else {
		e.GET("/metrics", gin.WrapH(promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{})))
	}
}

func joinComma(s []string) string {
	out := ""
	for i, x := range s {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}

// recovery turns a panic into INTERNAL and logs the stack; it never lets the process die.
func recovery(d Deps) gin.HandlerFunc {
	return func(g *gin.Context) {
		defer func() {
			if p := recover(); p != nil {
				d.Logger.Error("panic while serving a request", "panic", fmt.Sprint(p), "stack", string(debug.Stack()),
					"path", g.Request.URL.Path)
				if !g.Writer.Written() {
					errs.New(errs.ReasonInternal, "").WriteHTTP(g.Writer, traceIDOf(g.Request.Context()))
				}
				g.Abort()
			}
		}()
		g.Next()
	}
}

func traceIDOf(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// serve runs one request through the chain: request ID, trace, deadline, body limit, handler, error mapping, access
// log and metrics.
func serve(g *gin.Context, d Deps, r Route, duration *prometheus.HistogramVec) {
	start := time.Now()
	req := g.Request

	rid := req.Header.Get(HeaderRequestID)
	if !requestIDRe.MatchString(rid) {
		rid = uuid.Must(uuid.NewV7()).String()
	}
	g.Header(HeaderRequestID, rid)

	ctx := obs.Propagator.Extract(req.Context(), propagation.HeaderCarrier(req.Header))
	ctx, span := d.Tracer.Start(ctx, r.Method+" "+r.Path, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	traceID := span.SpanContext().TraceID().String()
	log := d.Logger.With("request_id", rid, "trace_id", traceID)

	err := func() error {
		limit := r.deadline()
		if h := req.Header.Get(HeaderTimeoutMs); h != "" {
			ms, perr := strconv.ParseInt(h, 10, 64)
			if perr != nil || ms <= 0 {
				return errs.New(errs.ReasonRequestInvalid, HeaderTimeoutMs+" must be a positive integer").
					WithViolations(errs.Violation{Field: "header:" + HeaderTimeoutMs, Reason: "INVALID", Description: "positive integer of milliseconds"})
			}
			limit = min(limit, time.Duration(ms)*time.Millisecond)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, limit)
		defer cancel()
		g.Request = req.WithContext(ctx)
		g.Request.Body = http.MaxBytesReader(g.Writer, req.Body, r.maxBody())
		return r.Handle(&Context{gin: g, route: r, requestID: rid, traceID: traceID, logger: log})
	}()

	if err != nil {
		e := errs.From(err)
		if lvl, ok := errs.LogLevel(e.Code); ok {
			log.Log(ctx, lvl, "request failed", "reason", e.Reason, "domain", e.Domain, "error", e.Error())
		}
		if !g.Writer.Written() {
			e.WriteHTTP(g.Writer, traceID)
		}
	}
	status := g.Writer.Status()
	elapsed := time.Since(start)
	duration.WithLabelValues(r.Method, r.Path, strconv.Itoa(status)).Observe(elapsed.Seconds())
	log.Log(ctx, errs.AccessLogLevel(status), "access", "method", r.Method, "route", r.Path, "status", status,
		"duration_ms", elapsed.Milliseconds())
}
