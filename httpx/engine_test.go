package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/obs"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func init() { gin.SetMode(gin.ReleaseMode) }

// admitAll stands in for the guard: these tests are about the chain around it (the guard has its own tests).
type admitAll struct{}

func (admitAll) Admit(ctx context.Context, _ Route, _ *http.Request) (context.Context, error) {
	return ctx, nil
}

type harness struct {
	engine http.Handler
	logs   *bytes.Buffer
	ready  bool
}

func newHarness(t *testing.T, routes *Routes) *harness {
	t.Helper()
	h := &harness{logs: &bytes.Buffer{}}
	m := obs.Member{ID: "demo/alpha", Version: "3.0.0"}
	d := Deps{Member: m, Logger: obs.NewLogger(m, slog.LevelDebug, h.logs), Registry: prometheus.NewRegistry(),
		Tracer: sdktrace.NewTracerProvider().Tracer("test"), Guard: admitAll{},
		Platform: Platform{
			Ready:    func() (bool, string, []string) { return h.ready, "starting", []string{"demo/beta"} },
			Describe: func() any { return map[string]string{"component_id": m.ID} },
		}}
	e, err := NewEngine(d, routes)
	require.NoError(t, err)
	h.engine = e
	return h
}

func (h *harness) do(method, path string, body io.Reader, header map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.engine.ServeHTTP(rec, req)
	return rec
}

func problem(t *testing.T, rec *httptest.ResponseRecorder) errs.Problem {
	t.Helper()
	assert.Equal(t, errs.ProblemContentType, rec.Header().Get("Content-Type"))
	var p errs.Problem
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	return p
}

func TestRouteValidation(t *testing.T) {
	var rs Routes
	h := func(*Context) error { return nil }
	rs.GET("/api/a", "", h)
	rs.Add(Route{Method: "GET", Path: "/api/b", Public: true, Operation: "x", Handle: h})
	rs.GET("/healthz", "op", h)
	rs.GET("/api/c", "op", h)
	rs.GET("/api/c", "op", h)
	rs.GET("/api/d", "op", nil)
	err := rs.Validate()
	require.Error(t, err)
	for _, want := range []string{"/api/a: declares no operation", "/api/b: a public route", "/healthz belongs to the platform",
		"/api/c: registered twice", "/api/d: no handler"} {
		assert.Contains(t, err.Error(), want)
	}
	var ok Routes
	ok.GET("/api/x", "demo.alpha.read", h)
	ok.Add(Route{Method: "GET", Path: "/api/open", Public: true, Handle: h})
	assert.NoError(t, ok.Validate())
}

func TestRequestIDAndDeadline(t *testing.T) {
	var seen time.Duration
	var rs Routes
	rs.GET("/api/x", "op", func(c *Context) error {
		dl, ok := c.Ctx().Deadline()
		require.True(t, ok)
		seen = time.Until(dl)
		return c.JSON(200, map[string]string{"request_id": c.RequestID(), "trace_id": c.TraceID()})
	})
	rs.Add(Route{Method: "GET", Path: "/api/long", Operation: "op", Orchestration: true, Handle: func(c *Context) error {
		dl, _ := c.Ctx().Deadline()
		seen = time.Until(dl)
		return c.NoContent(204)
	}})
	h := newHarness(t, &rs)

	rec := h.do("GET", "/api/x", nil, map[string]string{HeaderRequestID: "abc-123"})
	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "abc-123", rec.Header().Get(HeaderRequestID))
	assert.InDelta(t, DefaultDeadline.Seconds(), seen.Seconds(), 0.5, "no header: the route's default")

	rec = h.do("GET", "/api/x", nil, map[string]string{HeaderRequestID: "bad id with spaces", HeaderTimeoutMs: "1500"})
	assert.NotEqual(t, "bad id with spaces", rec.Header().Get(HeaderRequestID), "an invalid request ID is replaced")
	assert.Len(t, rec.Header().Get(HeaderRequestID), 36)
	assert.InDelta(t, 1.5, seen.Seconds(), 0.3, "a shorter remaining time wins")

	h.do("GET", "/api/x", nil, map[string]string{HeaderTimeoutMs: "60000"})
	assert.InDelta(t, DefaultDeadline.Seconds(), seen.Seconds(), 0.5, "a deadline never grows")

	h.do("GET", "/api/long", nil, nil)
	assert.InDelta(t, OrchestrationDeadline.Seconds(), seen.Seconds(), 0.5)

	rec = h.do("GET", "/api/x", nil, map[string]string{HeaderTimeoutMs: "-5"})
	assert.Equal(t, 400, rec.Code)
	assert.Equal(t, errs.ReasonRequestInvalid, problem(t, rec).Reason)
}

func TestErrorsPanicsBodiesAndUnknownRoutes(t *testing.T) {
	var rs Routes
	rs.GET("/api/busy", "op", func(*Context) error {
		return errs.New(errs.ReasonServiceBusy, "busy").WithRetryAfter(2 * time.Second)
	})
	rs.GET("/api/boom", "op", func(*Context) error { panic("db password is hunter2") })
	rs.Add(Route{Method: "POST", Path: "/api/small", Operation: "op", MaxBody: 16, Handle: func(c *Context) error {
		var v map[string]any
		if err := c.BindJSON(&v); err != nil {
			return err
		}
		return c.NoContent(204)
	}})
	h := newHarness(t, &rs)

	rec := h.do("GET", "/api/busy", nil, nil)
	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, "2", rec.Header().Get("Retry-After"))
	p := problem(t, rec)
	assert.Equal(t, errs.ReasonServiceBusy, p.Reason)
	assert.Len(t, p.TraceID, 32)

	rec = h.do("GET", "/api/boom", nil, nil)
	assert.Equal(t, 500, rec.Code)
	assert.NotContains(t, rec.Body.String(), "hunter2")
	assert.Contains(t, h.logs.String(), "panic while serving a request")

	rec = h.do("POST", "/api/small", strings.NewReader(`{"a":"0123456789abcdef"}`), nil)
	assert.Equal(t, 413, rec.Code)
	assert.Equal(t, errs.ReasonBodyTooLarge, problem(t, rec).Reason)

	rec = h.do("POST", "/api/small", strings.NewReader(`{"a":`), nil)
	assert.Equal(t, 400, rec.Code)
	assert.Equal(t, errs.ReasonRequestInvalid, problem(t, rec).Reason)

	rec = h.do("GET", "/api/nothing", nil, nil)
	assert.Equal(t, 404, rec.Code)
	assert.Equal(t, errs.ReasonNotFound, problem(t, rec).Reason)
}

func TestPlatformEndpoints(t *testing.T) {
	var rs Routes
	rs.GET("/api/x", "op", func(c *Context) error { return c.NoContent(204) })
	h := newHarness(t, &rs)

	assert.Equal(t, 200, h.do("GET", "/healthz", nil, nil).Code)
	assert.Equal(t, 200, h.do("HEAD", "/healthz", nil, nil).Code)

	rec := h.do("GET", "/readyz", nil, nil)
	assert.Equal(t, 503, rec.Code)
	p := problem(t, rec)
	assert.Equal(t, errs.ReasonNotReady, p.Reason)
	assert.Equal(t, map[string]string{"state": "starting", "waiting": "demo/beta"}, p.Metadata)
	h.ready = true
	assert.Equal(t, 200, h.do("GET", "/readyz", nil, nil).Code)

	assert.Contains(t, h.do("GET", "/.well-known/be-component", nil, nil).Body.String(), "demo/alpha")

	before := h.logs.Len()
	h.do("GET", "/healthz", nil, nil)
	assert.Equal(t, before, h.logs.Len(), "platform endpoints are not access-logged")

	h.do("GET", "/api/x", nil, nil)
	metrics := h.do("GET", "/metrics", nil, nil).Body.String()
	assert.Contains(t, metrics, `be_http_server_duration_seconds_count{method="GET",route="/api/x",status="204"} 1`)
	assert.Contains(t, h.logs.String(), `"msg":"access"`)
}

func TestMetricsAndAccessLinesOfChapter10(t *testing.T) { // P10.2, P10.3, P10.6
	var rs Routes
	rs.GET("/api/things/:id", "op", func(c *Context) error { return c.NoContent(204) })
	h := newHarness(t, &rs)
	h.do("GET", "/api/things/7", nil, map[string]string{"Authorization": "Bearer secret-token-xyz"})
	metrics := h.do("GET", "/metrics", nil, nil).Body.String()
	for _, le := range []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "+Inf"} {
		assert.Contains(t, metrics, `be_http_server_duration_seconds_bucket{method="GET",route="/api/things/{id}",status="204",le="`+le+`"}`)
	}
	var access map[string]any
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		var r map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &r))
		if r["msg"] == "access" {
			access = r
		}
	}
	require.NotNil(t, access)
	assert.Equal(t, "/api/things/{id}", access["route"], "the template in the OpenAPI form, not the path")
	assert.Regexp(t, `^[0-9a-f]{32}$`, access["trace_id"])
	assert.NotContains(t, h.logs.String(), "secret-token-xyz") // P10.4
}

func TestCookiesAndRedirects(t *testing.T) { // a login's hops (protocol 20)
	var r Routes
	r.Add(Route{Method: "GET", Path: "/api/login", Public: true, Handle: func(c *Context) error {
		c.SetCookie(&http.Cookie{Name: "be_session", Value: "s1", Path: "/", HttpOnly: true, Secure: true,
			SameSite: http.SameSiteLaxMode})
		return c.Redirect(http.StatusFound, "https://idp.test/authorize?state=x")
	}})
	r.Add(Route{Method: "GET", Path: "/api/bad", Public: true, Handle: func(c *Context) error {
		return c.Redirect(http.StatusOK, "/elsewhere")
	}})
	h := newHarness(t, &r)
	rec := h.do("GET", "/api/login", nil, nil)
	assert.Equal(t, http.StatusFound, rec.Code)
	assert.Equal(t, "https://idp.test/authorize?state=x", rec.Header().Get("Location"))
	ck := rec.Result().Cookies()
	require.Len(t, ck, 1)
	assert.True(t, ck[0].HttpOnly && ck[0].Secure && ck[0].SameSite == http.SameSiteLaxMode && ck[0].Value == "s1")
	assert.Equal(t, http.StatusInternalServerError, h.do("GET", "/api/bad", nil, nil).Code, "only 302, 303 or 307")
}

func TestByteResponses(t *testing.T) { // a downloaded document, such as an invoice's PDF
	var r Routes
	pdf := []byte("%PDF-1.7\n")
	r.Add(Route{Method: "POST", Path: "/api/doc", Public: true, Handle: func(c *Context) error {
		return c.Bytes(http.StatusOK, "application/pdf", pdf)
	}})
	r.Add(Route{Method: "POST", Path: "/api/untyped", Public: true, Handle: func(c *Context) error {
		return c.Bytes(http.StatusOK, "", pdf)
	}})
	h := newHarness(t, &r)
	rec := h.do("POST", "/api/doc", nil, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/pdf", rec.Header().Get("Content-Type"))
	assert.Equal(t, pdf, rec.Body.Bytes())
	assert.Equal(t, http.StatusInternalServerError, h.do("POST", "/api/untyped", nil, nil).Code, "a media type is required")
}
