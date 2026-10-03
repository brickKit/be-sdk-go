package besdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/gin-gonic/gin"
)

type fakeGuardian struct{ seen []Guard }

func (f *fakeGuardian) check(c *gin.Context, g Guard) *problem.Error {
	f.seen = append(f.seen, g)
	if g == PermKey("deny.me") {
		return problem.Be("MISSING_PERMISSION", map[string]string{"permission": "deny.me"})
	}
	return nil
}

func newTestRouter(t *testing.T) (*gin.Engine, *Router, *fakeGuardian) {
	gin.SetMode(gin.ReleaseMode) // process-wide in gin; tests only — the runtime never calls it
	g := &fakeGuardian{}
	eng := gin.New()
	r := newRouter(eng, routerConfig{componentID: "erp/sales", catalogue: problem.NewCatalogue(), locale: "en",
		defaultDeadline: 10 * time.Second, guardian: g})
	return eng, r, g
}

func TestRouterPrefixGuardAndDeadline(t *testing.T) {
	eng, r, g := newTestRouter(t)
	var dl time.Duration
	GET(r, "/orders/:id", PermKey("erp.sales.view"), func(c *gin.Context) {
		d, _ := c.Request.Context().Deadline()
		dl = time.Until(d)
		Respond(c, 200, map[string]string{"id": c.Param("id")})
	}, Timeout(15*time.Second))
	rec := httptest.NewRecorder()
	eng.ServeHTTP(rec, httptest.NewRequest("GET", "/erp/sales/orders/7", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"id":"7"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if dl < 14*time.Second || dl > 15*time.Second {
		t.Fatalf("route deadline %v", dl)
	}
	if len(g.seen) != 1 || g.seen[0] != PermKey("erp.sales.view") {
		t.Fatalf("guard %v", g.seen)
	}
}

func TestRouterGuardRefusal(t *testing.T) {
	eng, r, _ := newTestRouter(t)
	called := false
	POST(r, "/x", PermKey("deny.me"), func(c *gin.Context) { called = true })
	rec := httptest.NewRecorder()
	eng.ServeHTTP(rec, httptest.NewRequest("POST", "/erp/sales/x", nil))
	if rec.Code != 403 || called {
		t.Fatalf("%d called=%v", rec.Code, called)
	}
	var p problem.Problem
	_ = json.Unmarshal(rec.Body.Bytes(), &p)
	if p.Reason != "MISSING_PERMISSION" || p.Metadata["permission"] != "deny.me" {
		t.Fatalf("%+v", p)
	}
}

func TestRouterBodyLimit(t *testing.T) {
	eng, r, _ := newTestRouter(t)
	type in struct{ Name string }
	POST(r, "/small", Public, func(c *gin.Context) {
		v, err := Bind[in](c)
		if err != nil {
			Fail(c, err)
			return
		}
		Respond(c, 200, v)
	}, BodyLimit(16))
	// declared length over the limit: refused before reading
	rec := httptest.NewRecorder()
	eng.ServeHTTP(rec, httptest.NewRequest("POST", "/erp/sales/small", strings.NewReader(`{"name":"`+strings.Repeat("x", 40)+`"}`)))
	if rec.Code != 413 || !strings.Contains(rec.Body.String(), "BODY_TOO_LARGE") {
		t.Fatalf("declared: %d %s", rec.Code, rec.Body)
	}
	// unknown length (chunked): refused while reading
	req := httptest.NewRequest("POST", "/erp/sales/small", strings.NewReader(`{"name":"`+strings.Repeat("x", 40)+`"}`))
	req.ContentLength = -1
	rec = httptest.NewRecorder()
	eng.ServeHTTP(rec, req)
	if rec.Code != 413 {
		t.Fatalf("streamed: %d %s", rec.Code, rec.Body)
	}
	// within the limit
	rec = httptest.NewRecorder()
	eng.ServeHTTP(rec, httptest.NewRequest("POST", "/erp/sales/small", strings.NewReader(`{"name":"ok"}`)))
	if rec.Code != 200 {
		t.Fatalf("ok: %d %s", rec.Code, rec.Body)
	}
}

func TestBindInvalidJSON(t *testing.T) {
	eng, r, _ := newTestRouter(t)
	POST(r, "/j", Public, func(c *gin.Context) {
		_, err := Bind[struct{ N int }](c)
		Fail(c, err)
	})
	rec := httptest.NewRecorder()
	eng.ServeHTTP(rec, httptest.NewRequest("POST", "/erp/sales/j", strings.NewReader(`{"n":"x"}`)))
	var p problem.Problem
	_ = json.Unmarshal(rec.Body.Bytes(), &p)
	if rec.Code != 400 || p.Code != "INVALID_ARGUMENT" || p.Domain != "erp/sales" || len(p.Violations) != 1 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestRouterRejectsGuardlessRoute(t *testing.T) {
	_, r, _ := newTestRouter(t)
	defer func() {
		if recover() == nil {
			t.Fatal("a route without a guard must be refused at registration")
		}
	}()
	GET(r, "/x", nil, func(c *gin.Context) {})
}

func TestFailMapsContextErrors(t *testing.T) {
	eng, r, _ := newTestRouter(t)
	GET(r, "/d", Public, func(c *gin.Context) { Fail(c, context.DeadlineExceeded) })
	rec := httptest.NewRecorder()
	eng.ServeHTTP(rec, httptest.NewRequest("GET", "/erp/sales/d", nil))
	if rec.Code != 504 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	_ = http.StatusOK
}
