package besdk

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authn"
	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/gin-gonic/gin"
)

type fakeVerifier struct{ claims *authn.Claims }

func (f fakeVerifier) Verify(_ context.Context, h string) (*authn.Claims, error) {
	if h != "Bearer good" {
		return nil, authn.ErrTokenInvalid
	}
	c := *f.claims
	return &c, nil
}

type staticBundle struct{ b *authz.Bundle }

func (s staticBundle) Current() *authz.Bundle { return s.b }

func exampleBundle(t *testing.T) *authz.Bundle {
	raw, err := os.ReadFile("internal/authz/testdata/bundle.example.json")
	if err != nil {
		t.Fatal(err)
	}
	b, err := authz.ParseBundle(raw)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func guardEngine(t *testing.T, b *authz.Bundle, claims *authn.Claims) (*gin.Engine, *[]string) {
	gin.SetMode(gin.ReleaseMode)
	denied := &[]string{}
	g := &authGuardian{verifier: fakeVerifier{claims: claims}, bundles: staticBundle{b},
		now: func() time.Time { return time.Unix(1700000000, 0) }, denied: func(r string) { *denied = append(*denied, r) }}
	eng := gin.New()
	r := newRouter(eng, routerConfig{componentID: "erp/sales", catalogue: problem.NewCatalogue(), locale: "en",
		defaultDeadline: time.Second, guardian: g})
	handler := func(c *gin.Context) {
		a, err := AccessFrom(c.Request.Context())
		if err != nil {
			Fail(c, err)
			return
		}
		Respond(c, 200, map[string]any{"sub": a.User().Sub, "cancel": a.Has("erp.sales.cancel"), "adjust": a.Has("erp.inventory.adjust")})
	}
	GET(r, "/public", Public, func(c *gin.Context) { Respond(c, 200, "ok") })
	GET(r, "/me", Authenticated, handler)
	GET(r, "/orders", PermKey("erp.sales.view"), handler)
	POST(r, "/adjust", PermKey("erp.inventory.adjust"), handler)
	return eng, denied
}

func call(eng *gin.Engine, method, path, auth string) (int, map[string]any) {
	req := httptest.NewRequest(method, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	eng.ServeHTTP(rec, req)
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestGuardChain(t *testing.T) {
	claims := &authn.Claims{Sub: "u1", Roles: []string{"dev_sales_rep"}, IssuedAt: time.Unix(1699999990, 0)}
	eng, denied := guardEngine(t, exampleBundle(t), claims)
	cases := []struct {
		method, path, auth string
		status             int
		reason             string
	}{
		{"GET", "/erp/sales/public", "", 200, ""},
		{"GET", "/erp/sales/me", "", 401, "TOKEN_INVALID"},
		{"GET", "/erp/sales/me", "Bearer bad", 401, "TOKEN_INVALID"},
		{"GET", "/erp/sales/me", "Bearer good", 200, ""},
		{"GET", "/erp/sales/orders", "Bearer good", 200, ""},
		{"POST", "/erp/sales/adjust", "Bearer good", 403, "MISSING_PERMISSION"},
	}
	for _, c := range cases {
		status, body := call(eng, c.method, c.path, c.auth)
		if status != c.status || (c.reason != "" && body["reason"] != c.reason) {
			t.Fatalf("%s %s %q: %d %v", c.method, c.path, c.auth, status, body)
		}
	}
	_, body := call(eng, "GET", "/erp/sales/orders", "Bearer good")
	if body["sub"] != "u1" || body["cancel"] != true || body["adjust"] != false {
		t.Fatalf("access: %v", body)
	}
	if len(*denied) != 3 {
		t.Fatalf("denied counted %v", *denied)
	}
}

func TestGuardWithoutBundle(t *testing.T) {
	claims := &authn.Claims{Sub: "u1", Roles: []string{"dev_sales_rep"}, IssuedAt: time.Unix(1699999990, 0)}
	eng, _ := guardEngine(t, nil, claims)
	if s, b := call(eng, "GET", "/erp/sales/orders", "Bearer good"); s != 503 || b["reason"] != "AUTHZ_NOT_READY" {
		t.Fatalf("%d %v", s, b)
	}
	if s, _ := call(eng, "GET", "/erp/sales/me", "Bearer good"); s != 200 {
		t.Fatalf("Authenticated route before the bundle: %d", s)
	}
	if s, _ := call(eng, "GET", "/erp/sales/public", ""); s != 200 {
		t.Fatalf("public route before the bundle: %d", s)
	}
}

func TestAccessFromWithoutUser(t *testing.T) {
	if _, err := AccessFrom(context.Background()); err == nil {
		t.Fatal("no user must be an error")
	}
}

func TestResourceGuardDecidesItsKey(t *testing.T) {
	const salesView PermKey = "erp.sales.view"
	const order ResourceType = "erp.sales.order"
	claims := &authn.Claims{Sub: "u1", Roles: []string{"dev_sales_rep"}, IssuedAt: time.Unix(1699999990, 0)}
	eng, _ := guardEngine(t, exampleBundle(t), claims)
	_ = eng
	g := salesView.On(order, "id")
	if g.Key != salesView || g.Type != order || g.Param != "id" || guardKey(salesView.List(order)) != salesView {
		t.Fatalf("%+v", g)
	}
}
