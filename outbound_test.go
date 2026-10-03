package besdk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/httpx"
	"github.com/brickKit/be-sdk-go/internal/problem"
)

// testRuntime builds a Runtime from the test manifest and env without serving.
func testRuntime(t *testing.T, env map[string]string, inTx func(context.Context) bool) *Runtime {
	t.Helper()
	if env["THING_TOKEN_FILE"] == "" {
		env["THING_TOKEN_FILE"] = secretFile(t, "x")
	}
	spec := Spec{ID: "test/thing", Manifest: []byte(strings.Replace(testManifest, "PORT_PLACEHOLDER", "8080", 1))}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	b, err := bootstrap(context.Background(), spec, processEnv{lookup: lookup, stdout: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{id: b.id, version: b.version, cfg: &Config{vals: b.vals}, log: b.log, tel: b.member,
		catalogue: b.catalogue, locale: b.locale, clock: time.Now}
	if inTx == nil {
		inTx = func(context.Context) bool { return false }
	}
	out, err := newOutbound(rt, inTx)
	if err != nil {
		t.Fatal(err)
	}
	rt.deps.out = out
	return rt
}

func withUser(ctx context.Context, sub, token string) context.Context {
	ctx = context.WithValue(ctx, accessKey{}, &Access{user: User{Sub: sub}, now: time.Now})
	return context.WithValue(ctx, rawTokenKey{}, token)
}

func TestUserHTTPForwardsAndRestores(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		if r.URL.Path == "/erp/inventory/fail" {
			w.Header().Set("Content-Type", problem.ContentType)
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"code":"FAILED_PRECONDITION","reason":"INSUFFICIENT_STOCK","domain":"erp/inventory","metadata":{"available":"2"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	rt := testRuntime(t, map[string]string{"ERP_INVENTORY_ENDPOINT": srv.URL + "/"}, nil)
	c, err := rt.UserHTTP("erp/inventory")
	if err != nil {
		t.Fatal(err)
	}
	ctx := httpx.WithRequestID(withUser(context.Background(), "u1", "Bearer tok"), "req-9")
	var out struct{ OK bool }
	if err := c.JSON(ctx, "GET", "/erp/inventory/stock", nil, &out); err != nil || !out.OK {
		t.Fatalf("%v %v", err, out)
	}
	if got.Get("Authorization") != "Bearer tok" || got.Get("X-Request-Id") != "req-9" {
		t.Fatalf("headers %v", got)
	}
	err = c.JSON(ctx, "POST", "/erp/inventory/fail", map[string]int{"n": 1}, nil)
	if !problem.Is(err, "erp/inventory", "INSUFFICIENT_STOCK") {
		t.Fatalf("restored %v", err)
	}
	if err := c.JSON(context.Background(), "GET", "/x", nil, nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("no user: %v", err)
	}
	if _, err := rt.UserHTTP("erp/absent"); !errors.Is(err, ErrDependencyAbsent) {
		t.Fatalf("absent: %v", err)
	}
}

func TestUserHTTPBudgetBulkheadAndTx(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	inTx := func(ctx context.Context) bool { return ctx.Value("tx") != nil }
	rt := testRuntime(t, map[string]string{"ERP_INVENTORY_ENDPOINT": srv.URL}, inTx)
	c, _ := rt.UserHTTP("erp/inventory")
	user := withUser(context.Background(), "u1", "Bearer tok")
	short, cancel := context.WithTimeout(user, 40*time.Millisecond)
	defer cancel()
	if err := c.JSON(short, "GET", "/x", nil, nil); !problem.Is(err, "be", "DEADLINE_BUDGET_EXHAUSTED") {
		t.Fatalf("budget: %v", err)
	}
	if err := problem.Catch(func() error { return c.JSON(context.WithValue(user, "tx", 1), "GET", "/x", nil, nil) }); !problem.Is(err, "be", "NETWORK_IN_TX") {
		t.Fatalf("tx: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < outboundBulkhead; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.JSON(user, "GET", "/hold", nil, nil) }()
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(c.slots) < outboundBulkhead && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.JSON(user, "GET", "/x", nil, nil); !problem.Is(err, "be", "OUTBOUND_LIMIT") {
		t.Fatalf("65th call: %v", err)
	}
	release <- struct{}{}
}

func TestExternalHTTPStripsInternalHeaders(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got = r.Header.Clone() }))
	defer srv.Close()
	rt := testRuntime(t, map[string]string{}, nil)
	client := rt.ExternalHTTP("dingtalk", ExternalOptions{})
	req, _ := http.NewRequest("GET", srv.URL, nil)
	for _, h := range []string{"Authorization", "X-Request-Id", "Be-Caller", "X-Authz-Revision"} {
		req.Header.Set(h, "secret")
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	for _, h := range []string{"Authorization", "X-Request-Id", "Be-Caller", "X-Authz-Revision"} {
		if got.Get(h) != "" {
			t.Fatalf("%s forwarded", h)
		}
	}
	if got.Get("Accept") != "application/json" {
		t.Fatal("ordinary header lost")
	}
}
