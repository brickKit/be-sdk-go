package besdk

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

// withAuthzRuntime 在测试期间把 authzRuntime 这个包级全局状态换成给定
// 值，测试结束后原样还原——RequirePermission 读的是包级变量（同
// otel.SetTracerProvider 那一类"进程只能有一份"的东西，见 authz.go
// 注释），测试之间不这样隔离会互相污染：一条测试装好真实判定后，
// 后面那些验"stub 下一律拒绝"的既有测试会突然表现不一样。
func withAuthzRuntime(t *testing.T, verifier *jwtVerifier, bundle *bundleCache) {
	t.Helper()
	prevVerifier, prevBundle := authzRuntime.verifier, authzRuntime.bundle
	authzRuntime.verifier, authzRuntime.bundle = verifier, bundle
	t.Cleanup(func() { authzRuntime.verifier, authzRuntime.bundle = prevVerifier, prevBundle })
}

// TestGET_标了Public的路由放行 是增补 B 三条断言的第 2 条。
func TestGET_标了Public的路由放行(t *testing.T) {
	r := gin.New()
	GET(r, "/x", Public, func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("Public 路由应该放行，got %d", w.Code)
	}
}

// TestGET_非Public的路由在stub实现下一律拒绝 是增补 B 三条断言的第 1 条。
// 阶段三 infra-authz 上线前，RequirePermission 是 fail-closed stub：
// Public 放行，其余一律拒绝——不能因为还没实现权限判定就放行一切，
// 那样会让还没上线的权限键形同虚设。
func TestGET_非Public的路由在stub实现下一律拒绝(t *testing.T) {
	r := gin.New()
	GET(r, "/x", PermKey("mdm.customer.view"), func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))

	if w.Code != http.StatusForbidden {
		t.Fatalf("非 Public 路由在 stub 实现下应该拒绝(403)，got %d", w.Code)
	}
}

// TestPOST_PUT_PATCH_DELETE_都接权限键 确认四个动词方法都真的接了权限键
// 参数并接上了 RequirePermission，不是只有 GET 一个人做了实事。
func TestPOST_PUT_PATCH_DELETE_都接权限键(t *testing.T) {
	cases := []struct {
		method string
		reg    func(r gin.IRoutes, path string, perm PermKey, h gin.HandlerFunc) gin.IRoutes
	}{
		{http.MethodPost, POST},
		{http.MethodPut, PUT},
		{http.MethodPatch, PATCH},
		{http.MethodDelete, DELETE},
	}
	for _, tc := range cases {
		r := gin.New()
		tc.reg(r, "/x", PermKey("some.perm"), func(c *gin.Context) { c.Status(http.StatusOK) })

		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(tc.method, "/x", nil))

		if w.Code != http.StatusForbidden {
			t.Fatalf("%s 非 Public 路由应该在 stub 下拒绝，got %d", tc.method, w.Code)
		}
	}
}

// Public 必须是显式常量而不是任意空字符串字面量的巧合——这条测试确认
// 类型系统真的接住了它：把 Public 直接当 PermKey 用不需要转换。
func TestPublic_是PermKey类型的常量(t *testing.T) {
	var p PermKey = Public
	if p != "" {
		t.Fatalf("Public 的底层值应该是空字符串，实际 %q", p)
	}
}

// ── 阶段三 Task 5：真实判定上线后的状态机 ─────────────────────────

func newTestRequest(method, path, bearer string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	return req
}

func serveOnce(r *gin.Engine, req *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestRequirePermission_没配iamJwksUrl时对非Public一律fail_closed(t *testing.T) {
	withAuthzRuntime(t, nil, nil) // 显式确认 nil,nil 就是阶段二遗留状态
	r := gin.New()
	GET(r, "/x", PermKey("some.perm"), func(c *gin.Context) { c.Status(http.StatusOK) })
	w := serveOnce(r, newTestRequest(http.MethodGet, "/x", ""))
	if w.Code != http.StatusForbidden {
		t.Fatalf("没有验签器时应该 fail-closed 403，实际 %d", w.Code)
	}
}

func TestRequirePermission_配了验签器但没带token返回401(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	withAuthzRuntime(t, v, newBundleCache())
	r := gin.New()
	GET(r, "/x", Authenticated, func(c *gin.Context) { c.Status(http.StatusOK) })
	w := serveOnce(r, newTestRequest(http.MethodGet, "/x", ""))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("没带 token 应该 401，实际 %d", w.Code)
	}
}

func TestRequirePermission_token签名不合法返回401(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	withAuthzRuntime(t, v, newBundleCache())
	r := gin.New()
	GET(r, "/x", Authenticated, func(c *gin.Context) { c.Status(http.StatusOK) })
	w := serveOnce(r, newTestRequest(http.MethodGet, "/x", "this.is.not.a.jwt"))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("非法 token 应该 401，实际 %d", w.Code)
	}
}

// TestRequirePermission_Authenticated放行任何合法登录不查权限键 是
// Authenticated 哨兵值本次新增的核心断言：合法 JWT，即使不含任何
// bundle 里查得到的权限键，也应该放行。
func TestRequirePermission_Authenticated放行任何合法登录不查权限键(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	withAuthzRuntime(t, v, nil) // 连 bundle 都没配——Authenticated 不应该关心它
	r := gin.New()
	GET(r, "/x", Authenticated, func(c *gin.Context) { c.Status(http.StatusOK) })
	token := tj.sign(t, "u_zhangsan", nil, "", "", time.Now())
	w := serveOnce(r, newTestRequest(http.MethodGet, "/x", token))
	if w.Code != http.StatusOK {
		t.Fatalf("合法 token 走 Authenticated 应该 200，实际 %d，body=%s", w.Code, w.Body.String())
	}
}

// TestRequirePermission_bundle从没连上过返回503 是 §14.1.9 明确要求的
// 那一档："启动时始终拿不到第一份 bundle → 业务请求返 503（不是
// 403）"——与"连上了但这个角色没这条权限"（403）必须是两个不同的
// 状态码，调用方/运维能一眼分清是"authz 还没就绪"还是"真的没权限"。
func TestRequirePermission_bundle从没连上过返回503(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	withAuthzRuntime(t, v, newBundleCache()) // newBundleCache：造出来但从没 fetch 过
	r := gin.New()
	GET(r, "/x", PermKey("erp.sales.view"), func(c *gin.Context) { c.Status(http.StatusOK) })
	token := tj.sign(t, "u_zhangsan", []string{"sales_manager"}, "", "", time.Now())
	w := serveOnce(r, newTestRequest(http.MethodGet, "/x", token))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("bundle 从没连上过应该 503，实际 %d", w.Code)
	}
}

func TestRequirePermission_权限键够200不够403(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeBundleServer(t)
	f.setBundle(map[string][]string{"sales_manager": {"erp.sales.view"}}, nil, `"v1"`)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	bundle := startBundlePoller(ctx, f.url(), testLogger())
	waitUntil(t, time.Second, bundle.hasEverFetched)
	withAuthzRuntime(t, v, bundle)

	r := gin.New()
	GET(r, "/view", PermKey("erp.sales.view"), func(c *gin.Context) { c.Status(http.StatusOK) })
	GET(r, "/approve", PermKey("erp.sales.approve"), func(c *gin.Context) { c.Status(http.StatusOK) })
	token := tj.sign(t, "u_zhangsan", []string{"sales_manager"}, "", "", time.Now())

	if w := serveOnce(r, newTestRequest(http.MethodGet, "/view", token)); w.Code != http.StatusOK {
		t.Fatalf("有 erp.sales.view 应该 200，实际 %d", w.Code)
	}
	if w := serveOnce(r, newTestRequest(http.MethodGet, "/approve", token)); w.Code != http.StatusForbidden {
		t.Fatalf("没有 erp.sales.approve 应该 403，实际 %d", w.Code)
	}
}

// TestRequirePermission_token早于stale_since返回401token_stale 是
// §14.1.6 判定链第 3 步的真实验证：一个人被踢出角色之后，他手上那份
// "旧快照"token 必须在下一次请求时就失效，不用等 TTL 到期。
func TestRequirePermission_token早于stale_since返回401token_stale(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	// ⚠️ token 的自然 TTL 是 10 分钟（sign() helper 写死），签发时间不能
	// 早到让 golang-jwt 自己的"exp 过期"判定先手——要测的是"在 TTL 有效
	// 期内、但比 stale_since 更早签发"这个专属场景，不是自然过期。
	tokenIssuedAt := time.Now().Add(-2 * time.Minute)
	token := tj.sign(t, "u_zhangsan", []string{"sales_manager"}, "", "", tokenIssuedAt)

	bundle := newBundleCache()
	bundle.staleSince["u_zhangsan"] = time.Now().Add(-time.Minute).Unix() // 比 token 签发晚
	bundle.everFetched = true
	withAuthzRuntime(t, v, bundle)

	r := gin.New()
	GET(r, "/x", Authenticated, func(c *gin.Context) { c.Status(http.StatusOK) })
	w := serveOnce(r, newTestRequest(http.MethodGet, "/x", token))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("stale 的 token 应该 401，实际 %d，body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("WWW-Authenticate"); got == "" {
		t.Fatalf("stale 的 401 应该带 WWW-Authenticate: Bearer error=\"token_stale\"，body=%s", w.Body.String())
	}
	// 顺带确认真的走的是 staleness 分支，不是恰好也 401 的验签失败——
	// 两者响应体不同，光看状态码分不清测的是哪一条。
	if !strings.Contains(w.Body.String(), "token_stale") {
		t.Fatalf("响应体应该提到 token_stale，实际 %s", w.Body.String())
	}
}

// TestRequirePermission_token晚于stale_since不受影响 确认时间比较方向
// 没有写反——这是最容易把 401 判定反过来的一条断言。
func TestRequirePermission_token晚于stale_since不受影响(t *testing.T) {
	tj := newTestJWKS(t)
	v, err := newJWTVerifier(t.Context(), tj.url())
	if err != nil {
		t.Fatal(err)
	}
	staleAt := time.Now().Add(-time.Hour)
	token := tj.sign(t, "u_zhangsan", []string{"sales_manager"}, "", "", time.Now()) // 刚刚重新登录，比 stale_since 晚

	bundle := newBundleCache()
	bundle.staleSince["u_zhangsan"] = staleAt.Unix()
	bundle.everFetched = true
	bundle.roles["sales_manager"] = []string{"erp.sales.view"}
	withAuthzRuntime(t, v, bundle)

	r := gin.New()
	GET(r, "/x", PermKey("erp.sales.view"), func(c *gin.Context) { c.Status(http.StatusOK) })
	w := serveOnce(r, newTestRequest(http.MethodGet, "/x", token))
	if w.Code != http.StatusOK {
		t.Fatalf("token 晚于 stale_since 不该被判 stale，实际 %d", w.Code)
	}
}
