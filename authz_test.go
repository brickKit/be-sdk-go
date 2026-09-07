package besdk

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

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
