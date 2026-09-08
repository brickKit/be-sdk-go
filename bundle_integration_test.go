package besdk

import (
	"os"
	"testing"
	"time"
)

// TestBundleCache_真机对接infra_authz 不是 mock——直接打真实跑着的
// infra-authz 容器的 GET /authz/bundle，确认 be-sdk-go 的轮询客户端
// 认得它实际吐出来的 JSON 形状（两边是本项目自己分两次写的，最容易
// 出现"字段名各写各的"这类耦合裂缝）。设了 TEST_AUTHZ_BUNDLE_URL 才跑，
// 同 TEST_PG_DSN 的约定（未设时跳过，不是放宽断言）。
func TestBundleCache_真机对接infra_authz(t *testing.T) {
	url := os.Getenv("TEST_AUTHZ_BUNDLE_URL")
	if url == "" {
		t.Skip("未设置 TEST_AUTHZ_BUNDLE_URL，跳过（本地至少跑一次真的）")
	}

	c := startBundlePoller(t.Context(), url, testLogger())
	waitUntil(t, 5*time.Second, c.hasEverFetched)

	// infra-authz 的迁移种了 authz_admin/infra.authz.admin 这条真实的
	// 自举数据（003_seed_bootstrap_admin_role.up.sql）——用它做断言，
	// 不用测试自己造的数据，这样即使全新环境第一次跑也能通过。
	if !c.hasPermission([]string{"authz_admin"}, "infra.authz.admin") {
		t.Fatal("应该能从真实 infra-authz 的 bundle 里查到自举角色 authz_admin 的 infra.authz.admin 权限")
	}
}
