package besdk

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, nil))
}

// fakeBundleServer 是一个可以在运行中改内容的假 /authz/bundle——用来在
// 一条测试里模拟"角色分配变了"这个真实场景，同时统计请求次数/校验
// If-None-Match 有没有真的被发送。
type fakeBundleServer struct {
	mu       sync.Mutex
	body     bundleWireFormat
	etag     string
	server   *httptest.Server
	hitCount atomic.Int64
	notMatch atomic.Int64 // 收到过 If-None-Match 且命中、返回 304 的次数
}

func newFakeBundleServer(t *testing.T) *fakeBundleServer {
	t.Helper()
	f := &fakeBundleServer{
		body: bundleWireFormat{Roles: map[string][]string{}, StaleSince: map[string]int64{}},
		etag: `"v1"`,
	}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hitCount.Add(1)
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("If-None-Match") == f.etag {
			f.notMatch.Add(1)
			w.Header().Set("ETag", f.etag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", f.etag)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(f.body)
	}))
	t.Cleanup(f.server.Close)
	return f
}

// setBundle 更新假服务器返回的内容并换一个新 ETag——真实 infra-authz
// 每次内容变化时 ETag 也会跟着变（bundle.go 的 ETag 是整份 JSON 的
// SHA-256），这里用递增版本号模拟同一件事。
func (f *fakeBundleServer) setBundle(roles map[string][]string, staleSince map[string]int64, etag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.body = bundleWireFormat{Roles: roles, StaleSince: staleSince}
	f.etag = etag
}

func (f *fakeBundleServer) url() string { return f.server.URL }

func TestBundleCache_轮询后能查到权限(t *testing.T) {
	f := newFakeBundleServer(t)
	f.setBundle(map[string][]string{"sales_manager": {"erp.sales.view", "erp.sales.approve"}}, nil, `"v1"`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startBundlePoller(ctx, f.url(), testLogger())

	waitUntil(t, time.Second, func() bool { return c.hasEverFetched() })

	if !c.hasPermission([]string{"sales_manager"}, "erp.sales.view") {
		t.Fatal("应该能查到 sales_manager 的 erp.sales.view")
	}
	if c.hasPermission([]string{"sales_manager"}, "erp.sales.export") {
		t.Fatal("不该查到没声明过的权限键")
	}
}

// TestBundleCache_ETag未变化返回304不清空内容 是 GET /authz/bundle 条件
// 请求这条机制最容易写反的地方：304 响应没有 body，如果代码没有正确
// short-circuit 而是继续往下解析，会把 Roles/StaleSince 覆盖成空。
func TestBundleCache_ETag未变化返回304不清空内容(t *testing.T) {
	f := newFakeBundleServer(t)
	f.setBundle(map[string][]string{"r1": {"perm.a"}}, nil, `"v1"`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startBundlePoller(ctx, f.url(), testLogger())
	waitUntil(t, time.Second, func() bool { return c.hasEverFetched() })

	// 手动再触发一次 fetchOnce（不改内容，ETag 不变，服务器应该回 304）。
	c.fetchOnce(ctx, f.url(), testLogger())

	if f.notMatch.Load() == 0 {
		t.Fatal("第二次拉取应该命中 If-None-Match 拿到 304，测试设计有问题")
	}
	if !c.hasPermission([]string{"r1"}, "perm.a") {
		t.Fatal("304 之后旧内容不该被清空")
	}
}

func TestBundleCache_单次拉取失败不清空旧内容(t *testing.T) {
	f := newFakeBundleServer(t)
	f.setBundle(map[string][]string{"r1": {"perm.a"}}, nil, `"v1"`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startBundlePoller(ctx, f.url(), testLogger())
	waitUntil(t, time.Second, func() bool { return c.hasEverFetched() })

	// 拿一个连不上的地址再拉一次，模拟 authz 抖动。
	c.fetchOnce(ctx, "http://127.0.0.1:1/nope", testLogger())

	if !c.hasPermission([]string{"r1"}, "perm.a") {
		t.Fatal("fail-static：单次拉取失败不该清空内存里已有的 bundle")
	}
}

// TestBundleCache_15秒后角色变更真的生效 是阶段三 Task 5 计划明确要求的
// 断言："改角色分配后不重启组件，等 15 秒左右重新请求，断言权限跟着
// 变——这条要真等 15 秒，不是 mock 时钟。"
func TestBundleCache_15秒后角色变更真的生效(t *testing.T) {
	if testing.Short() {
		t.Skip("真等 15 秒，-short 模式跳过")
	}
	f := newFakeBundleServer(t)
	f.setBundle(map[string][]string{"sales_rep": {}}, nil, `"v1"`)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := startBundlePoller(ctx, f.url(), testLogger())
	waitUntil(t, time.Second, func() bool { return c.hasEverFetched() })

	if c.hasPermission([]string{"sales_rep"}, "erp.sales.approve") {
		t.Fatal("一开始不该有这条权限")
	}

	// 管理员刚给 sales_rep 加了 erp.sales.approve——服务器内容变了，
	// ETag 也跟着变（真实 infra-authz 的 ETag 是内容的哈希）。
	f.setBundle(map[string][]string{"sales_rep": {"erp.sales.approve"}}, nil, `"v2"`)

	time.Sleep(bundlePollInterval + 2*time.Second) // 真睡，不 mock 时钟

	if !c.hasPermission([]string{"sales_rep"}, "erp.sales.approve") {
		t.Fatal("15 秒轮询之后应该拿到新权限，实际没有")
	}
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}
