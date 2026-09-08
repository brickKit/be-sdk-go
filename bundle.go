package besdk

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// bundlePollInterval 是 GET /authz/bundle 的条件轮询间隔（设计书
// §14.1.4/§14.1.6：生效时延全部 ~15 秒，就是这个数）。
const bundlePollInterval = 15 * time.Second

// bundleCache 是 RequirePermission 判定用的进程内 map——"组件里没有任何
// 一张权限表"这条在这里成立：这只是一份内存缓存，不落库、不进迁移
// （§14.1.4）。⚠️ 这是全组件唯一一份、由 RunStandalone 在启动时创建
// 一次，RequirePermission 只读它，不许模块代码碰这个类型本身。
type bundleCache struct {
	mu          sync.RWMutex
	roles       map[string][]string
	staleSince  map[string]int64
	etag        string
	everFetched bool // 区分"还没连上过 authz"与"连上了但暂时没有任何角色"
}

func newBundleCache() *bundleCache {
	return &bundleCache{roles: map[string][]string{}, staleSince: map[string]int64{}}
}

// startBundlePoller 立刻拉一次，之后每 15 秒条件 GET 一次。ctx 取消时
// 循环退出——不需要额外的 Stop 方法。
//
// ⚠️ 单次失败（网络抖动、authz 重启中）只记日志、沿用内存里最后一份
// bundle 继续跑——这是 §14.1.9 的 fail-static：一个授权服务抖动不该让
// 使用它的组件同时拒绝所有请求。
func startBundlePoller(ctx context.Context, url string, logger *slog.Logger) *bundleCache {
	c := newBundleCache()
	go c.loop(ctx, url, logger)
	return c
}

func (c *bundleCache) loop(ctx context.Context, url string, logger *slog.Logger) {
	c.fetchOnce(ctx, url, logger)

	ticker := time.NewTicker(bundlePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.fetchOnce(ctx, url, logger)
		}
	}
}

type bundleWireFormat struct {
	Roles      map[string][]string `json:"roles"`
	StaleSince map[string]int64    `json:"stale_since"`
}

func (c *bundleCache) fetchOnce(ctx context.Context, url string, logger *slog.Logger) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Error("构造 authz bundle 请求失败", "error", err)
		return
	}
	c.mu.RLock()
	etag := c.etag
	c.mu.RUnlock()
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Warn("拉取 authz bundle 失败，沿用内存里已有的旧版本", "error", err)
		return
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		return // ETag 命中，未变化，沿用旧的
	case http.StatusOK:
		// 往下解析
	default:
		logger.Warn("拉取 authz bundle 收到非预期状态码，沿用内存里已有的旧版本",
			"status", resp.StatusCode)
		return
	}

	var body bundleWireFormat
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		logger.Error("解析 authz bundle 失败，沿用内存里已有的旧版本", "error", err)
		return
	}

	c.mu.Lock()
	c.roles = body.Roles
	c.staleSince = body.StaleSince
	c.etag = resp.Header.Get("ETag")
	c.everFetched = true
	c.mu.Unlock()
}

// hasEverFetched 区分"authz 从启动到现在一次都没连上过"（§14.1.9：业务
// 请求该返 503）与"连上过、只是这个角色恰好没有这条权限"（该返 403）。
func (c *bundleCache) hasEverFetched() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.everFetched
}

// hasPermission 把 roles 按纯并集展开，判 perm 在不在里面（设计书
// §14.1.3：纯并集，无 Deny）。
func (c *bundleCache) hasPermission(roles []string, perm string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, role := range roles {
		for _, key := range c.roles[role] {
			if key == perm {
				return true
			}
		}
	}
	return false
}

// staleSinceFor 取 sub 在有界列表里的时间戳，不存在返回 0（永不 stale）。
func (c *bundleCache) staleSinceFor(sub string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.staleSince[sub]
}
