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

// bundleFirstRetryDelay 是首次拉取失败后的第一次重试间隔。首次成功之前
// 按它翻倍退避（封顶 bundlePollInterval），见 loop。
const bundleFirstRetryDelay = 500 * time.Millisecond

// nextBundleRetryDelay 把退避间隔翻倍，封顶轮询间隔。
func nextBundleRetryDelay(d time.Duration) time.Duration {
	if d *= 2; d > bundlePollInterval {
		return bundlePollInterval
	}
	return d
}

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
// ⚠️ 首次成功之前不等满 15 秒：组件常和 authz 同时启动，第一次拉取时
// authz 多半还没起来。旧实现要等一个完整轮询周期才重试，这期间每个受
// 保护的路由都答 503（"还不知道"），启动后大约 20 秒不可用。现在首次
// 成功之前按 0.5 秒起翻倍退避（封顶轮询间隔）重试，成功之后才进入 15
// 秒的条件轮询。
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
	delay := bundleFirstRetryDelay
	for !c.fetchOnce(ctx, url, logger) {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = nextBundleRetryDelay(delay)
	}

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

// fetchOnce 拉一次，返回这次是否拿到了可用的 bundle（200 解析成功，或
// 304 沿用已有的）。失败时只记日志、内存里的旧内容不动。
func (c *bundleCache) fetchOnce(ctx context.Context, url string, logger *slog.Logger) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Error("构造 authz bundle 请求失败", "error", err)
		return false
	}
	c.mu.RLock()
	etag := c.etag
	c.mu.RUnlock()
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return false // 关停中取消，不是故障，不记日志
		}
		logger.Warn("拉取 authz bundle 失败，沿用内存里已有的旧版本", "error", err)
		return false
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		return true // ETag 命中，未变化，沿用旧的
	case http.StatusOK:
		// 往下解析
	default:
		logger.Warn("拉取 authz bundle 收到非预期状态码，沿用内存里已有的旧版本",
			"status", resp.StatusCode)
		return false
	}

	var body bundleWireFormat
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		logger.Error("解析 authz bundle 失败，沿用内存里已有的旧版本", "error", err)
		return false
	}

	c.mu.Lock()
	c.roles = body.Roles
	c.staleSince = body.StaleSince
	c.etag = resp.Header.Get("ETag")
	c.everFetched = true
	c.mu.Unlock()
	return true
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
