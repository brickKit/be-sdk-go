package besdk

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// PermKey 是权限键——assembly.yaml 的 permissions 段声明的那些
// （设计书 §14.1.1）。类型化是为了让漏传参数在编译期就报错，而不是
// 悄悄用一个空字符串放行了不该放行的接口。
type PermKey string

// Public 是「显式公开」，不是「省略」。用空字符串做零值是刻意的：
// 调用点必须显式写 besdk.Public 才能编译通过，不存在"忘了传权限键"
// 这种失败模式（设计书 §14.1.7、导读第 23 条）。
const Public PermKey = ""

// Authenticated 是"已登录即可，不需要具体权限键"这一档（阶段三 Task 4
// 实现 infra-authz 时发现的真实缺口：GET /api/me/permissions 这类端点
// 任何登录用户都该能查，套一个具体权限键反而是画蛇添足）。⚠️ 仍然会验
// JWT 签名与 stale_since，只是跳过 bundle map 的权限键查找这一步。
const Authenticated PermKey = "__authenticated__"

// GET/POST/PUT/PATCH/DELETE 注册一条带权限键的路由。
//
// ⚠️ 业务代码不许再碰 gin.Engine/gin.RouterGroup 自己的 GET/POST 等方法
// ——那样绕开的不是一层封装，是权限键的签名强制：漏写权限键在这里编译
// 不过，裸用 gin 原生方法则完全不需要权限键，那个接口就从此无人鉴权且
// 没有任何症状（设计书 §14.1.7、导读第 23 条）。
//
// 第一个参数用 gin.IRoutes 不是 *gin.Engine：业务路由通常挂在
// engine.Group(...) 产出的 *gin.RouterGroup 上，*gin.Engine 收不下。
func GET(r gin.IRoutes, path string, perm PermKey, h gin.HandlerFunc) gin.IRoutes {
	return r.GET(path, RequirePermission(perm), h)
}

func POST(r gin.IRoutes, path string, perm PermKey, h gin.HandlerFunc) gin.IRoutes {
	return r.POST(path, RequirePermission(perm), h)
}

func PUT(r gin.IRoutes, path string, perm PermKey, h gin.HandlerFunc) gin.IRoutes {
	return r.PUT(path, RequirePermission(perm), h)
}

func PATCH(r gin.IRoutes, path string, perm PermKey, h gin.HandlerFunc) gin.IRoutes {
	return r.PATCH(path, RequirePermission(perm), h)
}

func DELETE(r gin.IRoutes, path string, perm PermKey, h gin.HandlerFunc) gin.IRoutes {
	return r.DELETE(path, RequirePermission(perm), h)
}

// authzRuntime 是 RequirePermission/ScopeOf 用的进程级状态——由
// RunStandalone 在启动时装配一次（同 otel.SetTracerProvider 那一类
// "只能有一份"的东西，§12.5.2）。为 nil 或字段为 nil 都代表"这个组件
// 没有配 iamJwksUrl/authzBundleUrl"，此时任何非 Public 权限键一律
// fail-closed 403——这是阶段二遗留的默认状态，阶段三给这两项配置赋值
// 之前，行为不变。
var authzRuntime struct {
	verifier *jwtVerifier
	bundle   *bundleCache
}

// setAuthzRuntime 只应由 RunStandalone 调用一次。
func setAuthzRuntime(verifier *jwtVerifier, bundle *bundleCache) {
	authzRuntime.verifier = verifier
	authzRuntime.bundle = bundle
}

// setupAuthzRuntime 从 rt.Config 读 iamJwksUrl/authzBundleUrl，装配
// JWT 验签器与 bundle 轮询——RunStandalone 专用，模块代码不调用。
// 两项配置任一缺失都返回 nil，调用方（RequirePermission）据此退化成
// fail-closed stub，不阻断组件启动（§14.1.9：authz 不可达不该拖累
// 组件本身）。
func setupAuthzRuntime(ctx context.Context, cfg Config, logger *slog.Logger) (*jwtVerifier, *bundleCache) {
	var verifier *jwtVerifier
	if jwksURL, ok := cfg.String("iamJwksUrl"); ok && jwksURL != "" {
		v, err := newJWTVerifier(ctx, jwksURL)
		if err != nil {
			// ⚠️ 不 exitf：JWKS 端点暂时不可达（iam 适配层还没起来）不该
			// 阻断本组件启动，keyfunc 内部的刷新协程会持续重试。这里
			// 失败通常是 URL 本身不合法这类配置错误，记日志即可定位。
			logger.Error("初始化 JWT 验签器失败，非 Public/Authenticated 权限键将 fail-closed", "error", err)
		} else {
			verifier = v
		}
	} else {
		logger.Info("未配置 iamJwksUrl，非 Public/Authenticated 权限键将 fail-closed（阶段二遗留行为）")
	}

	var bundle *bundleCache
	if bundleURL, ok := cfg.String("authzBundleUrl"); ok && bundleURL != "" {
		bundle = startBundlePoller(ctx, bundleURL, logger)
	} else {
		logger.Info("未配置 authzBundleUrl，具体权限键判定将始终 503")
	}

	return verifier, bundle
}

type claimsCtxKey struct{}

// claimsFromContext 供 ScopeOf 读取——RequirePermission 验签成功后把
// Claims 塞进请求的 context.Context。
func claimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsCtxKey{}).(*Claims)
	return claims, ok
}

// RequirePermission 是判定本体。判定链（设计书 §14.1.6 第 3 步、§14.1.9）：
//  1. Public：直接放行，不验签——/healthz 这类必须匿名可达的端点靠这条。
//  2. 验签 JWT（本地，JWKS 从 iamJwksUrl 来）；没配 iamJwksUrl（authzRuntime.verifier
//     为 nil）时退化成阶段二的 fail-closed stub：非 Public 一律 403，
//     行为对已经写好、还没升级 configSchema 的组件保持不变。
//  3. jwt.iat < stale_since[sub] → 401 token_stale（有界列表，§14.1.6）。
//  4. Authenticated：验签过、不 stale 就放行，不查权限键。
//  5. 具体权限键：bundle 从没连上过 authz → 503（不是 403，语义更准，
//     §14.1.9）；连上过就在纯并集展开后的权限键集合里查，查不到 403。
func RequirePermission(perm PermKey) gin.HandlerFunc {
	return func(c *gin.Context) {
		if perm == Public {
			c.Next()
			return
		}

		verifier := authzRuntime.verifier
		if verifier == nil {
			// 阶段二遗留的 fail-closed stub：没有真实判定能力时，非
			// Public 一律拒绝——安全机制的默认值只能 fail-closed。
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "权限判定尚未配置（iamJwksUrl 未注入）",
			})
			return
		}

		token, ok := bearerToken(c)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "缺少或格式不对的 Authorization"})
			return
		}
		claims, err := verifier.Verify(token)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token 无效: " + err.Error()})
			return
		}

		if isStale(claims, authzRuntime.bundle) {
			c.Header("WWW-Authenticate", `Bearer error="token_stale"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token_stale"})
			return
		}

		// ⚠️ 塞进 c.Request 的 context，不是 c.Set——ScopeOf 收的是
		// context.Context，仓储层调用链上拿到的是 c.Request.Context()，
		// 两者必须是同一份底层 context 才能读到刚塞进去的值。
		c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), claimsCtxKey{}, claims))

		if perm == Authenticated {
			c.Next()
			return
		}

		bundle := authzRuntime.bundle
		if bundle == nil || !bundle.hasEverFetched() {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "权限判定尚未就绪（authz 从启动到现在还没能连上过）",
			})
			return
		}
		if !bundle.hasPermission(claims.Roles, string(perm)) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "无权限"})
			return
		}
		c.Next()
	}
}

func bearerToken(c *gin.Context) (string, bool) {
	h := c.GetHeader("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(h, prefix))
	return token, token != ""
}

// staleTimeSkew 是 jwt.iat 与 stale_since 比较时的容忍余量——跨机器的
// 毫秒级时钟偏差会造成一次多余的刷新，留几秒余量消掉它（设计书 §14.1.6）。
const staleTimeSkewSeconds = 5

func isStale(claims *Claims, bundle *bundleCache) bool {
	if bundle == nil {
		return false
	}
	since := bundle.staleSinceFor(claims.Sub)
	if since == 0 {
		return false
	}
	return claims.IssuedAt.Unix() < since-staleTimeSkewSeconds
}
