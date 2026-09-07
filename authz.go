package besdk

import (
	"net/http"

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

// RequirePermission 是判定本体。阶段二是 fail-closed stub：Public 放行，
// 其余一律拒绝——阶段三 infra-authz 上线前没有真实的权限判定，默认值
// 只能 fail-closed（同 §14.2.2 对 data_scopes 的态度）。阶段三换成进程内
// bundle map 查找，签名不变，调用方不用跟着改。
func RequirePermission(perm PermKey) gin.HandlerFunc {
	return func(c *gin.Context) {
		if perm == Public {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
			"error": "权限判定尚未实现（阶段三 infra-authz 上线前，非 Public 路由一律拒绝）",
		})
	}
}
