package besdk

import "strings"

// envName 把组件 ID 推导成平台注入的变量名前缀。
//
// ⚠️ 规则必须与平台的 manifest.EnvPrefix 逐字一致：只把 / 与 - 换成 _，
// 然后全大写。不要多加 "." —— 组件 ID 的正则是
//
//	^[a-z0-9]([a-z0-9-]*[a-z0-9])?/[a-z0-9]([a-z0-9-]*[a-z0-9])?$
//
// 里面根本不允许出现点号（多写一条替换规则不会出错，但会让读的人以为 ID
// 里可能有点号，然后在别处按那个假设写代码）。
//
//	"mdm/customer" + ""     → MDM_CUSTOMER_ENDPOINT
//	"integration/im-dingtalk" + "grpc" → INTEGRATION_IM_DINGTALK_GRPC_ENDPOINT
func envName(dep, extra string) string {
	p := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(dep))
	if extra == "" {
		return p + "_ENDPOINT"
	}
	return p + "_" + strings.ToUpper(extra) + "_ENDPOINT"
}

// Endpoint 从 Config 读 <ID>[_<PORT>]_ENDPOINT 并剥掉 scheme。
//
// ⚠️ 读 Config 而不是进程环境：外壳里每个成员的依赖地址只存在于
// BRICKKIT_SERVED_MEMBERS_CONFIG 里它自己那一项的 config 中，进程环境属于外壳本身。
// 单跑时 RunStandalone 把进程环境整份灌进 Config，所以两种形态走的是同一个入口。
//
// ⚠️ 平台注入的值恒为 http:// 开头，额外端口也一样；grpc 拨号前必须剥掉。
// 可选依赖缺失时键不存在（不是空字符串）；空字符串同样当作缺失。
func (c Config) Endpoint(dep, extra string) (string, bool) {
	v, ok := c.String(envName(dep, extra))
	if !ok || v == "" {
		return "", false
	}
	v = strings.TrimPrefix(v, "http://")
	v = strings.TrimPrefix(v, "https://")
	return strings.TrimSuffix(v, "/"), true
}

// MustEndpoint 用于强依赖：缺失即 panic（强依赖缺失时平台本来就会阻断启动）。
func (c Config) MustEndpoint(dep, extra string) string {
	v, ok := c.Endpoint(dep, extra)
	if !ok {
		panic("强依赖 " + dep + " 的 " + envName(dep, extra) + " 未注入")
	}
	return v
}

// S3URL 读 S3_URL（完整 URL，原样返回）。v1 起 STORAGE_ENDPOINT 不再由平台注入，
// 而且 *_ENDPOINT 后缀是保留名，不能作为配置键。
func (c Config) S3URL() (string, bool) {
	v, ok := c.String("S3_URL")
	if !ok || v == "" {
		return "", false
	}
	return v, true
}
