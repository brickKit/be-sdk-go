package besdk

import (
	"os"
	"strings"
)

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

// Endpoint 读 *_ENDPOINT 并剥掉 scheme。
//
// ⚠️ 平台注入的值恒为 http:// 开头，额外端口也一样——没有 grpc:// 这种东西。
// grpc.Dial("http://host:9094") 连不上，而报错信息指向名称解析，
// 非常难联想到是这里。所以剥 scheme 这件事全项目只写在这一个函数里（§2.1）。
//
// ⚠️ 必须用 os.LookupEnv 而不是 os.Getenv 后判空：弱依赖缺失时那个变量
// 根本不存在，不是空字符串——这是平台刻意的设计（§3.6）。
func Endpoint(dep, extra string) (string, bool) {
	v, ok := os.LookupEnv(envName(dep, extra))
	if !ok || v == "" {
		return "", false
	}
	v = strings.TrimPrefix(v, "http://")
	v = strings.TrimPrefix(v, "https://")
	return strings.TrimSuffix(v, "/"), true
}

// MustEndpoint 用于强依赖：缺失即 panic（强依赖缺失时平台本来就会阻断启动）。
func MustEndpoint(dep, extra string) string {
	v, ok := Endpoint(dep, extra)
	if !ok {
		panic("强依赖 " + dep + " 的 " + envName(dep, extra) + " 未注入")
	}
	return v
}

// StorageEndpoint 读 STORAGE_ENDPOINT 并加上 scheme，返回完整 URL。
//
// ⚠️ 与 Endpoint 方向相反。STORAGE_ENDPOINT 是平台注入的资源变量，值是裸
// host:port；而 S3 SDK 要一个完整 URL。两个函数必须分开——共用一个必然
// 有一边错（十八条第 12 条）。
func StorageEndpoint(secure bool) (string, bool) {
	v, ok := os.LookupEnv("STORAGE_ENDPOINT")
	if !ok || v == "" {
		return "", false
	}
	scheme := "http://"
	if secure {
		scheme = "https://"
	}
	return scheme + v, true
}
