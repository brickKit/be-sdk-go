package besdk

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// authHeaderKey 是透传时使用的 metadata 键名，与网关注入 Authorization
// 的约定一致（JWT 本地验签，决策 87）。
const authHeaderKey = "authorization"

// UserClient 拨一条到 dep 的 gRPC 连接，把调用方请求里的 Authorization
// 透传给下游——下游按调用者身份做数据权限过滤（设计书 §14.2.3）。
//
// ⚠️ 只许出现在用户请求路径上。查内部批量数据、后台任务、事件 handler
// 一律用 SystemClient——这两个名字的区别就是安全边界（导读第 21 条：
// 这是「悄悄读到别人数据」的第三条路径，用错了不报错，返回的数据只是
// 「多了一些」）。
func UserClient(ctx context.Context, dep, extra string) (*grpc.ClientConn, error) {
	target, ok := Endpoint(dep, extra)
	if !ok {
		return nil, fmt.Errorf("besdk.UserClient: 依赖 %s 的地址未注入", dep)
	}
	var auth string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(authHeaderKey); len(v) > 0 {
			auth = v[0]
		}
	}
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(forwardAuthInterceptor(auth)),
	)
}

// SystemClient 拨一条到 dep 的 gRPC 连接，不透传任何调用者身份——下游
// 会把它当成组件自身发起的调用，数据权限被绕过（设计书 §14.2.6）。
// 只许出现在 Start() 与事件 handler 里，make gates 扫用户请求路径上的
// 误用。
func SystemClient(dep, extra string) (*grpc.ClientConn, error) {
	target, ok := Endpoint(dep, extra)
	if !ok {
		return nil, fmt.Errorf("besdk.SystemClient: 依赖 %s 的地址未注入", dep)
	}
	return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// forwardAuthInterceptor 把闭包捕获的 auth 值（拨号那一刻从调用方 ctx
// 里取到的）附到每一次出站调用的 metadata 上。之所以在拨号时取一次值
// 而不是在每次调用时重新解析 ctx，是因为 UserClient 的约定就是"每次
// 用户请求都现拨一条连接"（阶段二没有连接池/复用），拨号时的 ctx 与
// 调用时的 ctx 是同一个请求，取哪个时间点都一样，选在这里让接口更简单。
func forwardAuthInterceptor(auth string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if auth != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, authHeaderKey, auth)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
