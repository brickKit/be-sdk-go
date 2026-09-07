package besdk

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

// startTestGRPCServer 起一个真实的 gRPC 服务，借用 grpc-go 自带的标准
// 健康检查服务当被调用方——不用为这条测试专门写一份 .proto。拦截器把
// 每次调用收到的 authorization header 值送进返回的 channel。
func startTestGRPCServer(t *testing.T) (addr string, got chan string) {
	t.Helper()
	got = make(chan string, 8)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any,
		info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		auth := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get("authorization"); len(v) > 0 {
				auth = v[0]
			}
		}
		got <- auth
		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), got
}

func callHealth(t *testing.T, cc *grpc.ClientConn) {
	t.Helper()
	client := healthpb.NewHealthClient(cc)
	if _, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check 失败：%v", err)
	}
}

// TestUserClient_透传JWT而SystemClient不透传 是增补 B 三条断言的第 3 条：
// 两个 client 的名字区别必须是真的——UserClient 把调用方请求里的
// Authorization 透传给下游，SystemClient 一律不透传（导读第 21 条：
// 这是「悄悄读到别人数据」的第三条路径，名字区别就是安全边界）。
func TestUserClient_透传JWT而SystemClient不透传(t *testing.T) {
	addr, got := startTestGRPCServer(t)
	t.Setenv("TEST_DEP_ENDPOINT", "http://"+addr)

	incomingCtx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer user-token"))
	uc, err := UserClient(incomingCtx, "test/dep", "")
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	callHealth(t, uc)
	if auth := <-got; auth != "Bearer user-token" {
		t.Fatalf("UserClient 应该透传 Authorization，实际收到 %q", auth)
	}

	sc, err := SystemClient("test/dep", "")
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	callHealth(t, sc)
	if auth := <-got; auth != "" {
		t.Fatalf("SystemClient 不该透传任何身份，实际收到 %q", auth)
	}
}

func TestUserClient_弱依赖缺失时返回错误而不panic(t *testing.T) {
	if _, err := UserClient(context.Background(), "no/such-dep", ""); err == nil {
		t.Fatal("依赖地址未注入时应该返回 error")
	}
}

func TestUserClient_剥掉scheme(t *testing.T) {
	addr, _ := startTestGRPCServer(t)
	t.Setenv("TEST_DEP2_ENDPOINT", "http://"+addr)

	// 直接验证 UserClient 内部真的把 http:// 剥掉了——用同一个 target
	// 连一次没有 scheme 前缀的裸地址必须能连上（Endpoint 已经在别处测过
	// 剥 scheme 本身，这里测的是 UserClient 真的调用了它，不是自己拼了
	// 一套平行逻辑，参照 mdm-customer repo_test.go 的同类测试注释）。
	cc, err := UserClient(context.Background(), "test/dep2", "")
	if err != nil {
		t.Fatalf("剥掉 scheme 后应该能正常拨号：%v", err)
	}
	defer cc.Close()
	callHealth(t, cc)
}

// 占位：确认 SystemClient 也走同一条 Endpoint() 剥 scheme 的路径。
func TestSystemClient_剥掉scheme(t *testing.T) {
	addr, _ := startTestGRPCServer(t)
	t.Setenv("TEST_DEP3_ENDPOINT", "http://"+addr)

	cc, err := SystemClient("test/dep3", "")
	if err != nil {
		t.Fatalf("剥掉 scheme 后应该能正常拨号：%v", err)
	}
	defer cc.Close()
	callHealth(t, cc)
}
