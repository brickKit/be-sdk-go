package besdk

import (
	"context"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// TestNewShellRuntime_多模块共享DB与NATS但各自持有独立字段 是阶段四
// 外壳工程最核心的一条断言：§13.3 铁律二要求一个外壳一个连接池，模块
// 通过 SET LOCAL ROLE 切身份而不是各自开池——这里验证 NewShellRuntime
// 传入同一个 *sql.DB/*nats.Conn 时，两个模块的 Runtime 确实拿到同一个
// 指针（池真的被共享），而 ComponentID/Config/Logger/端口这些逐模块的
// 字段确实各自独立，不会被第二次调用覆盖第一次的结果。
func TestNewShellRuntime_多模块共享DB与NATS但各自持有独立字段(t *testing.T) {
	rt1 := NewShellRuntime(ShellModuleConfig{
		ComponentID:      "mdm/customer",
		ComponentVersion: "1.0.5",
		Env:              map[string]string{"FOO_BAR": "customer-value"},
		HTTPPort:         8080,
		ExtraPorts:       map[string]int{"grpc": 9090},
	}, nil, nil)

	rt2 := NewShellRuntime(ShellModuleConfig{
		ComponentID:      "erp/sales",
		ComponentVersion: "1.0.19",
		Env:              map[string]string{"FOO_BAR": "sales-value"},
		HTTPPort:         8084,
		ExtraPorts:       map[string]int{"grpc": 9094},
	}, nil, nil)

	if rt1.DB != rt2.DB {
		t.Fatalf("期望两个模块共享同一个 *sql.DB 指针（外壳只有一个池），实际不同")
	}
	if rt1.NATS != rt2.NATS {
		t.Fatalf("期望两个模块共享同一个 *nats.Conn 指针，实际不同")
	}

	if rt1.ComponentID != "mdm/customer" || rt2.ComponentID != "erp/sales" {
		t.Fatalf("期望 ComponentID 各自独立，实际 rt1=%q rt2=%q", rt1.ComponentID, rt2.ComponentID)
	}
	if rt1.HTTPPort != 8080 || rt2.HTTPPort != 8084 {
		t.Fatalf("期望 HTTPPort 各自独立，实际 rt1=%d rt2=%d", rt1.HTTPPort, rt2.HTTPPort)
	}
	if rt1.ExtraPorts["grpc"] != 9090 || rt2.ExtraPorts["grpc"] != 9094 {
		t.Fatalf("期望 ExtraPorts 各自独立，实际 rt1=%v rt2=%v", rt1.ExtraPorts, rt2.ExtraPorts)
	}

	v1, ok1 := rt1.Config.String("fooBar")
	v2, ok2 := rt2.Config.String("fooBar")
	if !ok1 || !ok2 || v1 != "customer-value" || v2 != "sales-value" {
		t.Fatalf("期望每个模块的 Config 只读到自己那份 env map，实际 rt1=(%q,%v) rt2=(%q,%v)", v1, ok1, v2, ok2)
	}

	if rt1.Logger == nil || rt2.Logger == nil || rt1.Tracer == nil || rt2.Tracer == nil {
		t.Fatal("期望 Logger/Tracer 都已装配，不是零值")
	}
	if rt1.Registry == rt2.Registry {
		t.Fatal("期望每个模块各自一份 Prometheus Registry（同 RunStandalone 的既有判据），不是共享同一个")
	}
}

// TestInitShellAuthz_只需调一次不需要每个模块各调 用 withAuthzRuntime
// 预置一个哨兵状态，验证 InitShellAuthz 真的调用到了 setAuthzRuntime
// （不是只是签名对但函数体是空的）——两项配置都缺失时 setupAuthzRuntime
// 按既有判据返回 (nil, nil)，调完之后哨兵应该被换成 nil，证明确实执行
// 到底，不是被短路跳过。
func TestInitShellAuthz_只需调一次不需要每个模块各调(t *testing.T) {
	withAuthzRuntime(t, &jwtVerifier{}, &bundleCache{})

	InitShellAuthz(context.Background(), NewConfig(map[string]string{}), NewLogger("test"))

	if authzRuntime.verifier != nil || authzRuntime.bundle != nil {
		t.Fatalf("期望空配置下 InitShellAuthz 把 authzRuntime 换成 (nil, nil)，实际 verifier=%v bundle=%v",
			authzRuntime.verifier, authzRuntime.bundle)
	}
}

// TestServeHTTP导出别名_行为与私有实现一致 只验证导出别名真的转发到了
// serveHTTP，不重复 TestServeHTTP_真的Listen且ctx_cancel后优雅关停 已经
// 覆盖过的完整生命周期断言。
func TestServeHTTP导出别名_行为与私有实现一致(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	port := freePortForTest(t)

	errCh := make(chan error, 1)
	go func() { errCh <- ServeHTTP(ctx, port, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})) }()

	waitForListen(t, port)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("期望优雅关闭无错误，实际 %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP 在 ctx 取消后没有及时退出")
	}
}

// TestServeExtraPort导出别名_行为与私有实现一致 只验证导出别名真的
// 转发到了 serveExtraPort（包括它内部已经挂好的 grpcRecoveryInterceptor），
// 不重复 gRPC 侧已有的完整断言。
func TestServeExtraPort导出别名_行为与私有实现一致(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	port := freePortForTest(t)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeExtraPort(ctx, "grpc", port, func(s *grpc.Server) {}, NewLogger("test"))
	}()

	waitForListen(t, port)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("期望优雅关闭无错误，实际 %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeExtraPort 在 ctx 取消后没有及时退出")
	}
}

// TestBuildPGDSN导出别名_行为与私有实现一致 与 TestBuildNATSURL导出别名
// 同理，只验证导出别名真的转发到了 buildPGDSN/buildNATSURL——完整的
// 拼接规则断言已经在 TestBuildPGDSN_从DATABASE前缀变量拼出DSN /
// TestBuildNATSURL_* 里覆盖过，这里不重复。
func TestBuildPGDSN导出别名_行为与私有实现一致(t *testing.T) {
	t.Setenv("DATABASE_HOST", "host.docker.internal")
	t.Setenv("DATABASE_PORT", "5432")
	t.Setenv("DATABASE_USER", "postgres")
	t.Setenv("DATABASE_PASSWORD", "s3cret")
	t.Setenv("DATABASE_NAME", "brickkit_db")

	got, err := BuildPGDSN()
	if err != nil {
		t.Fatal(err)
	}
	want, err := buildPGDSN()
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("期望导出别名与私有实现一致，got=%q want=%q", got, want)
	}
}

func TestBuildNATSURL导出别名_行为与私有实现一致(t *testing.T) {
	t.Setenv("MQ_HOST", "host.docker.internal")
	t.Setenv("MQ_PORT", "4222")

	if got, want := BuildNATSURL(), buildNATSURL(); got != want {
		t.Fatalf("期望导出别名与私有实现一致，got=%q want=%q", got, want)
	}
}
