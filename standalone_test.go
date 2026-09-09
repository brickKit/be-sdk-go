package besdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// syncBuffer 是并发安全的 bytes.Buffer 包装——测试里的 logger 会从
// serveExtraPort 的 goroutine 里写，主 goroutine 同时读，裸 bytes.Buffer
// 在 -race 下会报数据竞争。
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// 十八条第 17 条：gin.SetMode 是包级全局，必须在 Bootstrap 里设一次，
// 不能让它停在默认的 DebugMode（panic 堆栈会直接吐给客户端）。
func TestBootstrap_把Gin设成ReleaseMode(t *testing.T) {
	gin.SetMode(gin.DebugMode) // 模拟"还没调用过 Bootstrap"的初始状态
	shutdown, err := Bootstrap(context.Background(), "test-service", "")
	if err != nil {
		t.Fatalf("Bootstrap 不该报错：%v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	if gin.Mode() != gin.ReleaseMode {
		t.Fatalf("期望 gin.Mode()=release，实际 %q", gin.Mode())
	}
}

func TestServeHTTP_真的Listen且ctx_cancel后优雅关停(t *testing.T) {
	port := freePortForTest(t)
	handler := gin.New()
	handler.GET("/probe", func(c *gin.Context) { c.String(200, "ok") })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveHTTP(ctx, port, handler) }()

	waitForListen(t, port)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/probe", port))
	if err != nil {
		t.Fatalf("应该真的在监听这个端口：%v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("期望 200，得到 %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ctx cancel 后应该干净返回，got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveHTTP 在 ctx cancel 后没有及时返回（优雅关停超时）")
	}
}

func TestServeExtraPort_真的Listen且ctx_cancel后优雅关停(t *testing.T) {
	port := freePortForTest(t)
	// ⚠️ 实测踩坑（-race 抓到，且是真的会发生，不只是内存可见性问题）：
	// serveExtraPort 内部先 net.Listen 再 register(srv)——TCP 层的监听
	// backlog 在 register 跑之前就已经能接受连接了，waitForListen 只探测
	// 裸 TCP 连通性，不能保证 register 回调已经执行完。用一个专门的
	// channel 等 register 真的跑完，而不是靠一个没有同步原语保护的裸
	// bool 变量去猜"端口能连了 register 应该也跑完了"。
	registered := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveExtraPort(ctx, "grpc", port, func(s *grpc.Server) { close(registered) }, NewLogger("test"))
	}()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("RegisterGRPC 回调 2 秒内没有被调用")
	}
	waitForListen(t, port)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("应该真的在监听这个端口：%v", err)
	}
	conn.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ctx cancel 后应该干净返回，got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveExtraPort 在 ctx cancel 后没有及时返回（GracefulStop 超时）")
	}
}

// panicServiceDesc 是一个手写的最小 gRPC 服务描述，不需要专门写一份
// .proto——唯一一个方法的 handler 直接 panic，供下面的测试验证
// grpcRecoveryInterceptor 真的兜住了它（同 client_test.go 的
// startTestGRPCServer 判据：借用/手搭一个真实服务比为一条测试写.proto
// 更直接）。请求/响应都用 emptypb.Empty——内容不重要，只是要一个真实
// 的 proto.Message 类型满足 grpc-go 的编解码。
var panicServiceDesc = grpc.ServiceDesc{
	ServiceName: "besdktest.PanicService",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Panic",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				handler := func(ctx context.Context, req any) (any, error) {
					panic("boom：模拟一个未处理的 panic")
				}
				if interceptor == nil {
					return handler(ctx, in)
				}
				info := &grpc.UnaryServerInfo{FullMethod: "/besdktest.PanicService/Panic"}
				return interceptor(ctx, in, info, handler)
			},
		},
	},
	Streams: []grpc.StreamDesc{},
}

// TestServeExtraPort_handler里panic不崩进程返回Internal错误 是
// docs/dev/实测踩坑记录.md C11 的直接回归测试：erp-inventory 的
// Receive 等方法在 ctx 没有 Claims 时调 besdk.ScopeOf 会 panic，而裸
// grpc.NewServer() 对此没有任何防护，一路把整个容器进程带崩
// （真机复现：RestartCount 从 0 涨到 3）。这条测试真起一个
// serveExtraPort 服务、真拨号、真调一个会 panic 的方法，断言：①客户端
// 收到干净的 codes.Internal（不是连接被重置/EOF）；②服务进程本身活着
// ——用同一条连接紧接着再调一次证明 grpc.Server 没有被这一次 panic
// 拖垮（这正是"以前会崩容器"和"现在只是这一个 RPC 报错"的区别）。
func TestServeExtraPort_handler里panic不崩进程返回Internal错误(t *testing.T) {
	port := freePortForTest(t)
	var loggedPanic bool
	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- serveExtraPort(ctx, "grpc", port, func(s *grpc.Server) {
			s.RegisterService(&panicServiceDesc, nil)
		}, logger)
	}()
	waitForListen(t, port)

	cc, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	callPanic := func() error {
		return cc.Invoke(context.Background(), "/besdktest.PanicService/Panic", &emptypb.Empty{}, &emptypb.Empty{})
	}

	err = callPanic()
	if err == nil {
		t.Fatal("期望 panic 的方法返回 error，实际 nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("期望一个 gRPC status 错误（服务端干净返回，不是连接被重置），实际 %v", err)
	}
	if st.Code() != codes.Internal {
		t.Fatalf("期望 codes.Internal，实际 %v（%s）", st.Code(), st.Message())
	}
	if strings.Contains(st.Message(), "boom") {
		t.Fatalf("panic 的原始内容不该回传给客户端，实际响应里带了：%q", st.Message())
	}

	// ⚠️ 核心断言：进程/server 本身没有被这次 panic 拖垮，同一条连接立刻
	// 能再调一次（哪怕还是同一个会 panic 的方法，只要能收到第二次干净的
	// Internal 而不是连接失败，就证明 grpc.Server 挺过了第一次 panic）。
	if err := callPanic(); err != nil {
		if st, ok := status.FromError(err); !ok || st.Code() != codes.Internal {
			t.Fatalf("panic 之后 server 应该继续正常服务，第二次调用期望还是干净的 codes.Internal，实际 %v", err)
		}
	} else {
		t.Fatal("第二次调用也该报错（handler 本身还是 panic），但至少证明了连接没死")
	}

	if strings.Contains(logBuf.String(), "gRPC 处理 panic") {
		loggedPanic = true
	}
	if !loggedPanic {
		t.Fatalf("期望日志里记一条 \"gRPC 处理 panic\"，实际日志：%s", logBuf.String())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serveExtraPort 在 ctx cancel 后没有及时返回")
	}
}

// ⚠️ v0.1.0 读的是单个 PG_DSN 环境变量，但平台实际注入的是分开的
// DATABASE_HOST/PORT/USER/PASSWORD/NAME（mdm-customer 第一次真的
// brickkit up --dry-run 之后才核对出来的落差，PG_DSN 从来不存在）。
func TestBuildPGDSN_从DATABASE前缀变量拼出DSN(t *testing.T) {
	t.Setenv("DATABASE_HOST", "host.docker.internal")
	t.Setenv("DATABASE_PORT", "5432")
	t.Setenv("DATABASE_USER", "postgres")
	t.Setenv("DATABASE_PASSWORD", "s3cret")
	t.Setenv("DATABASE_NAME", "brickkit_db")

	dsn, err := buildPGDSN()
	if err != nil {
		t.Fatal(err)
	}
	want := "postgres://postgres:s3cret@host.docker.internal:5432/brickkit_db?sslmode=disable"
	if dsn != want {
		t.Fatalf("期望 %q，得到 %q", want, dsn)
	}
}

// 同理，NATS_URL 也从来不存在——平台注入的是 MQ_HOST/MQ_PORT（本项目的
// nats-shared 资源没配 username/password，所以 MQ_USER/MQ_PASSWORD 不会
// 被注入；这里两种情况都要对）。
func TestBuildNATSURL_无认证(t *testing.T) {
	t.Setenv("MQ_HOST", "host.docker.internal")
	t.Setenv("MQ_PORT", "4222")
	os.Unsetenv("MQ_USER")
	os.Unsetenv("MQ_PASSWORD")

	got := buildNATSURL()
	want := "nats://host.docker.internal:4222"
	if got != want {
		t.Fatalf("期望 %q，得到 %q", want, got)
	}
}

func TestBuildNATSURL_带认证(t *testing.T) {
	t.Setenv("MQ_HOST", "host.docker.internal")
	t.Setenv("MQ_PORT", "4222")
	t.Setenv("MQ_USER", "brickkit")
	t.Setenv("MQ_PASSWORD", "s3cret")

	got := buildNATSURL()
	want := "nats://brickkit:s3cret@host.docker.internal:4222"
	if got != want {
		t.Fatalf("期望 %q，得到 %q", want, got)
	}
}

func freePortForTest(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func waitForListen(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("端口 %d 在 2 秒内没有开始监听", port)
}

// TestRunStandalone_newModule失败时非零码退出且日志带componentID 必须走
// 子进程——exitf 会真的调用 os.Exit(1)，在当前测试进程里跑会直接把整个
// go test 杀掉。标准做法：用一个环境变量标记"这是那个会退出的子进程"，
// 父进程重新 exec 自己、只跑这一个测试，检查子进程的退出码与 stderr。
func TestRunStandalone_newModule失败时非零码退出且日志带componentID(t *testing.T) {
	if os.Getenv("BESDK_SUBPROCESS_EXIT_TEST") == "1" {
		os.Setenv("COMPONENT_ID", "test/failing-module")
		os.Setenv("COMPONENT_VERSION", "0.0.1")
		// HTTP_PORT/PG_DSN/NATS_URL 都不是平台真的会注入的变量（§13.8.1、
		// 006 §4.4）——端口从 component.yaml 读（cmd.Dir 指向的临时目录，
		// 见下方父进程），数据库/NATS 走分开的 DATABASE_*/MQ_* 变量。
		os.Setenv("DATABASE_HOST", "localhost")
		os.Setenv("DATABASE_PORT", "1")
		os.Setenv("DATABASE_USER", "user")
		os.Setenv("DATABASE_PASSWORD", "pass") // sql.Open 是懒的，不会真连
		os.Setenv("DATABASE_NAME", "doesnotmatter")
		host, port := natsHostPortForTest(t) // nats.Connect 是急的，必须真能连上
		os.Setenv("MQ_HOST", host)
		os.Setenv("MQ_PORT", port)
		RunStandalone(func(context.Context, *Runtime) (*Module, error) {
			return nil, errors.New("模拟初始化失败")
		})
		return // 走不到这里——RunStandalone 应该已经 os.Exit(1) 了
	}

	dir := t.TempDir()
	manifest := "deployment:\n  port: 0\n"
	if err := os.WriteFile(filepath.Join(dir, "component.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run", "TestRunStandalone_newModule失败时非零码退出且日志带componentID")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "BESDK_SUBPROCESS_EXIT_TEST=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("期望子进程以非零码退出，实际 err=%v", err)
	}
	if exitErr.ExitCode() != 1 {
		t.Fatalf("期望退出码 1，得到 %d", exitErr.ExitCode())
	}
	if !strings.Contains(stderr.String(), "test/failing-module") {
		t.Fatalf("stderr 应该带 componentID 才能在合并态排障时定位是哪个模块，实际输出：%s", stderr.String())
	}
	if !strings.Contains(stderr.String(), "模拟初始化失败") {
		t.Fatalf("stderr 应该带原始错误信息，实际输出：%s", stderr.String())
	}
}

// ⚠️ 实测踩坑：mdm-customer 第一次真的 brickkit up 起来后，/healthz 每次
// 请求都 panic："invalid memory address or nil pointer dereference"，
// 出处是 NewGinEngine 的 tracingMiddleware 调 rt.Tracer.Start(...)——
// RunStandalone 构造 Runtime 时压根没有把 Tracer/Meter 填进去，两个字段
// 一直是 nil interface。be-sdk-go 自己的 gin_test.go 从没抓到这个问题，
// 因为它的 newTestRuntime helper 手工塞了一个真 tracer，从来没有测过
// "RunStandalone 自己组出来的 Runtime 传给 NewGinEngine 会怎样"。
// 这个测试真的走一遍子进程：起一个只包一层 NewGinEngine 的模块，
// 真实 HTTP 请求 /healthz，必须是 200 而不是连接被重置或者 500。
func TestRunStandalone_healthz真的能响应不panic(t *testing.T) {
	if os.Getenv("BESDK_SUBPROCESS_HEALTHZ_TEST") == "1" {
		RunStandalone(func(ctx context.Context, rt *Runtime) (*Module, error) {
			return &Module{HTTPHandler: NewGinEngine(rt)}, nil
		})
		return
	}

	dir := t.TempDir()
	port := freePortForTest(t)
	manifest := fmt.Sprintf("deployment:\n  port: %d\n", port)
	if err := os.WriteFile(filepath.Join(dir, "component.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}

	host, natsPort := natsHostPortForTest(t)
	cmd := exec.Command(os.Args[0], "-test.run", "TestRunStandalone_healthz真的能响应不panic")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"BESDK_SUBPROCESS_HEALTHZ_TEST=1",
		"COMPONENT_ID=test/healthz-module",
		"COMPONENT_VERSION=0.0.1",
		"DATABASE_HOST=localhost", "DATABASE_PORT=1",
		"DATABASE_USER=user", "DATABASE_PASSWORD=pass", "DATABASE_NAME=doesnotmatter", // sql.Open 是懒的
		"MQ_HOST="+host, "MQ_PORT="+natsPort,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_, _ = cmd.Process.Wait()
	})

	waitForListen(t, port)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
	if err != nil {
		t.Fatalf("请求 /healthz 失败：%v\nstderr:\n%s", err, stderr.String())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("期望 200，得到 %d\nstderr:\n%s", resp.StatusCode, stderr.String())
	}
}
