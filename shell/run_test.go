package shell

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	besdk "github.com/brickKit/be-sdk-go"
	"google.golang.org/grpc"
)

// syncLogBuf 是一个并发安全、且在写入内容命中某个标记时对外发出信号的日志缓冲区——
// recover 之后的那条日志写在 Run 内部的另一个 goroutine 里，主 goroutine 只靠 sleep
// 去猜时序，-race 会如实报出数据竞争。close(matched) 建立的才是真正的同步点。
// ⚠️ 不能在"第一次 Write"就发信号：InitShellAuthz 在 panic 之前就往同一个 logger
// 写过日志，得按内容匹配。
type syncLogBuf struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	once    sync.Once
	marker  string
	matched chan struct{}
}

func newSyncLogBuf(marker string) *syncLogBuf {
	return &syncLogBuf{marker: marker, matched: make(chan struct{})}
}

func (s *syncLogBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	n, err := s.buf.Write(p)
	s.mu.Unlock()
	if strings.Contains(string(p), s.marker) {
		s.once.Do(func() { close(s.matched) })
	}
	return n, err
}

func (s *syncLogBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// fakeModule 是本包测试共用的最小假模块：HTTP 对任何路径答 200，Start/Stop 可注入，
// 构造时把拿到的 Runtime 记下来，供断言"共享池/各自配置"用。
type fakeModule struct {
	startFn func(context.Context) error
	stopped chan struct{}
	grpc    bool // 为 true 时返回非 nil 的 RegisterGRPC，让额外端口真的去监听
	mu      sync.Mutex
	rt      *besdk.Runtime
}

func (f *fakeModule) new(_ context.Context, rt *besdk.Runtime) (*besdk.Module, error) {
	f.mu.Lock()
	f.rt = rt
	f.mu.Unlock()
	return &besdk.Module{
		HTTPHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		RegisterGRPC: f.registerGRPC(),
		Start:        f.startFn,
		Stop: func(context.Context) error {
			if f.stopped != nil {
				close(f.stopped)
			}
			return nil
		},
	}, nil
}

func (f *fakeModule) registerGRPC() func(*grpc.Server) {
	if !f.grpc {
		return nil
	}
	return func(*grpc.Server) {}
}

func (f *fakeModule) runtime() *besdk.Runtime {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rt
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitHealthy(t *testing.T, port int) {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
	for i := 0; i < 50; i++ {
		if resp, err := http.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s 5 秒内未就绪", url)
}

// testShellConfig 把 TEST_PG_DSN 拆成外壳自己的 PG_* 连接键——外壳只认这组键，
// 不认整串 DSN。本项目惯例是"真实基础设施优先"，不 mock：缺任一变量就 Skip。
func testShellConfig(t *testing.T, port int) Config {
	t.Helper()
	dsn, nurl := os.Getenv("TEST_PG_DSN"), os.Getenv("TEST_NATS_URL")
	if dsn == "" || nurl == "" {
		t.Skip("需要 TEST_PG_DSN 与 TEST_NATS_URL")
	}
	u, err := neturl.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pw, _ := u.User.Password()
	return Config{ShellName: "test-shell", HTTPPort: port, ShellConfig: besdk.NewConfig(map[string]string{
		"PG_HOST": u.Hostname(), "PG_PORT": u.Port(), "PG_DATABASE": strings.TrimPrefix(u.Path, "/"),
		"PG_USER": u.User.Username(), "PG_PASSWORD": pw, "NATS_URL": nurl,
	})}
}

func buildModulesForTest(t *testing.T, ms []ServedMember, reg Registry) error {
	t.Helper()
	_, err := buildModules(context.Background(), ms, reg, nil, nil)
	return err
}

// startRun 在后台跑 Run，返回取消函数与结果通道。
func startRun(cfg Config, ms []ServedMember, reg Registry, logger *slog.Logger) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, ms, reg, logger) }()
	return cancel, done
}

func waitRunReturn(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run 5 秒内没有返回")
		return nil
	}
}

func TestRunRejectsMemberWithoutConstructor(t *testing.T) {
	err := buildModulesForTest(t, []ServedMember{{ComponentID: "x/unknown", HTTPPort: 1}}, Registry{})
	if err == nil || !strings.Contains(err.Error(), "x/unknown") {
		t.Fatalf("未登记构造函数的成员应报错并点名：%v", err)
	}
}

func TestRunRejectsNilModule(t *testing.T) {
	reg := Registry{"x/nil": func(context.Context, *besdk.Runtime) (*besdk.Module, error) { return nil, nil }}
	err := buildModulesForTest(t, []ServedMember{{ComponentID: "x/nil", HTTPPort: 1}}, reg)
	if err == nil || !strings.Contains(err.Error(), "x/nil") {
		t.Fatalf("构造函数返回 nil Module 应报错并点名：%v", err)
	}
}

func TestRunRequiresShellHTTPPort(t *testing.T) {
	err := Run(context.Background(), Config{ShellName: "test-shell"}, []ServedMember{}, Registry{}, besdk.NewLogger("test-shell"))
	if err == nil || !strings.Contains(err.Error(), "端口") {
		t.Fatalf("外壳端口为 0 应在启动前报错：%v", err)
	}
}

func TestRunZeroMembersServesOnlyShellHealth(t *testing.T) {
	// 零成员：只起外壳自己的 /healthz，200；不调用 Registry 里的任何构造函数
	called := false
	reg := Registry{"test/a": func(context.Context, *besdk.Runtime) (*besdk.Module, error) { called = true; return nil, nil }}
	port := freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, testShellConfig(t, port), []ServedMember{}, reg, besdk.NewLogger("test-shell"))
	}()
	waitHealthy(t, port)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("零成员时不应构造任何模块")
	}
}

// 两个成员装进同一个进程：各自的端口都真的在服务，共用同一个池和同一条 NATS 连接，
// 各自的 Config 只来自自己那条 JSON；ctx 取消后优雅退出，两个 Stop 都被调到。
func TestRunTwoMembersShareOneProcess(t *testing.T) {
	shellPort, p1, p2 := freePort(t), freePort(t), freePort(t)
	cfg := testShellConfig(t, shellPort)
	m1 := &fakeModule{stopped: make(chan struct{})}
	m2 := &fakeModule{stopped: make(chan struct{})}
	members := []ServedMember{
		{ComponentID: "test/a", Version: "1.0.0", HTTPPort: p1, Config: map[string]string{"PG_SCHEMA": "test_a"}},
		{ComponentID: "test/b", Version: "2.0.0", HTTPPort: p2, Config: map[string]string{"PG_SCHEMA": "test_b"}},
	}
	cancel, done := startRun(cfg, members, Registry{"test/a": m1.new, "test/b": m2.new}, besdk.NewLogger("test-shell"))

	waitHealthy(t, shellPort)
	waitHealthy(t, p1)
	waitHealthy(t, p2)

	rt1, rt2 := m1.runtime(), m2.runtime()
	if rt1.DB == nil || rt1.DB != rt2.DB {
		t.Error("两个成员应共用外壳同一个连接池")
	}
	if rt1.NATS == nil || rt1.NATS != rt2.NATS {
		t.Error("两个成员应共用外壳同一条 NATS 连接")
	}
	if rt1.ComponentID != "test/a" || rt1.ComponentVersion != "1.0.0" || rt2.ComponentVersion != "2.0.0" {
		t.Errorf("成员身份不对：%s@%s / %s@%s", rt1.ComponentID, rt1.ComponentVersion, rt2.ComponentID, rt2.ComponentVersion)
	}
	if s, _ := rt1.Config.String("PG_SCHEMA"); s != "test_a" {
		t.Errorf("成员 a 的 PG_SCHEMA = %q，应只来自自己的 JSON", s)
	}
	if s, _ := rt2.Config.String("PG_SCHEMA"); s != "test_b" {
		t.Errorf("成员 b 的 PG_SCHEMA = %q，应只来自自己的 JSON", s)
	}
	if _, ok := rt1.Config.String("PG_HOST"); ok {
		t.Error("成员 Config 不应混入外壳自己的配置")
	}

	cancel()
	if err := waitRunReturn(t, done); err != nil {
		t.Fatalf("期望优雅关闭无错误，实际 %v", err)
	}
	for name, ch := range map[string]chan struct{}{"a": m1.stopped, "b": m2.stopped} {
		select {
		case <-ch:
		default:
			t.Errorf("成员 %s 的 Stop 没有被调用", name)
		}
	}
}

// 单个成员 Start 里 panic：只隔离在它自己身上——另一个成员、它自己的 HTTP、外壳 /healthz
// 都照常服务，Run 一直阻塞到 ctx 取消才干净返回 nil，日志点名出事的成员。
func TestRunPanicInOneMemberDoesNotStopOthers(t *testing.T) {
	shellPort, pPanic, pOK := freePort(t), freePort(t), freePort(t)
	cfg := testShellConfig(t, shellPort)
	panicked := make(chan struct{})
	bad := &fakeModule{startFn: func(context.Context) error {
		close(panicked)
		panic("模拟模块自己代码里的一个真实 bug")
	}}
	good := &fakeModule{startFn: func(ctx context.Context) error { <-ctx.Done(); return nil }}
	logBuf := newSyncLogBuf("recovered")
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	cancel, done := startRun(cfg, []ServedMember{
		{ComponentID: "test/panics", HTTPPort: pPanic},
		{ComponentID: "test/healthy", HTTPPort: pOK},
	}, Registry{"test/panics": bad.new, "test/healthy": good.new}, logger)

	select {
	case <-panicked:
	case <-time.After(5 * time.Second):
		t.Fatal("成员的 Start 5 秒内没有触发预期的 panic")
	}
	select {
	case <-logBuf.matched:
	case <-time.After(5 * time.Second):
		t.Fatal("panic 之后 5 秒内没有等到 recover 那条日志")
	}

	// 日志已落地，此时三个端口都还应在服务——隔离生效的直接证据。
	waitHealthy(t, shellPort)
	waitHealthy(t, pOK)
	waitHealthy(t, pPanic)
	if out := logBuf.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "module_component_id=test/panics") {
		t.Fatalf("应记一条点名出事成员的 ERROR 日志，实际：%s", out)
	}
	select {
	case err := <-done:
		t.Fatalf("panic 不应让 Run 返回，实际返回 %v", err)
	default:
	}

	cancel()
	if err := waitRunReturn(t, done); err != nil {
		t.Fatalf("期望优雅关闭无错误，实际 %v", err)
	}
}

// 单个成员 Start 主动返回错误：与 panic 同样只隔离在它自己身上——它自己的 HTTP、其余成员、
// 外壳 /healthz 照常服务，Run 不返回。
func TestRunStartErrorInOneMemberDoesNotStopOthers(t *testing.T) {
	shellPort, pBad, pOK := freePort(t), freePort(t), freePort(t)
	cfg := testShellConfig(t, shellPort)
	bad := &fakeModule{startFn: func(context.Context) error { return errors.New("下游资源不可用") }}
	good := &fakeModule{}
	logBuf := newSyncLogBuf("下游资源不可用")
	logger := slog.New(slog.NewTextHandler(logBuf, nil))

	cancel, done := startRun(cfg, []ServedMember{
		{ComponentID: "test/fails", HTTPPort: pBad},
		{ComponentID: "test/healthy", HTTPPort: pOK},
	}, Registry{"test/fails": bad.new, "test/healthy": good.new}, logger)

	select {
	case <-logBuf.matched:
	case <-time.After(5 * time.Second):
		t.Fatal("5 秒内没有等到 Start 失败那条日志")
	}
	waitHealthy(t, shellPort)
	waitHealthy(t, pOK)
	waitHealthy(t, pBad)
	if out := logBuf.String(); !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "module_component_id=test/fails") {
		t.Fatalf("应记一条点名出事成员的 ERROR 日志，实际：%s", out)
	}
	select {
	case err := <-done:
		t.Fatalf("Start 返回错误不应让 Run 返回，实际返回 %v", err)
	default:
	}

	cancel()
	if err := waitRunReturn(t, done); err != nil {
		t.Fatalf("期望优雅关闭无错误，实际 %v", err)
	}
}

// 启动阶段某个成员构造失败：Run 中止并点名，返回前共享池与 NATS 连接都已关闭。
func TestRunConstructorFailureAbortsAndClosesSharedConnections(t *testing.T) {
	cfg := testShellConfig(t, freePort(t))
	first := &fakeModule{}
	reg := Registry{
		"test/a": first.new,
		"test/b": func(context.Context, *besdk.Runtime) (*besdk.Module, error) {
			return nil, errors.New("配置不合法")
		},
	}
	err := Run(context.Background(), cfg, []ServedMember{
		{ComponentID: "test/a", HTTPPort: freePort(t)},
		{ComponentID: "test/b", HTTPPort: freePort(t)},
	}, reg, besdk.NewLogger("test-shell"))
	if err == nil || !strings.Contains(err.Error(), "test/b") || !strings.Contains(err.Error(), "配置不合法") {
		t.Fatalf("构造失败应中止启动并点名成员：%v", err)
	}
	rt := first.runtime()
	if rt == nil {
		t.Fatal("第一个成员应已被构造")
	}
	if !rt.NATS.IsClosed() {
		t.Error("中止启动后共享 NATS 连接应已关闭")
	}
	if pingErr := rt.DB.Ping(); pingErr == nil || !strings.Contains(pingErr.Error(), "closed") {
		t.Errorf("中止启动后共享连接池应已关闭，Ping 得到 %v", pingErr)
	}
}

// 外壳自己的 /healthz 服务失败（这里用端口被占模拟）：这是唯一让 Run 带错误结束的情况，
// 其余成员随之优雅退出。
func TestRunShellHealthFailureEndsRun(t *testing.T) {
	occupied, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	shellPort := occupied.Addr().(*net.TCPAddr).Port
	cfg := testShellConfig(t, shellPort)
	m := &fakeModule{stopped: make(chan struct{})}

	_, done := startRun(cfg, []ServedMember{{ComponentID: "test/a", HTTPPort: freePort(t)}},
		Registry{"test/a": m.new}, besdk.NewLogger("test-shell"))
	if err := waitRunReturn(t, done); err == nil {
		t.Fatal("外壳 /healthz 监听失败时 Run 应返回错误")
	}
	select {
	case <-m.stopped:
	default:
		t.Error("外壳退出时成员的 Stop 应被调用")
	}
}

// 任一成员的端口失败（这里用端口被占模拟，HTTP 与额外端口各一例）：Run 必须带着点名该成员
// 的错误返回，外壳以非零码退出；其余成员随之优雅退出、Stop 被调到。成员端口死了而外壳
// /healthz 还是绿的，是平台探测不到的静默故障，所以这里必须闹大。
func TestRunMemberPortFailureEndsRun(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra bool
	}{{"HTTP端口", false}, {"额外端口", true}} {
		t.Run(tc.name, func(t *testing.T) {
			occupied, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatal(err)
			}
			defer occupied.Close()
			busy := occupied.Addr().(*net.TCPAddr).Port
			cfg := testShellConfig(t, freePort(t))

			blockedMember := ServedMember{ComponentID: "test/blocked", HTTPPort: busy}
			blocked := &fakeModule{}
			if tc.extra {
				blockedMember = ServedMember{ComponentID: "test/blocked", HTTPPort: freePort(t),
					ExtraPorts: []ExtraPort{{Name: "grpc", Port: busy}}}
				blocked.grpc = true
			}
			healthy := &fakeModule{stopped: make(chan struct{})}

			_, done := startRun(cfg, []ServedMember{blockedMember, {ComponentID: "test/healthy", HTTPPort: freePort(t)}},
				Registry{"test/blocked": blocked.new, "test/healthy": healthy.new}, besdk.NewLogger("test-shell"))

			err = waitRunReturn(t, done)
			if err == nil || !strings.Contains(err.Error(), "test/blocked") {
				t.Fatalf("成员端口失败时 Run 应返回点名该成员的错误，实际 %v", err)
			}
			select {
			case <-healthy.stopped:
			default:
				t.Error("外壳退出时其余成员的 Stop 应被调用")
			}
		})
	}
}

// ctx 已取消之后，服务协程不管带着什么错误返回都是正常关停：gRPC 的 Serve 如果在 GracefulStop
// 之后才开始，会返回 grpc.ErrServerStopped——启动窗口内收到 SIGTERM 时这是常态，不能被当成
// 成员故障（否则多一条点名健康成员的 ERROR，外壳以 1 退出）。
func TestServeResultAfterCancelIsCleanShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logBuf := newSyncLogBuf("level=ERROR")
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	for _, err := range []error{nil, grpc.ErrServerStopped, fmt.Errorf("额外端口 grpc 服务退出：%w", grpc.ErrServerStopped)} {
		if got := serveResult(ctx, logger, "test/a", "额外端口 grpc", err); got != nil {
			t.Errorf("ctx 已取消时 serveResult(%v) 应返回 nil，实际 %v", err, got)
		}
	}
	if out := logBuf.String(); out != "" {
		t.Errorf("正常关停不应记日志，实际：%s", out)
	}
}

// ctx 未取消时，意外返回 nil 与返回错误都算失败——这是 R15 的另一半，防止上面的修正把它带歪。
func TestServeResultBeforeCancelIsFailure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(newSyncLogBuf("x"), nil))
	for _, err := range []error{nil, errors.New("bind: address already in use")} {
		got := serveResult(context.Background(), logger, "test/a", "HTTP", err)
		if got == nil || !strings.Contains(got.Error(), "test/a") {
			t.Errorf("ctx 未取消时 serveResult(%v) 应返回点名成员的错误，实际 %v", err, got)
		}
	}
}

// 端到端：ctx 在 Run 开始之前就已取消（启动窗口内收到 SIGTERM 的极端形态），成员带额外端口——
// 所有服务协程都在取消之后才开始，Run 必须干净返回 nil。
func TestRunCancelledBeforeServeIsCleanShutdown(t *testing.T) {
	cfg := testShellConfig(t, freePort(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 20; i++ { // GracefulStop 与 Serve 的先后由调度决定，多跑几轮覆盖两种顺序
		m := &fakeModule{grpc: true}
		err := Run(ctx, cfg, []ServedMember{{ComponentID: "test/a", HTTPPort: freePort(t),
			ExtraPorts: []ExtraPort{{Name: "grpc", Port: freePort(t)}}}}, Registry{"test/a": m.new}, besdk.NewLogger("test-shell"))
		if err != nil {
			t.Fatalf("第 %d 轮：ctx 已取消时 Run 应返回 nil，实际 %v", i, err)
		}
	}
}

// 成员声明了额外端口却没有 RegisterGRPC：没人服务那个端口就是一次静默故障，启动即失败，
// 错误点名成员与端口。
func TestRunRejectsExtraPortWithoutRegisterGRPC(t *testing.T) {
	err := buildModulesForTest(t, []ServedMember{{ComponentID: "test/a", HTTPPort: 1,
		ExtraPorts: []ExtraPort{{Name: "grpc", Port: 9101}}}}, Registry{"test/a": (&fakeModule{}).new})
	if err == nil || !strings.Contains(err.Error(), "test/a") || !strings.Contains(err.Error(), "grpc") {
		t.Fatalf("声明了额外端口却没有 RegisterGRPC 应报错并点名成员与端口：%v", err)
	}
}
