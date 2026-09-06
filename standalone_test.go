package besdk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

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
	var registered bool

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveExtraPort(ctx, "grpc", port, func(s *grpc.Server) { registered = true })
	}()

	waitForListen(t, port)
	if !registered {
		t.Fatal("RegisterGRPC 回调应该已经被调用")
	}

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
		os.Setenv("HTTP_PORT", "0")
		os.Setenv("PG_DSN", "postgres://user:pass@localhost:1/doesnotmatter") // sql.Open 是懒的，不会真连
		os.Setenv("NATS_URL", natsURLForTest(t))                             // nats.Connect 是急的，必须真能连上
		RunStandalone(func(context.Context, *Runtime) (*Module, error) {
			return nil, errors.New("模拟初始化失败")
		})
		return // 走不到这里——RunStandalone 应该已经 os.Exit(1) 了
	}

	cmd := exec.Command(os.Args[0], "-test.run", "TestRunStandalone_newModule失败时非零码退出且日志带componentID")
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
