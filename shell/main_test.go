package shell

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	besdk "github.com/brickKit/be-sdk-go"
)

// Main 读外壳进程自己的环境并以退出码报告结果，只能在子进程里测：子进程是本测试二进制自己，
// 由 BE_SHELL_MAIN_CHILD=1 进入 runMainChild。
func TestMain(m *testing.M) {
	if os.Getenv("BE_SHELL_MAIN_CHILD") == "1" {
		runMainChild()
		return
	}
	os.Exit(m.Run())
}

// runMainChild 是子进程里的外壳 main.go：一个成员 test/a，它的 HTTP 回答自己 rt.Config 里的
// PG_SCHEMA——用来证明成员配置来自成员清单 JSON，而不是外壳进程环境。
func runMainChild() {
	Main("test-shell", Registry{
		"test/a": func(_ context.Context, rt *besdk.Runtime) (*besdk.Module, error) {
			return &besdk.Module{HTTPHandler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, rt.Config.StringOr("PG_SCHEMA", "<none>"))
			})}, nil
		},
	})
	os.Exit(0) // Main 正常返回 = 关停干净
}

type mainChild struct {
	cmd *exec.Cmd
	out *bytes.Buffer
}

// startMainChild 在 dir 里启动子进程外壳（component.yaml 按相对路径从 cwd 读）。
func startMainChild(t *testing.T, dir string, env []string) *mainChild {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe)
	cmd.Dir = dir
	cmd.Env = append([]string{"BE_SHELL_MAIN_CHILD=1"}, env...)
	out := &bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return &mainChild{cmd: cmd, out: out}
}

func (c *mainChild) wait(t *testing.T) int {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- c.cmd.Wait() }()
	select {
	case err := <-done:
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}
		if err != nil {
			t.Fatalf("等待子进程：%v", err)
		}
		return 0
	case <-time.After(10 * time.Second):
		_ = c.cmd.Process.Kill()
		t.Fatalf("子进程 10 秒内没有退出，输出：%s", c.out.String())
		return -1
	}
}

func writeShellManifest(t *testing.T, port int) string {
	t.Helper()
	dir := t.TempDir()
	yaml := fmt.Sprintf("deployment:\n  port: %d\n", port)
	if err := os.WriteFile(filepath.Join(dir, "component.yaml"), []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// 启动前的三类失败都以 1 退出，日志说清是哪一步：成员清单未设置、成员清单是 null、
// 读不到外壳自己的 component.yaml。这些都在连库之前，不需要 TEST_PG_DSN。
func TestMainStartupFailuresExitOne(t *testing.T) {
	withManifest := writeShellManifest(t, 1)
	cases := []struct {
		name string
		dir  string
		env  []string
		want string
	}{
		{"成员清单未设置", withManifest, nil, "未设置"},
		{"成员清单是 null", withManifest, []string{"BRICKKIT_SERVED_MEMBERS_CONFIG=null"}, "null"},
		{"没有 component.yaml", t.TempDir(), []string{"BRICKKIT_SERVED_MEMBERS_CONFIG=[]"}, "component.yaml"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			child := startMainChild(t, c.dir, c.env)
			if code := child.wait(t); code != 1 {
				t.Fatalf("应以 1 退出，got %d，输出：%s", code, child.out.String())
			}
			if !strings.Contains(child.out.String(), c.want) {
				t.Fatalf("输出应含 %q：%s", c.want, child.out.String())
			}
		})
	}
}

// 端到端：Main 读外壳自己的 component.yaml 端口与环境，按成员清单起成员；成员的配置来自
// 清单 JSON（PG_SCHEMA=member_a），不是外壳进程环境（PG_SCHEMA=shell_own）；SIGTERM 后以 0 退出。
func TestMainServesMembersAndExitsZeroOnSIGTERM(t *testing.T) {
	dsn, nurl := os.Getenv("TEST_PG_DSN"), os.Getenv("TEST_NATS_URL")
	if dsn == "" || nurl == "" {
		t.Skip("需要 TEST_PG_DSN 与 TEST_NATS_URL")
	}
	u, err := neturl.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pw, _ := u.User.Password()
	shellPort, memberPort := freePort(t), freePort(t)
	members := fmt.Sprintf(`[{"componentId":"test/a","version":"2.0.0","httpPort":%d,"extraPorts":[],"config":{"PG_SCHEMA":"member_a"}}]`, memberPort)
	child := startMainChild(t, writeShellManifest(t, shellPort), []string{
		"BRICKKIT_SERVED_MEMBERS_CONFIG=" + members,
		"PG_HOST=" + u.Hostname(), "PG_PORT=" + u.Port(), "PG_DATABASE=" + strings.TrimPrefix(u.Path, "/"),
		"PG_USER=" + u.User.Username(), "PG_PASSWORD=" + pw, "PG_SCHEMA=shell_own", "NATS_URL=" + nurl,
		"AUTHZ_BUNDLE_URL=http://127.0.0.1:1/authz/bundle", "IAM_JWKS_URL=http://127.0.0.1:1/.well-known/jwks.json",
	})
	waitHealthy(t, shellPort)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/anything", memberPort))
	if err != nil {
		t.Fatalf("成员端口应在服务：%v，输出：%s", err, child.out.String())
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "member_a" {
		t.Fatalf("成员的 PG_SCHEMA 应来自成员清单（member_a），got %q", body)
	}

	if err := child.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := child.wait(t); code != 0 {
		t.Fatalf("SIGTERM 后应以 0 退出，got %d，输出：%s", code, child.out.String())
	}
}
