package shell

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/brickKit/be-sdk-go/v3/internal/sectest"
	"io"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type greeter struct{ id, greeting string }

func (g *greeter) Routes(r *httpx.Routes) error {
	r.GET("/api/hello", opKey(g.id), func(c *httpx.Context) error {
		return c.JSON(200, map[string]string{"from": g.id, "greeting": g.greeting})
	})
	return nil
}

func opKey(id string) string { return strings.ReplaceAll(id, "/", ".") + ".read" }

func def(id, version string) component.Definition {
	return component.Definition{ID: id, Version: version, Authz: sectest.Defs(id, strings.ReplaceAll(id, "/", ".")+".greeting", opKey(id)),
		Keys: []config.Key{{Name: "GREETING", Type: config.String, Required: true}},
		New: func(rt *runtime.Runtime) (component.Module, error) {
			return &greeter{id: id, greeting: rt.Config.String("GREETING")}, nil
		}}
}

// fake build info: each member is its own module at the given version, optionally replaced.
func buildInfo(versions map[string]string, replaced ...string) func() (*debug.BuildInfo, bool) {
	return func() (*debug.BuildInfo, bool) {
		bi := &debug.BuildInfo{Main: debug.Module{Path: "example.test/shell"}}
		for id, v := range versions {
			m := &debug.Module{Path: "example.test/" + id, Version: v}
			for _, r := range replaced {
				if r == id {
					m.Replace = &debug.Module{Path: "../" + id}
				}
			}
			bi.Deps = append(bi.Deps, m)
		}
		return bi, true
	}
}

func moduleByID(d component.Definition, bi *debug.BuildInfo) (*debug.Module, string) {
	for _, m := range bi.Deps {
		if m.Path == "example.test/"+d.ID {
			return m, m.Path
		}
	}
	return nil, "example.test/" + d.ID
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func served(t *testing.T, members ...Served) string {
	b, err := json.Marshal(members)
	require.NoError(t, err)
	return string(b)
}

func envOf(m map[string]string) config.Env {
	return config.Env{LookupEnv: func(k string) (string, bool) { v, ok := m[k]; return v, ok },
		Read: os.ReadFile} // certificates are files
}

var self = Identity{ID: "be/go-core", Version: "3.0.0"}

func shellEnv(port int, membersJSON string, extra ...string) config.Env {
	m := map[string]string{"COMPONENT_ID": "be/go-core", "COMPONENT_VERSION": "3.0.0", "PORT": strconv.Itoa(port),
		"BRICKKIT_SERVED_MEMBERS_CONFIG": membersJSON, "STOP_DRAIN_DELAY": "100ms"}
	for i := 0; i+1 < len(extra); i += 2 {
		m[extra[i]] = extra[i+1]
	}
	return envOf(m)
}

func refuse(t *testing.T, env config.Env, bi func() (*debug.BuildInfo, bool), defs ...component.Definition) string {
	t.Helper()
	var stderr bytes.Buffer
	code := Run(context.Background(), self, defs, Options{Env: env, Stdout: io.Discard, Stderr: &stderr,
		BuildInfo: bi, ModuleOf: moduleByID})
	assert.Equal(t, 78, code)
	return stderr.String()
}

func get(t *testing.T, url string) (int, string) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	authorize(t, req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestHostsMembersOnTheirOwnPorts(t *testing.T) {
	pa, pb, ps := freePort(t), freePort(t), freePort(t)
	defs := []component.Definition{def("demo/alpha", "0.1.0"), def("demo/beta", "0.1.0")}
	js := served(t, secure(t, defs,
		Served{ComponentID: "demo/alpha", Version: "0.1.0", HTTPPort: pa, Config: map[string]string{"GREETING": "hi from alpha"}},
		Served{ComponentID: "demo/beta", Version: "0.1.0", HTTPPort: pb, Config: map[string]string{"GREETING": "hi from beta"}})...)
	ctx, cancel := context.WithCancel(context.Background())
	var stdout syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, self, []component.Definition{def("demo/alpha", "0.1.0"), def("demo/beta", "0.1.0")},
			Options{Env: shellEnv(ps, js), Stdout: &stdout, Stderr: io.Discard,
				BuildInfo: buildInfo(map[string]string{"demo/alpha": "v0.1.0", "demo/beta": "v0.1.0"}), ModuleOf: moduleByID})
	}()
	shellURL := "http://127.0.0.1:" + strconv.Itoa(ps)
	require.Eventually(t, func() bool { c, _ := get(t, shellURL+"/readyz"); return c == 200 }, 5*time.Second, 20*time.Millisecond)

	_, body := get(t, "http://127.0.0.1:"+strconv.Itoa(pa)+"/api/hello")
	assert.JSONEq(t, `{"from":"demo/alpha","greeting":"hi from alpha"}`, body, "each member reads its own config")
	_, body = get(t, "http://127.0.0.1:"+strconv.Itoa(pb)+"/api/hello")
	assert.JSONEq(t, `{"from":"demo/beta","greeting":"hi from beta"}`, body)

	_, desc := get(t, shellURL+"/.well-known/be-component")
	assert.Contains(t, desc, `"component_id":"be/go-core"`)
	assert.Contains(t, desc, `"demo/beta"`)
	_, metrics := get(t, shellURL+"/metrics")
	assert.Contains(t, metrics, `component="demo/alpha"`)
	assert.Contains(t, metrics, `component="demo/beta"`)

	cancel()
	assert.Equal(t, 0, <-done)
	c, _ := get(t, "http://127.0.0.1:"+strconv.Itoa(pa)+"/healthz")
	assert.Equal(t, 0, c, "every member's listener is closed after the stop")
}

func TestRefusesMemberNotCompiledIn(t *testing.T) { // 0-04 C6
	js := served(t, Served{ComponentID: "demo/alpha", Version: "0.1.0", HTTPPort: 18081, Config: map[string]string{"GREETING": "x"}},
		Served{ComponentID: "demo/beta", Version: "0.1.0", HTTPPort: 18082, Config: map[string]string{"GREETING": "x"}})
	out := refuse(t, shellEnv(18080, js), buildInfo(map[string]string{"demo/alpha": "v0.1.0"}), def("demo/alpha", "0.1.0"))
	assert.Contains(t, out, `demo/beta:componentId`)
	assert.Contains(t, out, "this shell has no module demo/beta")
}

func TestRefusesOtherVersionInBuildInfo(t *testing.T) { // 0-04 C5
	js := served(t, Served{ComponentID: "demo/alpha", Version: "0.1.0", HTTPPort: 18081, Config: map[string]string{"GREETING": "x"}})
	out := refuse(t, shellEnv(18080, js), buildInfo(map[string]string{"demo/alpha": "v0.2.0"}), def("demo/alpha", "0.1.0"))
	assert.Contains(t, out, "module example.test/demo/alpha at v0.1.0, but the build info has v0.2.0")
}

func TestRefusesReplaceUnlessAllowed(t *testing.T) {
	js := served(t, secure(t, []component.Definition{def("demo/alpha", "0.1.0")},
		Served{ComponentID: "demo/alpha", Version: "0.1.0", HTTPPort: freePort(t), Config: map[string]string{"GREETING": "x"}})...)
	bi := buildInfo(map[string]string{"demo/alpha": "v0.1.0"}, "demo/alpha")
	out := refuse(t, shellEnv(freePort(t), js), bi, def("demo/alpha", "0.1.0"))
	assert.Contains(t, out, "no replace in a release build")

	ctx, cancel := context.WithCancel(context.Background())
	var stdout syncBuffer
	ps := freePort(t)
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, self, []component.Definition{def("demo/alpha", "0.1.0")}, Options{
			Env: shellEnv(ps, js, "SHELL_ALLOW_REPLACE", "true"), Stdout: &stdout, Stderr: io.Discard, BuildInfo: bi, ModuleOf: moduleByID})
	}()
	require.Eventually(t, func() bool { c, _ := get(t, "http://127.0.0.1:"+strconv.Itoa(ps)+"/readyz"); return c == 200 },
		5*time.Second, 20*time.Millisecond)
	cancel()
	assert.Equal(t, 0, <-done)
	assert.Contains(t, stdout.String(), "shell built for development")
}

func TestRefusesVersionMismatchAndBadConfigAllAtOnce(t *testing.T) {
	js := served(t, Served{ComponentID: "demo/alpha", Version: "0.1.0", HTTPPort: 18081, Config: map[string]string{}},
		Served{ComponentID: "demo/beta", Version: "0.2.0", HTTPPort: 18082, Config: map[string]string{"LOG_LEVEL": "loud", "GREETING": "x"}})
	out := refuse(t, shellEnv(18080, js), buildInfo(map[string]string{"demo/alpha": "v0.1.0", "demo/beta": "v0.2.0"}),
		def("demo/alpha", "0.1.0"), def("demo/beta", "0.1.0"))
	assert.Contains(t, out, `demo/alpha:GREETING`)
	assert.Contains(t, out, `demo/beta:LOG_LEVEL`)
	assert.Contains(t, out, "0.2.0, the version compiled in is 0.1.0")
}

func TestRefusesProcessKeysThatDisagree(t *testing.T) {
	js := served(t, Served{ComponentID: "demo/alpha", Version: "0.1.0", HTTPPort: 18081, Config: map[string]string{"GREETING": "x", "OTEL_URL": "http://otel-a:4318"}},
		Served{ComponentID: "demo/beta", Version: "0.1.0", HTTPPort: 18082, Config: map[string]string{"GREETING": "x", "OTEL_URL": "http://otel-b:4318"}})
	out := refuse(t, shellEnv(18080, js), buildInfo(map[string]string{"demo/alpha": "v0.1.0", "demo/beta": "v0.1.0"}),
		def("demo/alpha", "0.1.0"), def("demo/beta", "0.1.0"))
	assert.Contains(t, out, `demo/beta:OTEL_URL`)
}

func TestShellDoesNotMigrate(t *testing.T) {
	var stderr bytes.Buffer
	code := Run(context.Background(), self, nil, Options{Args: []string{"migrate"}, Env: envOf(nil), Stdout: io.Discard, Stderr: &stderr})
	assert.Equal(t, 64, code)
	assert.Contains(t, stderr.String(), "each member's own image")
}

func TestPackageOfAndModuleOf(t *testing.T) {
	for name, want := range map[string]string{
		"example.test/alpha.New":                            "example.test/alpha",
		"github.com/brickKit/erp-sales/v3/sales.init.func1": "github.com/brickKit/erp-sales/v3/sales",
		"github.com/brickKit/erp-sales/v3/sales.(*m).New":   "github.com/brickKit/erp-sales/v3/sales",
		"github.com/brickKit/erp-sales/v3.New.func2":        "github.com/brickKit/erp-sales/v3",
		"main.main": "main",
	} {
		assert.Equal(t, want, packageOf(name), name)
	}
	// the real path, from the function name of a definition declared in this package
	bi := &debug.BuildInfo{Deps: []*debug.Module{{Path: "github.com/brickKit/be-sdk-go/v3/shellx"}}}
	mod, pkg := moduleOf(def("demo/alpha", "0.1.0"), bi)
	assert.Equal(t, "github.com/brickKit/be-sdk-go/v3/shell", pkg)
	assert.Nil(t, mod, "a module path is a prefix only at a path boundary: .../shellx does not provide .../shell")
	// Go resolves a package to the longest module path that prefixes it: the v3 module wins over an older module whose
	// tree could also hold a v3/shell directory
	bi.Deps = append(bi.Deps, &debug.Module{Path: "github.com/brickKit/be-sdk-go"},
		&debug.Module{Path: "github.com/brickKit/be-sdk-go/v3", Version: "v3.0.0"})
	mod, _ = moduleOf(def("demo/alpha", "0.1.0"), bi)
	require.NotNil(t, mod)
	assert.Equal(t, "github.com/brickKit/be-sdk-go/v3", mod.Path)
}
