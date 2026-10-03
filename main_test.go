package besdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

const testManifest = `apiVersion: brickkit/v1
kind: Component
metadata: {id: test/thing, name: Thing, version: 3.0.0}
configSchema:
  type: object
  properties:
    LOG_LEVEL: {type: string, default: info}
    HTTP_DEFAULT_TIMEOUT: {type: string, default: 10s}
    SHUTDOWN_GRACE: {type: string, default: 2s}
    DEFAULT_LOCALE: {type: string, default: en}
    THING_TOKEN_FILE: {type: string, secret: true, mount: file}
    THING_LIMIT: {type: integer, default: 5}
  required: [THING_TOKEN_FILE]
deployment:
  port: PORT_PLACEHOLDER
`

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

type testProc struct {
	port int
	out  *syncBuf
	done chan int
	stop context.CancelFunc
}

func startProc(t *testing.T, spec Spec, env map[string]string, args ...string) *testProc {
	t.Helper()
	gin.SetMode(gin.ReleaseMode)
	port := freePort(t)
	spec.Manifest = []byte(strings.Replace(string(spec.Manifest), "PORT_PLACEHOLDER", fmt.Sprint(port), 1))
	if spec.Manifest == nil || len(spec.Manifest) == 0 {
		spec.Manifest = []byte(strings.Replace(testManifest, "PORT_PLACEHOLDER", fmt.Sprint(port), 1))
	}
	lookup := testLookup(spec, env)
	ctx, cancel := context.WithCancel(context.Background())
	p := &testProc{port: port, out: &syncBuf{}, done: make(chan int, 1), stop: cancel}
	go func() { p.done <- run(ctx, spec, args, processEnv{lookup: lookup, stdout: p.out, instanceID: "test"}) }()
	t.Cleanup(func() { cancel(); <-p.done })
	return p
}

func (p *testProc) get(t *testing.T, path string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", p.port, path))
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp.StatusCode, string(b)
		}
		select {
		case code := <-p.done:
			t.Fatalf("process exited %d before serving: %s", code, p.out.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET %s: %v\n%s", path, err, p.out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func secretFile(t *testing.T, content string) string {
	p := filepath.Join(t.TempDir(), "THING_TOKEN_FILE")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func thingSpec(seen *string) Spec {
	return Spec{ID: "test/thing", Manifest: []byte(testManifest), New: func(ctx context.Context, rt *Runtime) (*Module, error) {
		*seen = rt.Config().Secret("THING_TOKEN_FILE").Current()
		limit := rt.Config().Int("THING_LIMIT", 0)
		return &Module{HTTP: func(r *Router) {
			GET(r, "/hello", Public, func(c *gin.Context) { Respond(c, 200, map[string]int{"limit": limit}) })
		}}, nil
	}}
}

func TestServeEndToEnd(t *testing.T) {
	var seen string
	p := startProc(t, thingSpec(&seen), map[string]string{"THING_TOKEN_FILE": secretFile(t, "s3cret\n"), "COMPONENT_ID": "test/thing"})
	if code, _ := p.get(t, "/healthz"); code != 200 {
		t.Fatalf("healthz %d", code)
	}
	if code, body := p.get(t, "/test/thing/hello"); code != 200 || !strings.Contains(body, `"limit":5`) {
		t.Fatalf("hello %d %s", code, body)
	}
	if code, _ := p.get(t, "/readyz"); code != 200 {
		t.Fatalf("readyz %d", code)
	}
	_, body := p.get(t, "/_be/info")
	var info map[string]any
	if err := json.Unmarshal([]byte(body), &info); err != nil || info["component_id"] != "test/thing" || info["component_version"] != "3.0.0" {
		t.Fatalf("info %s", body)
	}
	if code, body := p.get(t, "/test/thing/nope"); code != 404 || !strings.Contains(body, "NOT_FOUND") {
		t.Fatalf("unknown route %d %s", code, body)
	}
	if code, body := p.get(t, "/metrics"); code != 200 || !strings.Contains(body, `be_http_server_requests_total{component="test/thing"`) {
		t.Fatalf("metrics %d %s", code, body)
	}
	if seen != "s3cret" {
		t.Fatalf("secret %q", seen)
	}
	if strings.Contains(p.out.String(), "s3cret") {
		t.Fatalf("secret value logged: %s", p.out.String())
	}
	p.stop()
	if code := <-p.done; code != 0 {
		t.Fatalf("exit %d after SIGTERM: %s", code, p.out.String())
	}
	p.done <- 0 // for the cleanup
	for _, line := range strings.Split(strings.TrimSpace(p.out.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil || m["component_id"] != "test/thing" {
			t.Fatalf("not a JSON log line of the component: %q", line)
		}
	}
}

func runOnce(t *testing.T, spec Spec, env map[string]string, args ...string) (int, string) {
	t.Helper()
	port := freePort(t)
	if spec.Manifest == nil {
		spec.Manifest = []byte(testManifest)
	}
	spec.Manifest = []byte(strings.Replace(string(spec.Manifest), "PORT_PLACEHOLDER", fmt.Sprint(port), 1))
	var out syncBuf
	lookup := testLookup(spec, env)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code := run(ctx, spec, args, processEnv{lookup: lookup, stdout: &out})
	return code, out.String()
}

func TestExitCodes(t *testing.T) {
	var seen string
	good := map[string]string{"THING_TOKEN_FILE": secretFile(t, "x")}
	cases := []struct {
		name string
		spec Spec
		env  map[string]string
		args []string
		code int
		log  string
	}{
		{"unknown argument", thingSpec(&seen), good, []string{"migrat", "up"}, 64, "usage"},
		{"missing required secret", thingSpec(&seen), map[string]string{}, nil, 78, "THING_TOKEN_FILE"},
		{"unparsable value", thingSpec(&seen), map[string]string{"THING_TOKEN_FILE": good["THING_TOKEN_FILE"], "THING_LIMIT": "5x"}, nil, 78, "THING_LIMIT"},
		{"component id mismatch", thingSpec(&seen), map[string]string{"THING_TOKEN_FILE": good["THING_TOKEN_FILE"], "COMPONENT_ID": "test/other"}, nil, 78, "COMPONENT_ID"},
		{"New fails", Spec{ID: "test/thing", New: func(context.Context, *Runtime) (*Module, error) { return nil, errors.New("boom") }}, good, nil, 1, "boom"},
		{"New reads an undeclared key", Spec{ID: "test/thing", New: func(_ context.Context, rt *Runtime) (*Module, error) {
			rt.Config().String("NOT_DECLARED", "")
			return &Module{}, nil
		}}, good, nil, 78, "NOT_DECLARED"},
		{"protected route without auth keys", Spec{ID: "test/thing", New: func(_ context.Context, rt *Runtime) (*Module, error) {
			return &Module{HTTP: func(r *Router) { GET(r, "/x", PermKey("test.thing.view"), func(*gin.Context) {}) }}, nil
		}}, good, nil, 78, "AUTHZ_URL"},
		{"Start fails", Spec{ID: "test/thing", New: func(context.Context, *Runtime) (*Module, error) {
			return &Module{Start: func(context.Context) error { return errors.New("cannot start") }}, nil
		}}, good, nil, 1, "cannot start"},
		{"no COMPONENT_ID", thingSpec(&seen), map[string]string{"THING_TOKEN_FILE": good["THING_TOKEN_FILE"], noComponentID: "1"}, nil, 64, "COMPONENT_ID"},
		{"job run without background work", thingSpec(&seen), good, []string{"job", "run", "x"}, 64, "no background work"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, out := runOnce(t, c.spec, c.env, c.args...)
			if code != c.code || !strings.Contains(out, c.log) {
				t.Fatalf("exit %d (want %d), log:\n%s", code, c.code, out)
			}
		})
	}
}

// noComponentID in a test environment leaves COMPONENT_ID out; otherwise the platform's injection is
// simulated with the Spec's ID (P1.2: a process without COMPONENT_ID was not started by the platform).
const noComponentID = "TEST_NO_COMPONENT_ID"

func testLookup(spec Spec, env map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		if v, ok := env[k]; ok {
			return v, true
		}
		if k == "COMPONENT_ID" && env[noComponentID] == "" {
			return spec.ID, true
		}
		return "", false
	}
}
