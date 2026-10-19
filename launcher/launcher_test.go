package launcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	protocol "github.com/brickKit/be-protocol/v3"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/runtime"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// syncBuffer is a bytes.Buffer safe for the launcher's goroutines and the test reading it.
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

func env(m map[string]string) config.Env {
	return config.Env{LookupEnv: func(k string) (string, bool) { v, ok := m[k]; return v, ok },
		Read: os.ReadFile} // certificates are files
}

type demo struct {
	started, stopped atomic.Bool
	release          chan struct{}
}

func (d *demo) Routes(r *httpx.Routes) error {
	r.GET("/api/hello", "demo.alpha.read", func(c *httpx.Context) error {
		return c.JSON(200, map[string]string{"greeting": "hi"})
	})
	r.GET("/api/slow", "demo.alpha.read", func(c *httpx.Context) error {
		<-d.release
		return c.NoContent(204)
	})
	return nil
}
func (d *demo) Start(context.Context) error { d.started.Store(true); return nil }
func (d *demo) Stop(context.Context) error  { d.stopped.Store(true); return nil }

func definition(d *demo) component.Definition {
	return component.Definition{ID: "demo/alpha", Version: "3.0.0", Capabilities: []string{"core"},
		Authz: sectest.Defs("demo/alpha", "demo.alpha", "demo.alpha.read"),
		Keys:  []config.Key{{Name: "GREETING", Type: config.String, Default: "hello"}},
		New:   func(*runtime.Runtime) (component.Module, error) { return d, nil }}
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestUnknownEntry(t *testing.T) {
	var stderr bytes.Buffer
	code := Run(context.Background(), definition(&demo{}), Options{Args: []string{"dance"}, Env: env(nil), Stderr: &stderr, Stdout: io.Discard})
	assert.Equal(t, ExitUsage, code)
	assert.Contains(t, stderr.String(), "dance")
}

func TestConfigurationErrorsAllAtOnce(t *testing.T) {
	var stderr bytes.Buffer
	code := Run(context.Background(), definition(&demo{}), Options{Env: env(map[string]string{
		"COMPONENT_ID": "demo/alpha", "COMPONENT_VERSION": "2.9.9", "LOG_LEVEL": "loud"}), Stderr: &stderr, Stdout: io.Discard})
	assert.Equal(t, ExitConfiguration, code)
	out := stderr.String()
	for _, want := range []string{`"key":"PORT"`, `"key":"LOG_LEVEL"`, `"key":"COMPONENT_VERSION"`} {
		assert.Contains(t, out, want)
	}
}

func TestModuleFailuresDoNotCrash(t *testing.T) {
	base := map[string]string{"COMPONENT_ID": "demo/alpha", "COMPONENT_VERSION": "3.0.0", "PORT": strconv.Itoa(freePort(t))}
	for name, newFn := range map[string]func(*runtime.Runtime) (component.Module, error){
		"error": func(*runtime.Runtime) (component.Module, error) { return nil, errors.New("bad wiring") },
		"panic": func(*runtime.Runtime) (component.Module, error) { panic("boom") },
	} {
		def := definition(&demo{})
		def.New = newFn
		var stdout syncBuffer
		code := Run(context.Background(), def, Options{Env: env(base), Stdout: &stdout, Stderr: io.Discard})
		assert.Equal(t, ExitFailure, code, name)
		assert.Contains(t, stdout.String(), "the module could not be built", name)
	}
}

func get(t *testing.T, url string) (int, []byte) {
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	authorize(t, req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestServeReadyAndGracefulStop(t *testing.T) {
	port := freePort(t)
	base := "http://127.0.0.1:" + strconv.Itoa(port)
	d := &demo{release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	var stdout syncBuffer
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, definition(d), Options{Env: env(secured(t, definition(d), map[string]string{"COMPONENT_ID": "demo/alpha",
			"COMPONENT_VERSION": "3.0.0", "PORT": strconv.Itoa(port), "STOP_DRAIN_DELAY": "400ms", "STOP_TIMEOUT": "5s"})),
			Stdout: &stdout, Stderr: io.Discard})
	}()

	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 5*time.Second, 20*time.Millisecond)
	assert.True(t, d.started.Load())
	code, body := get(t, base+"/api/hello")
	assert.Equal(t, 200, code)
	assert.JSONEq(t, `{"greeting":"hi"}`, string(body))
	_, body = get(t, base+"/.well-known/be-component")
	var desc map[string]any
	require.NoError(t, json.Unmarshal(body, &desc))
	assert.Equal(t, "demo/alpha", desc["component_id"])
	assert.Equal(t, protocol.Version, desc["protocol"], "the version of the protocol the SDK pins")

	slow := make(chan int, 1)
	go func() { c, _ := get(t, base+"/api/slow"); slow <- c }()
	time.Sleep(50 * time.Millisecond)

	cancel() // the stop signal
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 503 }, time.Second, 10*time.Millisecond,
		"not ready as soon as stopping, while still listening during the drain delay")
	close(d.release)
	assert.Equal(t, 204, <-slow, "a request in flight finishes during the stop")
	assert.Equal(t, ExitOK, <-done)
	assert.True(t, d.stopped.Load())
	logs := stdout.String()
	for _, ev := range []string{`"event":"ready"`, `"event":"stopping"`, `"event":"stopped"`} {
		assert.Contains(t, logs, ev)
	}
	c, _ := get(t, base+"/healthz")
	assert.Equal(t, 0, c, "the listener is closed after the stop")
}

func TestResponsesAreNotHeldBackOnAKeptAliveConnection(t *testing.T) { // P1.19
	// a response often leaves in more than one write; with Nagle on, the second waits for the client's delayed ACK
	// (about 40 ms on Linux), so every request on a kept-alive connection takes that long whatever the work
	port := freePort(t)
	base := "http://127.0.0.1:" + strconv.Itoa(port)
	d := &demo{release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		done <- Run(ctx, definition(d), Options{Env: env(secured(t, definition(d), map[string]string{"COMPONENT_ID": "demo/alpha",
			"COMPONENT_VERSION": "3.0.0", "PORT": strconv.Itoa(port), "STOP_DRAIN_DELAY": "0s"})),
			Stdout: io.Discard, Stderr: io.Discard})
	}()
	require.Eventually(t, func() bool { c, _ := get(t, base+"/readyz"); return c == 200 }, 5*time.Second, 20*time.Millisecond)
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1}}
	var took []time.Duration
	for i := 0; i < 20; i++ {
		started := time.Now()
		resp, err := client.Get(base + "/.well-known/be-component")
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		took = append(took, time.Since(started))
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	assert.Less(t, took[len(took)/2], 15*time.Millisecond, "median of 20 requests on one connection")
	cancel()
	close(d.release)
	assert.Equal(t, ExitOK, <-done)
}
