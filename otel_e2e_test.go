package besdk

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// collector is an in-process OTLP/HTTP trace receiver: service.name and trace id of every span.
type collector struct {
	mu    sync.Mutex
	spans []string // "<service.name> <trace id hex> <span name>"
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body := io.Reader(r.Body)
	if r.Header.Get("Content-Encoding") == "gzip" {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		body = gz
	}
	raw, _ := io.ReadAll(body)
	var req coltrace.ExportTraceServiceRequest
	if err := proto.Unmarshal(raw, &req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range req.ResourceSpans {
		svc := ""
		for _, a := range rs.GetResource().GetAttributes() {
			if a.Key == "service.name" {
				svc = a.GetValue().GetStringValue()
			}
		}
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				c.spans = append(c.spans, svc+" "+hexOf(s.TraceId)+" "+s.Name)
			}
		}
	}
	w.WriteHeader(200)
}

func hexOf(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(b))
	for _, x := range b {
		out = append(out, digits[x>>4], digits[x&15])
	}
	return string(out)
}

// P18.1: the server span continues the caller's trace, carries the component's service.name, and is
// exported by the time the process exits (the member flushes, then the platform closes the exporter).
func TestTracesExportedWithMemberIdentity(t *testing.T) {
	col := &collector{}
	srv := httptest.NewServer(col)
	defer srv.Close()
	manifest := strings.Replace(testManifest, "    THING_LIMIT:", "    OTEL_BASE_URL: {type: string, default: \"\"}\n    THING_LIMIT:", 1)
	spec := Spec{ID: "test/thing", Manifest: []byte(manifest), New: func(_ context.Context, rt *Runtime) (*Module, error) {
		return &Module{HTTP: func(r *Router) { GET(r, "/hi", Public, func(c *gin.Context) { Respond(c, 200, "hi") }) }}, nil
	}}
	p := startProc(t, spec, map[string]string{"THING_TOKEN_FILE": secretFile(t, "x"), "OTEL_BASE_URL": srv.URL})
	p.get(t, "/healthz")
	req, _ := http.NewRequest("GET", "http://127.0.0.1:"+strconv.Itoa(p.port)+"/test/thing/hi", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	p.stop()
	if code := <-p.done; code != 0 {
		t.Fatalf("exit %d", code)
	}
	p.done <- 0
	col.mu.Lock()
	defer col.mu.Unlock()
	want := "test/thing 4bf92f3577b34da6a3ce929d0e0e4736 GET /test/thing/hi"
	for _, s := range col.spans {
		if s == want {
			return
		}
	}
	t.Fatalf("span %q not exported; got %v", want, col.spans)
}
