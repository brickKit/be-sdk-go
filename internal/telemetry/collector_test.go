package telemetry

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// gotSpan is one span as an OTLP/HTTP collector received it.
type gotSpan struct {
	Resource map[string]string
	Name     string
	TraceID  string
}

// collector is an in-process OTLP/HTTP trace collector that decodes every export request (as r1-01).
type collector struct {
	srv   *httptest.Server
	mu    sync.Mutex
	spans []gotSpan
	paths []string
}

func newCollector(t *testing.T) *collector {
	t.Helper()
	c := &collector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.paths = append(c.paths, r.URL.Path)
		for _, rs := range req.ResourceSpans {
			res := map[string]string{}
			for _, kv := range rs.Resource.Attributes {
				res[kv.Key] = kv.Value.GetStringValue()
			}
			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					c.spans = append(c.spans, gotSpan{res, s.Name, fmt.Sprintf("%x", s.TraceId)})
				}
			}
		}
		out, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

// byService counts received spans per service.name.
func (c *collector) byService() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := map[string]int{}
	for _, s := range c.spans {
		m[s.Resource["service.name"]]++
	}
	return m
}

func (c *collector) find(name string) (gotSpan, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range c.spans {
		if s.Name == name {
			return s, true
		}
	}
	return gotSpan{}, false
}

func (c *collector) requestPaths() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.paths...)
}
