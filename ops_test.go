package besdk

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/prometheus/client_golang/prometheus"
)

func newTestOps(ready *readiness) *opsHandler {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "be_test_total", Help: "t"})
	reg.MustRegister(c)
	c.Inc()
	return &opsHandler{
		ready: ready, gatherer: reg, catalogue: problem.NewCatalogue(), locale: "en",
		info: func() Info {
			return Info{ComponentID: "erp/sales", ComponentVersion: "3.0.0", Protocol: ProtocolVersion,
				SDK: &SDKInfo{Name: "be-sdk-go", Version: Version}, Language: LanguageInfo{Name: "go", Version: "1.25.11"},
				Profiles: []string{"core"}, Ports: map[string]int{"http": 8085}}
		},
		next: http.NotFoundHandler(),
	}
}

func TestHealthzAlwaysOK(t *testing.T) {
	h := newTestOps(newReadiness("db_identity"))
	for _, m := range []string{"GET", "HEAD"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(m, "/healthz", nil))
		if rec.Code != 200 {
			t.Fatalf("%s /healthz = %d", m, rec.Code)
		}
	}
}

func TestReadyzNotReadyThenReady(t *testing.T) {
	r := newReadiness("bundle", "db_identity")
	h := newTestOps(r)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	var p problem.Problem
	_ = json.Unmarshal(rec.Body.Bytes(), &p)
	if rec.Code != 503 || p.Reason != "NOT_READY" || p.Metadata["waiting"] != "bundle,db_identity" {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	r.Set("bundle", true)
	r.Set("db_identity", true)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != 200 {
		t.Fatalf("ready: %d", rec.Code)
	}
}

func TestMetricsAndInfo(t *testing.T) {
	h := newTestOps(newReadiness())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "be_test_total 1") {
		t.Fatalf("metrics: %d %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/_be/info", nil))
	var info map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info["component_id"] != "erp/sales" || info["protocol"] != "1.0" || info["members"] != nil {
		t.Fatalf("info: %s", rec.Body)
	}
	if _, ok := info["members"]; !ok {
		t.Fatalf("members must be present (null): %s", rec.Body)
	}
	if m := info["migrations"].(map[string]any); m["component"] != nil || m["platform"] != nil {
		t.Fatalf("migrations without a database are null: %s", rec.Body)
	}
}

func TestOpsPassesOtherPaths(t *testing.T) {
	h := newTestOps(newReadiness())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/erp/sales/orders", nil))
	if rec.Code != 404 {
		t.Fatalf("%d", rec.Code)
	}
}
