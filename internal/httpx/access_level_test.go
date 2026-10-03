package httpx

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// P3.10 / P4.6 (stage-B ruling, rc.2 draft text and vectors errors access_log_level): the access-log
// line's level follows the answer: ERROR for 500, WARN for 503 and 504, INFO for everything else
// (2xx, 4xx, 499, 501), with or without a problem body; ops endpoints (Quiet) stay at DEBUG.
func TestAccessLogLevelFollowsTheStatus(t *testing.T) {
	cases := []struct {
		name  string
		write func(w http.ResponseWriter, r *http.Request)
		level string
	}{
		{"200", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }, "INFO"},
		{"404 problem", func(w http.ResponseWriter, r *http.Request) {
			WriteProblem(w, r, problem.NewCatalogue(), "en", problem.Be("NOT_FOUND", nil))
		}, "INFO"},
		{"500 plain", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }, "ERROR"},
		{"503 problem", func(w http.ResponseWriter, r *http.Request) {
			WriteProblem(w, r, problem.NewCatalogue(), "en", problem.Be("AUTHZ_NOT_READY", nil))
		}, "WARN"},
		{"503 plain", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, "WARN"},
		{"501 plain", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(501) }, "INFO"},
		{"499 cancelled", func(w http.ResponseWriter, r *http.Request) {
			WriteProblem(w, r, problem.NewCatalogue(), "en", &problem.Error{Code: 1})
		}, "INFO"},
		{"504 problem", func(w http.ResponseWriter, r *http.Request) {
			WriteProblem(w, r, problem.NewCatalogue(), "en", problem.Be("DEADLINE_BUDGET_EXHAUSTED", nil))
		}, "WARN"},
	}
	for _, c := range cases {
		h, logs, _ := newTestHandler(http.HandlerFunc(c.write))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
		var line map[string]any
		for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var m map[string]any
			if json.Unmarshal([]byte(l), &m) == nil && m["msg"] == "http_request" {
				line = m
			}
		}
		if line == nil || line["level"] != c.level {
			t.Fatalf("%s: %v\n%s", c.name, line, logs)
		}
	}
}

func TestAccessLogLevelOfOpsEndpointsIsDebug(t *testing.T) {
	h, _, _ := newTestHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
	var logs bytes.Buffer
	h.Logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h.Quiet = func(p string) bool { return p == "/healthz" }
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/healthz", nil))
	if !strings.Contains(logs.String(), `"level":"DEBUG"`) {
		t.Fatalf("%s", logs.String())
	}
}
