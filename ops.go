package besdk

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/httpx"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Version is this SDK's version, reported in /_be/info (P20.3).
const Version = "0.6.0"

// ProtocolVersion is the be-protocol version this SDK implements (P20.3).
const ProtocolVersion = "1.0"

const opsDeadline = 5 * time.Second

// Info is the body of GET /_be/info (P20.4, schemas/info.schema.json).
type Info struct {
	ComponentID      string         `json:"component_id"`
	ComponentVersion string         `json:"component_version"`
	Protocol         string         `json:"protocol"`
	SDK              *SDKInfo       `json:"sdk"`
	Language         LanguageInfo   `json:"language"`
	Profiles         []string       `json:"profiles"`
	Ports            map[string]int `json:"ports"`
	Migrations       MigrationsInfo `json:"migrations"`
	Capabilities     []string       `json:"capabilities,omitempty"`
	Degraded         []string       `json:"degraded,omitempty"`
	Members          []Info         `json:"members"`
}

// SDKInfo names the official SDK.
type SDKInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// LanguageInfo names the language and its runtime version.
type LanguageInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// MigrationsInfo reports the newest component migration in the image and the platform migration
// version; both null for a component without a database.
type MigrationsInfo struct {
	Component *string `json:"component"`
	Platform  *int    `json:"platform"`
}

// opsHandler serves the operations endpoints of P1 on the main port and passes every other path on.
// None of them touches a dependency (P1.3) or needs a token.
type opsHandler struct {
	ready     *readiness
	gatherer  prometheus.Gatherer
	catalogue *problem.Catalogue
	locale    string
	info      func() Info
	next      http.Handler
}

func (h *opsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			r = r.WithContext(httpx.SetRoute(r.Context(), "/healthz", opsDeadline))
			w.WriteHeader(http.StatusOK)
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte("ok"))
			}
			return
		}
	case "/readyz":
		r = r.WithContext(httpx.SetRoute(r.Context(), "/readyz", opsDeadline))
		if waiting := h.ready.Waiting(); len(waiting) > 0 {
			httpx.WriteProblem(w, r, h.catalogue, h.locale,
				problem.Be("NOT_READY", map[string]string{"waiting": strings.Join(waiting, ",")}))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
		return
	case "/metrics":
		r = r.WithContext(httpx.SetRoute(r.Context(), "/metrics", opsDeadline))
		promhttp.HandlerFor(h.gatherer, promhttp.HandlerOpts{}).ServeHTTP(w, r)
		return
	case "/_be/info":
		r = r.WithContext(httpx.SetRoute(r.Context(), "/_be/info", opsDeadline))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(h.info())
		return
	}
	h.next.ServeHTTP(w, r)
}

// isOpsPath reports the operations paths, whose access-log lines go out at debug level so probes
// every few seconds do not drown the business lines.
func isOpsPath(p string) bool {
	return p == "/healthz" || p == "/readyz" || p == "/metrics" || p == "/_be/info"
}
