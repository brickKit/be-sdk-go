// Package httpx is the user surface: one Gin engine per member, the SDK's middleware chain written once (request ID,
// trace, access log and metrics, deadline, body limit, error mapping, panic recovery), and route registration in which
// every route declares its operation.
package httpx

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Handler handles one user-surface request. Returning an error renders it as problem+json (protocol chapter 03).
type Handler func(*Context) error

// Route is one user-surface route.
type Route struct {
	Method string
	Path   string // Gin syntax, under the component's own prefix
	// Operation is the operation this route performs, as declared in the component's resource definition. Required
	// unless Public: a route without an operation would have no authorization check at all.
	Operation string
	// Public marks a route that is deliberately open (no operation, no authorization). It has to be said explicitly.
	Public bool
	// Orchestration routes get the longer default deadline (15 s instead of 10 s, protocol P4.3).
	Orchestration bool
	// Timeout overrides the default deadline of this route.
	Timeout time.Duration
	// MaxBody caps the request body (default 1 MiB).
	MaxBody int64
	Handle  Handler
}

// Default deadlines and limits (protocol 17's starting values).
const (
	DefaultDeadline       = 10 * time.Second
	OrchestrationDeadline = 15 * time.Second
	DefaultMaxBody        = 1 << 20
)

func (r Route) deadline() time.Duration {
	switch {
	case r.Timeout > 0:
		return r.Timeout
	case r.Orchestration:
		return OrchestrationDeadline
	}
	return DefaultDeadline
}

func (r Route) maxBody() int64 {
	if r.MaxBody > 0 {
		return r.MaxBody
	}
	return DefaultMaxBody
}

// Routes collects a module's routes.
type Routes struct{ list []Route }

// Add registers a route.
func (rs *Routes) Add(r Route) { rs.list = append(rs.list, r) }

// GET registers a GET route performing operation.
func (rs *Routes) GET(path, operation string, h Handler) {
	rs.Add(Route{Method: http.MethodGet, Path: path, Operation: operation, Handle: h})
}

// POST registers a POST route performing operation.
func (rs *Routes) POST(path, operation string, h Handler) {
	rs.Add(Route{Method: http.MethodPost, Path: path, Operation: operation, Handle: h})
}

// List returns the registered routes.
func (rs *Routes) List() []Route { return append([]Route(nil), rs.list...) }

var reserved = []string{"/healthz", "/readyz", "/metrics", "/.well-known/"}

// Validate checks every route before anything listens: an operation unless public, a handler, no clash with the
// platform's own endpoints, no duplicates. All problems are reported together.
func (rs *Routes) Validate() error {
	var problems []string
	seen := map[string]bool{}
	for _, r := range rs.list {
		id := r.Method + " " + r.Path
		switch {
		case r.Method == "" || !strings.HasPrefix(r.Path, "/"):
			problems = append(problems, fmt.Sprintf("%s: a method and an absolute path are required", id))
		case r.Handle == nil:
			problems = append(problems, fmt.Sprintf("%s: no handler", id))
		case r.Operation == "" && !r.Public:
			problems = append(problems, fmt.Sprintf("%s: declares no operation; declare it, or mark the route Public explicitly", id))
		case r.Operation != "" && r.Public:
			problems = append(problems, fmt.Sprintf("%s: a public route has no operation", id))
		case seen[id]:
			problems = append(problems, fmt.Sprintf("%s: registered twice", id))
		}
		for _, p := range reserved {
			if r.Path == strings.TrimSuffix(p, "/") || strings.HasPrefix(r.Path, p) {
				problems = append(problems, fmt.Sprintf("%s: %s belongs to the platform", id, p))
			}
		}
		seen[id] = true
	}
	if len(problems) > 0 {
		return errors.New("routes: " + strings.Join(problems, "; "))
	}
	return nil
}
