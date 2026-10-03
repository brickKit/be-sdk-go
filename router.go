package besdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/internal/httpx"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
)

// DefaultBodyLimit is the request body limit of a route that declares none (P3.6).
const DefaultBodyLimit int64 = 1 << 20

// Guard is the one access rule every business route declares (P6.2): a permission key, Public or
// Authenticated (later waves add generated resource guards). There is no route without one.
type Guard interface{ guard() }

// PermKey is a permission key guard, e.g. "erp.sales.view".
type PermKey string

func (PermKey) guard() {}

// The two guards that are not permission keys.
const (
	Public        PermKey = ""                  // no token needed
	Authenticated PermKey = "__authenticated__" // any verified, non-stale token
)

// guardian decides a guard for one request (the P6.2 chain); nil error = allowed.
type guardian interface {
	check(c *gin.Context, g Guard) *problem.Error
}

type routerConfig struct {
	componentID     string
	catalogue       *problem.Catalogue
	locale          string
	defaultDeadline time.Duration
	guardian        guardian
}

// Router registers user-plane routes under /{domain}/{name} (P3.1); the engine and the middleware are
// the SDK's.
type Router struct {
	group *gin.RouterGroup
	cfg   routerConfig
}

func newRouter(eng *gin.Engine, cfg routerConfig) *Router {
	return &Router{group: eng.Group("/" + cfg.componentID), cfg: cfg}
}

type routeOptions struct {
	deadline  time.Duration
	bodyLimit int64
}

// RouteOption tunes one route; the same numbers belong on its OpenAPI operation as
// x-be-deadline-seconds and x-be-max-body-bytes (P3.13).
type RouteOption func(*routeOptions)

// Timeout sets the route's deadline (P3.4; orchestrating routes declare 15 s).
func Timeout(d time.Duration) RouteOption { return func(o *routeOptions) { o.deadline = d } }

// BodyLimit sets the route's request body limit (P3.6).
func BodyLimit(n int64) RouteOption { return func(o *routeOptions) { o.bodyLimit = n } }

// GET registers a GET route.
func GET(r *Router, path string, g Guard, h gin.HandlerFunc, o ...RouteOption) {
	r.handle(http.MethodGet, path, g, h, o)
}

// POST registers a POST route.
func POST(r *Router, path string, g Guard, h gin.HandlerFunc, o ...RouteOption) {
	r.handle(http.MethodPost, path, g, h, o)
}

// PUT registers a PUT route.
func PUT(r *Router, path string, g Guard, h gin.HandlerFunc, o ...RouteOption) {
	r.handle(http.MethodPut, path, g, h, o)
}

// PATCH registers a PATCH route.
func PATCH(r *Router, path string, g Guard, h gin.HandlerFunc, o ...RouteOption) {
	r.handle(http.MethodPatch, path, g, h, o)
}

// DELETE registers a DELETE route.
func DELETE(r *Router, path string, g Guard, h gin.HandlerFunc, o ...RouteOption) {
	r.handle(http.MethodDelete, path, g, h, o)
}

func (r *Router) handle(method, path string, g Guard, h gin.HandlerFunc, opts []RouteOption) {
	if g == nil {
		panic(fmt.Sprintf("besdk: route %s %s has no guard; declare a permission key, Public or Authenticated", method, path))
	}
	o := routeOptions{deadline: r.cfg.defaultDeadline, bodyLimit: DefaultBodyLimit}
	for _, opt := range opts {
		opt(&o)
	}
	r.group.Handle(method, path, r.prelude(o, g), h)
}

// prelude runs before the handler: route and deadline (P3.4), body limit (P3.6), guard (P6.2).
func (r *Router) prelude(o routeOptions, g Guard) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request = c.Request.WithContext(httpx.SetRoute(c.Request.Context(), c.FullPath(), o.deadline))
		c.Set(ctxRouterKey, &r.cfg)
		if c.Request.ContentLength > o.bodyLimit {
			fail(c, &r.cfg, bodyTooLarge(o.bodyLimit))
			c.Abort()
			return
		}
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, o.bodyLimit)
		}
		c.Set(ctxBodyLimitKey, o.bodyLimit)
		if e := r.cfg.guardian.check(c, g); e != nil {
			fail(c, &r.cfg, e)
			c.Abort()
		}
	}
}

const (
	ctxRouterKey    = "besdk.router"
	ctxBodyLimitKey = "besdk.bodyLimit"
)

func bodyTooLarge(limit int64) *problem.Error {
	return problem.Be("BODY_TOO_LARGE", map[string]string{"limit": strconv.FormatInt(limit, 10)})
}

// Bind decodes the JSON body into T. A body over the route's limit answers BODY_TOO_LARGE (413); a
// body that does not decode answers INVALID_ARGUMENT, reason INVALID_BODY of the component's own
// domain, with one field violation (the component lists INVALID_BODY in its errors.yaml).
func Bind[T any](c *gin.Context) (T, error) {
	var v T
	if c.Request.Body == nil {
		return v, invalidBody(c, "body", "a JSON body is required")
	}
	err := json.NewDecoder(c.Request.Body).Decode(&v)
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return v, bodyTooLarge(tooLarge.Limit)
	case errors.Is(err, io.EOF):
		return v, invalidBody(c, "body", "a JSON body is required")
	case err != nil:
		field := "body"
		var te *json.UnmarshalTypeError
		if errors.As(err, &te) && te.Field != "" {
			field = te.Field
		}
		return v, invalidBody(c, field, err.Error())
	}
	return v, nil
}

func invalidBody(c *gin.Context, field, desc string) error {
	domain := ""
	if cfg, ok := c.Get(ctxRouterKey); ok {
		domain = cfg.(*routerConfig).componentID
	}
	e := problem.New(codes.InvalidArgument, domain, "INVALID_BODY", map[string]string{}, "the request body is not valid")
	e.Violations = []problem.Violation{{Field: field, Reason: "INVALID", Description: desc}}
	return e
}

// Respond writes v as JSON with status.
func Respond(c *gin.Context, status int, v any) { c.JSON(status, v) }

// Fail answers err as problem+json (P4): a protocol error keeps its code and reason; anything else is
// INTERNAL with a generic text, the original only in the log (P4.3).
func Fail(c *gin.Context, err error) {
	cfg, _ := c.Get(ctxRouterKey)
	rc, _ := cfg.(*routerConfig)
	fail(c, rc, problem.From(err))
}

func fail(c *gin.Context, cfg *routerConfig, e *problem.Error) {
	cat, locale := problem.NewCatalogue(), "zh-CN"
	if cfg != nil {
		cat, locale = cfg.catalogue, cfg.locale
	}
	httpx.WriteProblem(c.Writer, c.Request, cat, locale, e)
}
