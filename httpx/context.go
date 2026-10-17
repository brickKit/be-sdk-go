package httpx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/gin-gonic/gin"
)

// Context is what a handler gets: the request, its context (with the deadline), the request and trace IDs, and the
// member's logger already tagged with both.
type Context struct {
	gin       *gin.Context
	route     Route
	requestID string
	traceID   string
	logger    *slog.Logger
}

// Ctx is the request's context: it carries the deadline and the trace; pass it to every call.
func (c *Context) Ctx() context.Context { return c.gin.Request.Context() }

// Request is the underlying HTTP request.
func (c *Context) Request() *http.Request { return c.gin.Request }

// Param returns a path parameter.
func (c *Context) Param(name string) string { return c.gin.Param(name) }

// Query returns a query parameter.
func (c *Context) Query(name string) string { return c.gin.Query(name) }

// RequestID is the request's ID (echoed in X-Request-Id).
func (c *Context) RequestID() string { return c.requestID }

// TraceID is the W3C trace ID of this request.
func (c *Context) TraceID() string { return c.traceID }

// Operation is the operation the route declared.
func (c *Context) Operation() string { return c.route.Operation }

// Logger is the member's logger with request_id and trace_id.
func (c *Context) Logger() *slog.Logger { return c.logger }

// BindJSON decodes the body into v. A body over the route's limit is BODY_TOO_LARGE; anything that does not decode
// is REQUEST_INVALID. Unknown fields are accepted (contracts only grow, protocol 19).
func (c *Context) BindJSON(v any) error {
	dec := json.NewDecoder(c.gin.Request.Body)
	if err := dec.Decode(v); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return errs.New(errs.ReasonBodyTooLarge, "the request body is too large")
		}
		if errors.Is(err, io.EOF) {
			return errs.New(errs.ReasonRequestInvalid, "the request body is empty")
		}
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return errs.New(errs.ReasonRequestInvalid, "a field has the wrong type").WithViolations(errs.Violation{
				Field: "/" + typeErr.Field, Reason: "WRONG_TYPE", Description: "expected " + typeErr.Type.String()})
		}
		return errs.New(errs.ReasonRequestInvalid, "the request body is not valid JSON")
	}
	return nil
}

// JSON writes a JSON response.
func (c *Context) JSON(status int, v any) error {
	c.gin.JSON(status, v)
	return nil
}

// NoContent writes an empty response with the status.
func (c *Context) NoContent(status int) error {
	c.gin.Status(status)
	return nil
}
