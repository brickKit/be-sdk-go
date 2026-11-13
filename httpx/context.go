package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

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
	body      []byte
	bodyRead  bool
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

// Body returns the request body's bytes, read once (up to the route's limit; over it is BODY_TOO_LARGE).
func (c *Context) Body() ([]byte, error) {
	if !c.bodyRead {
		b, err := io.ReadAll(c.gin.Request.Body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				return nil, errs.New(errs.ReasonBodyTooLarge, "the request body is too large")
			}
			return nil, errs.New(errs.ReasonRequestInvalid, "the request body could not be read")
		}
		c.body, c.bodyRead = b, true
	}
	return c.body, nil
}

// BindJSON decodes the body into v. A body over the route's limit is BODY_TOO_LARGE; anything that does not decode
// is REQUEST_INVALID. Unknown fields are accepted (contracts only grow, protocol 19).
func (c *Context) BindJSON(v any) error {
	body, err := c.Body()
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return errs.New(errs.ReasonRequestInvalid, "the request body is empty")
	}
	if err := json.Unmarshal(body, v); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return errs.New(errs.ReasonRequestInvalid, "a field has the wrong type").WithViolations(errs.Violation{
				Field: "/" + typeErr.Field, Reason: "WRONG_TYPE", Description: "expected " + typeErr.Type.String()})
		}
		return errs.New(errs.ReasonRequestInvalid, "the request body is not valid JSON")
	}
	return nil
}

// Header returns a request header.
func (c *Context) Header(name string) string { return c.gin.GetHeader(name) }

// JSON writes a JSON response.
func (c *Context) JSON(status int, v any) error {
	c.gin.JSON(status, v)
	return nil
}

// Bytes writes a body of the media type as it is: a document the user downloads, such as a rendered PDF. Errors still
// answer problem JSON (return them instead of writing).
func (c *Context) Bytes(status int, mediaType string, body []byte) error {
	if mediaType == "" {
		return errs.New(errs.ReasonInternal, "a byte response needs its media type")
	}
	c.gin.Data(status, mediaType, body)
	return nil
}

// Download streams size bytes as a file the user saves, named fileName: the headers keep the content from running in
// the application's origin whatever it is (an uploaded HTML or SVG): Content-Disposition attachment (RFC 5987 file
// name), X-Content-Type-Options nosniff, Content-Security-Policy sandbox, no caching (assembly protocol 12's strict
// downloads). Errors before the first byte still answer problem JSON (return them instead of calling Download).
func (c *Context) Download(mediaType, fileName string, size int64, body io.Reader) error {
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	h := c.gin.Writer.Header()
	h.Set("Content-Disposition", "attachment; filename*=UTF-8''"+rfc5987(fileName))
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "sandbox; default-src 'none'")
	h.Set("Cache-Control", "no-store")
	c.gin.DataFromReader(http.StatusOK, size, mediaType, body, nil)
	return nil
}

// rfc5987 encodes a file name for filename*= (RFC 5987): unreserved characters as they are, the rest percent-encoded.
func rfc5987(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for _, ch := range []byte(s) {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.IndexByte("-._~", ch) >= 0 {
			b.WriteByte(ch)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[ch>>4])
		b.WriteByte(hex[ch&15])
	}
	return b.String()
}

// NoContent writes an empty response with the status.
func (c *Context) NoContent(status int) error {
	c.gin.Status(status)
	return nil
}

// SetCookie adds a Set-Cookie header to the response (the identity member's session cookie, protocol 20). Call it
// before writing the response.
func (c *Context) SetCookie(ck *http.Cookie) {
	http.SetCookie(c.gin.Writer, ck)
}

// Redirect answers with a redirect to location; status is 302, 303 or 307 (a login's hops, protocol 20).
func (c *Context) Redirect(status int, location string) error {
	if status != http.StatusFound && status != http.StatusSeeOther && status != http.StatusTemporaryRedirect {
		return errs.New(errs.ReasonInternal, "a redirect is 302, 303 or 307")
	}
	c.gin.Redirect(status, location)
	return nil
}
