package problem

import (
	"encoding/json"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
)

// ContentType is the media type of every 4xx/5xx REST body (P4.1).
const ContentType = "application/problem+json"

// Request is what the body takes from the request.
type Request struct {
	Path      string `json:"path"`
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id"`
}

// Problem is the RFC 9457 body with the AIP-193 members (P4.1, schemas/problem.schema.json).
type Problem struct {
	Type       string            `json:"type"`
	Title      string            `json:"title"`
	Status     int               `json:"status"`
	Code       string            `json:"code"`
	Reason     string            `json:"reason"`
	Domain     string            `json:"domain"`
	Detail     string            `json:"detail"`
	Metadata   map[string]string `json:"metadata"`
	Violations []Violation       `json:"violations,omitempty"`
	Instance   string            `json:"instance"`
	RequestID  string            `json:"request_id"`
	TraceID    string            `json:"trace_id"`
}

// Problem renders the body a caller sees: the public form of e (P4.3) with its texts in locale.
func (c *Catalogue) Problem(e *Error, req Request, locale string) Problem {
	pub := Public(e)
	title, detail := c.Text(pub, locale)
	meta := pub.Metadata
	if meta == nil {
		meta = map[string]string{}
	}
	return Problem{
		Type: "urn:be:" + pub.Domain + ":" + pub.Reason, Title: title, Status: pub.HTTPStatus(),
		Code: CodeName(pub.Code), Reason: pub.Reason, Domain: pub.Domain, Detail: detail,
		Metadata: meta, Violations: pub.Violations,
		Instance: req.Path, RequestID: req.RequestID, TraceID: req.TraceID,
	}
}

// RetryAfterHeader is the Retry-After value for an error (P4.2): only with 429 and 503, only with a
// delay, in whole seconds rounded up; "" means no header.
func RetryAfterHeader(code codes.Code, delay time.Duration) string {
	if delay <= 0 || (code != codes.ResourceExhausted && code != codes.Unavailable) {
		return ""
	}
	secs := (delay + time.Second - 1) / time.Second
	return strconv.FormatInt(int64(secs), 10)
}

// RestoreHTTP restores a dependency's REST error as the caller sees it (P8.2): a problem body with a
// known code, a reason and a domain is kept exactly; otherwise the code comes from the status and no
// reason is invented.
func RestoreHTTP(status int, body []byte) *Error {
	var p struct {
		Code       string            `json:"code"`
		Reason     string            `json:"reason"`
		Domain     string            `json:"domain"`
		Detail     string            `json:"detail"`
		Metadata   map[string]string `json:"metadata"`
		Violations []Violation       `json:"violations"`
	}
	if len(body) > 0 && json.Unmarshal(body, &p) == nil && p.Reason != "" && p.Domain != "" {
		if code, err := ParseCode(p.Code); err == nil {
			return &Error{Code: code, Domain: p.Domain, Reason: p.Reason, Metadata: p.Metadata,
				Detail: p.Detail, Violations: p.Violations}
		}
	}
	return &Error{Code: codeFromStatus(status)}
}
