package problem

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/vectors"
)

func decode(t *testing.T, raw json.RawMessage, into any) {
	t.Helper()
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("decode input %s: %v", raw, err)
	}
}

func reasonOf(err error) string {
	if inv, ok := err.(*Invalid); ok {
		return inv.Reason
	}
	return "<" + err.Error() + ">"
}

func TestVectorsCodes(t *testing.T) {
	vectors.Run(t, "errors", "codes", map[string]func(*testing.T, vectors.Case){
		"grpc_to_http": func(t *testing.T, c vectors.Case) {
			var in struct{ Code, Reason, Domain string }
			decode(t, c.Input, &in)
			code, err := ParseCode(in.Code)
			if err != nil {
				vectors.RequireReason(t, c, reasonOf(err))
				return
			}
			vectors.RequireJSON(t, c, map[string]any{"number": int(code), "http": HTTPStatus(code, in.Domain, in.Reason)})
		},
		"be_reason": func(t *testing.T, c vectors.Case) {
			var in struct{ Reason string }
			decode(t, c.Input, &in)
			e := Be(in.Reason, nil)
			vectors.RequireJSON(t, c, map[string]any{"code": CodeName(e.Code), "domain": e.Domain, "http": e.HTTPStatus()})
		},
		"restore_http": func(t *testing.T, c vectors.Case) {
			var in struct {
				Status  int             `json:"status"`
				Problem json.RawMessage `json:"problem"`
			}
			decode(t, c.Input, &in)
			body := []byte(in.Problem)
			if string(body) == "null" {
				body = nil
			}
			e := RestoreHTTP(in.Status, body)
			vectors.RequireJSON(t, c, map[string]any{"code": CodeName(e.Code), "reason": nullable(e.Reason),
				"domain": nullable(e.Domain), "http": in.Status})
		},
	})
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func TestVectorsSQLState(t *testing.T) {
	vectors.Run(t, "errors", "sqlstate", map[string]func(*testing.T, vectors.Case){
		"classify": func(t *testing.T, c vectors.Case) {
			var in struct {
				SQLState string `json:"sqlstate"`
				Attempt  int    `json:"attempt"`
				Context  string `json:"context"`
				Mapping  *struct {
					Code, Reason, Domain string
				} `json:"component_mapping"`
			}
			decode(t, c.Input, &in)
			var mapped *Error
			if in.Mapping != nil {
				code, err := ParseCode(in.Mapping.Code)
				if err != nil {
					t.Fatal(err)
				}
				mapped = New(code, in.Mapping.Domain, in.Mapping.Reason, nil, "")
			}
			ctxKind := map[string]ContextState{"": ContextNone, "none": ContextNone,
				"deadline_exceeded": ContextDeadlineExceeded, "cancelled": ContextCancelled}[in.Context]
			got := ClassifySQLState(in.SQLState, max(in.Attempt, 1), ctxKind, mapped)
			if got.Retry {
				vectors.RequireJSON(t, c, map[string]any{"action": "retry", "base_delay_ms": got.BaseDelay.Milliseconds()})
				return
			}
			e := got.Err
			vectors.RequireJSON(t, c, map[string]any{"action": "fail", "code": CodeName(e.Code),
				"reason": nullable(e.Reason), "domain": nullable(e.Domain), "http": e.HTTPStatus()})
		},
	})
}

func TestVectorsLevels(t *testing.T) {
	vectors.Run(t, "errors", "levels", map[string]func(*testing.T, vectors.Case){
		"log_level": func(t *testing.T, c vectors.Case) {
			var in struct{ Code string }
			decode(t, c.Input, &in)
			code, err := ParseCode(in.Code)
			if err != nil {
				t.Fatal(err)
			}
			level, logged := LogLevel(code)
			name := "none"
			if logged {
				name = strings.ToLower(level.String())
			}
			vectors.RequireJSON(t, c, map[string]any{"level": name})
		},
	})
}

func TestVectorsProblem(t *testing.T) {
	cat := NewCatalogue()
	vectors.Run(t, "errors", "problem", map[string]func(*testing.T, vectors.Case){
		"problem": func(t *testing.T, c vectors.Case) {
			var in struct {
				Error struct {
					Code            string         `json:"code"`
					Reason          string         `json:"reason"`
					Domain          string         `json:"domain"`
					Metadata        map[string]any `json:"metadata"`
					Violations      []Violation    `json:"violations"`
					InternalMessage string         `json:"internal_message"`
				} `json:"error"`
				Request Request `json:"request"`
			}
			decode(t, c.Input, &in)
			meta, err := StringMetadata(in.Error.Metadata)
			if err != nil {
				vectors.RequireReason(t, c, reasonOf(err))
				return
			}
			e := &Error{Domain: in.Error.Domain, Reason: in.Error.Reason, Metadata: meta,
				Violations: in.Error.Violations, Detail: in.Error.InternalMessage}
			if in.Error.Code != "" {
				if e.Code, err = ParseCode(in.Error.Code); err != nil {
					t.Fatal(err)
				}
			}
			p := cat.Problem(e, in.Request, "en")
			raw, _ := json.Marshal(p)
			var body map[string]any
			_ = json.Unmarshal(raw, &body)
			detail, _ := body["detail"].(string)
			if body["title"] == "" || detail == "" {
				t.Fatalf("title and detail must be present: %s", raw)
			}
			delete(body, "title")
			delete(body, "detail")
			var want struct {
				NotContain []string `json:"detail_must_not_contain"`
			}
			_ = json.Unmarshal(c.Expected, &want)
			for _, s := range want.NotContain {
				if strings.Contains(detail, s) {
					t.Fatalf("detail %q leaks %q", detail, s)
				}
			}
			got := map[string]any{"content_type": ContentType, "body": body}
			if len(want.NotContain) > 0 {
				got["detail_must_not_contain"] = want.NotContain
			}
			vectors.RequireJSON(t, c, got)
		},
		"retry_after": func(t *testing.T, c vectors.Case) {
			var in struct {
				Code    string `json:"code"`
				DelayMS *int64 `json:"retry_delay_ms"`
			}
			decode(t, c.Input, &in)
			code, err := ParseCode(in.Code)
			if err != nil {
				t.Fatal(err)
			}
			var d time.Duration
			if in.DelayMS != nil {
				d = time.Duration(*in.DelayMS) * time.Millisecond
			}
			vectors.RequireJSON(t, c, map[string]any{"header": nullable(RetryAfterHeader(code, d))})
		},
		"reason_name": func(t *testing.T, c vectors.Case) {
			var in struct{ Reason, Domain string }
			decode(t, c.Input, &in)
			if err := ValidateReason(in.Reason, in.Domain); err != nil {
				vectors.RequireReason(t, c, reasonOf(err))
				return
			}
			vectors.RequireJSON(t, c, map[string]any{"valid": true})
		},
	})
}
