package errs

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func levelName(l slog.Level, ok bool) string {
	if !ok {
		return "none"
	}
	switch l {
	case slog.LevelError:
		return "error"
	case slog.LevelWarn:
		return "warn"
	}
	return "info"
}

// TestErrorsVectors runs the protocol's errors vectors: every platform reason, every code, every access-log status.
func TestErrorsVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/errors.json")
	require.NoError(t, err)
	var file struct {
		Cases []struct {
			ID, Kind, Reason, Domain, Type, Retryable string
			GRPCCode                                  string `json:"grpc_code"`
			HTTP                                      int
			LogLevel                                  string `json:"log_level"`
		}
	}
	require.NoError(t, json.Unmarshal(raw, &file))
	reasons := 0
	for _, c := range file.Cases {
		switch c.Kind {
		case "reason":
			reasons++
			e := New(c.Reason, "x")
			assert.Equal(t, c.Reason, e.Reason, c.ID)
			assert.Equal(t, c.Domain, e.Domain, c.ID)
			assert.Equal(t, Code(c.GRPCCode), e.Code, c.ID)
			assert.Equal(t, c.HTTP, e.HTTP, c.ID)
			assert.Equal(t, c.LogLevel, levelName(LogLevel(e.Code)), c.ID)
			entry, _ := Platform(c.Reason)
			assert.Equal(t, c.Retryable, entry.Retry, c.ID)
			if c.Reason != ReasonInternal {
				assert.Equal(t, c.Type, e.Problem("0").Type, c.ID)
			}
		case "code":
			assert.Equal(t, c.HTTP, HTTPStatus(Code(c.GRPCCode)), c.ID)
			assert.Equal(t, c.LogLevel, levelName(LogLevel(Code(c.GRPCCode))), c.ID)
		case "access_log":
			assert.Equal(t, c.LogLevel, levelName(AccessLogLevel(c.HTTP), true), c.ID)
		default:
			t.Errorf("%s: unknown kind %q", c.ID, c.Kind)
		}
	}
	assert.Equal(t, len(platform), reasons, "the SDK catalog and the protocol catalog must list the same reasons")
}

func TestInternalNeverLeaks(t *testing.T) {
	e := From(errors.New(`pq: relation "secret_table" does not exist`)).With("sql", "select 1")
	p := e.Problem("4bf92f3577b34da6a3ce929d0e0e4736")
	assert.Equal(t, ReasonInternal, p.Reason)
	assert.Equal(t, "be", p.Domain)
	assert.Equal(t, 500, p.Status)
	assert.NotContains(t, p.Detail, "secret_table")
	assert.Empty(t, p.Metadata)
	assert.Contains(t, e.Error(), "secret_table", "the cause stays available to the logs")
}

func TestUnknownReasonIsInternal(t *testing.T) {
	e := New("NO_SUCH_REASON", "x")
	assert.Equal(t, ReasonInternal, e.Reason)
	assert.ErrorContains(t, e, "NO_SUCH_REASON")
}

func TestContextErrors(t *testing.T) {
	assert.Equal(t, ReasonRequestCancelled, From(context.Canceled).Reason)
	assert.Equal(t, ReasonDeadlineExceeded, From(context.DeadlineExceeded).Reason)
	assert.Nil(t, From(nil))
}

func TestWriteHTTP(t *testing.T) {
	rec := httptest.NewRecorder()
	New(ReasonServiceBusy, "busy").With("resource", "queue").WithRetryAfter(1500*time.Millisecond).
		WriteHTTP(rec, "4bf92f3577b34da6a3ce929d0e0e4736")
	assert.Equal(t, 503, rec.Code)
	assert.Equal(t, ProblemContentType, rec.Header().Get("Content-Type"))
	assert.Equal(t, "2", rec.Header().Get("Retry-After"))
	var p map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	assert.Equal(t, "urn:be:be:SERVICE_BUSY", p["type"])
	assert.Equal(t, map[string]any{"resource": "queue"}, p["metadata"])
	assert.NotContains(t, p, "violations")
}

func TestDeclaredComponentReason(t *testing.T) {
	insufficient := Define("erp/inventory", "INSUFFICIENT_STOCK", FailedPrecondition, 409, RetryNo, "Not enough stock")
	e := insufficient.New("only 3 left").With("available", "3")
	assert.True(t, Is(e, "erp/inventory", "INSUFFICIENT_STOCK"))
	p := e.Problem("0")
	assert.Equal(t, "urn:be:erp/inventory:INSUFFICIENT_STOCK", p.Type)
	assert.Equal(t, 409, p.Status)
}

func TestWithDoesNotMutate(t *testing.T) {
	base := New(ReasonNotFound, "x")
	_ = base.With("k", "v")
	assert.Empty(t, base.Metadata)
}
