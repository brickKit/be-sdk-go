package problem

import (
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"
)

// The two reasons ruled into domain be after rc.1 (stage-B review: 33 → 35) are catalogued with their
// codes and rendered texts until the pinned errors-be.yaml carries them.
func TestPendingBeReasonsAreCatalogued(t *testing.T) {
	ri := Be("REQUEST_INVALID", nil)
	if ri.Code != codes.InvalidArgument || ri.Reason != "REQUEST_INVALID" || ri.HTTPStatus() != 400 {
		t.Fatalf("REQUEST_INVALID: %+v", ri)
	}
	du := Be("DEPENDENCY_UNAVAILABLE", map[string]string{"dependency": "db"})
	if du.Code != codes.Unavailable || du.Reason != "DEPENDENCY_UNAVAILABLE" || du.HTTPStatus() != 503 {
		t.Fatalf("DEPENDENCY_UNAVAILABLE: %+v", du)
	}
	c := NewCatalogue()
	if title, d := c.Text(du, "en"); title == "DEPENDENCY_UNAVAILABLE" || d != "A service this request needs (db) cannot be reached right now. Try again shortly." {
		t.Fatalf("en text: %q %q", title, d)
	}
	if _, d := c.Text(du, "zh"); d != "这个请求依赖的服务（db）暂时连不上，请稍后再试。" {
		t.Fatalf("zh text: %q", d)
	}
	if err := ValidateReason("REQUEST_INVALID", "erp/x"); err == nil {
		t.Fatal("REQUEST_INVALID is reserved once catalogued")
	}
}

// SQLSTATE class 08 (connection exception) and 57P01–57P03 (shutdown, cannot connect now) are the
// database being unreachable: DEPENDENCY_UNAVAILABLE with dependency db (stage-B ruling; rc.2 draft
// vectors errors.sqlstate.connection-failure … cannot-connect-now).
func TestClassifyConnectionExceptionClass(t *testing.T) {
	for _, state := range []string{"08000", "08001", "08003", "08006", "08P01", "57P01", "57P02", "57P03"} {
		e := ClassifySQLState(state, 1, ContextNone, nil).Err
		if e.Reason != "DEPENDENCY_UNAVAILABLE" || e.Code != codes.Unavailable || e.Metadata["dependency"] != "db" {
			t.Fatalf("%s: %+v", state, e)
		}
	}
	if e := ClassifySQLState("53300", 1, ContextNone, nil).Err; e.Reason != "DB_TOO_MANY_CONNECTIONS" {
		t.Fatalf("53300: %+v", e)
	}
}

// P4.6 (rc.2 draft, vectors errors access_log_level): the access-log line's level by code — every
// code is logged, CANCELLED included.
func TestAccessLogLevelByCode(t *testing.T) {
	want := map[codes.Code]slog.Level{
		codes.OK: slog.LevelInfo, codes.Canceled: slog.LevelInfo, codes.Unknown: slog.LevelError,
		codes.InvalidArgument: slog.LevelInfo, codes.DeadlineExceeded: slog.LevelWarn, codes.NotFound: slog.LevelInfo,
		codes.ResourceExhausted: slog.LevelInfo, codes.Unimplemented: slog.LevelInfo, codes.Internal: slog.LevelError,
		codes.Unavailable: slog.LevelWarn, codes.DataLoss: slog.LevelError, codes.Unauthenticated: slog.LevelInfo,
	}
	for c, l := range want {
		if got := AccessLogLevel(c); got != l {
			t.Fatalf("%s: %v, want %v", CodeName(c), got, l)
		}
	}
}
