package logx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// lineSink records each Write call separately, so a test can tell one write per record.
type lineSink struct {
	mu     sync.Mutex
	writes [][]byte
}

func (s *lineSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (s *lineSink) lines() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.writes))
	for i, w := range s.writes {
		out[i] = string(w)
	}
	return out
}

var fixedTime = time.Date(2026, 10, 2, 8, 0, 0, 120000000, time.FixedZone("CST", 8*3600))

func newTestHandler(w *lineSink, lvl slog.Leveler) slog.Handler {
	return NewHandler(Options{ComponentID: "erp/sales", ComponentVersion: "2.0.0", Level: lvl, Writer: w})
}

// handle logs one record with a fixed time through h.
func handle(t *testing.T, h slog.Handler, ctx context.Context, lvl slog.Level, msg string, attrs ...slog.Attr) {
	t.Helper()
	r := slog.NewRecord(fixedTime, lvl, msg, 0)
	r.AddAttrs(attrs...)
	if err := h.Handle(ctx, r); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// keysOf returns the top-level keys of one JSON line in order, and its decoded object.
func keysOf(t *testing.T, line string) ([]string, map[string]any) {
	t.Helper()
	d := json.NewDecoder(strings.NewReader(line))
	d.UseNumber()
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		t.Fatalf("not an object: %q (%v)", line, err)
	}
	var keys []string
	obj := map[string]any{}
	for d.More() {
		k, err := d.Token()
		if err != nil {
			t.Fatalf("token: %v in %q", err, line)
		}
		var v any
		if err := d.Decode(&v); err != nil {
			t.Fatalf("value: %v in %q", err, line)
		}
		keys = append(keys, k.(string))
		obj[k.(string)] = v
	}
	if _, err := d.Token(); err != nil {
		t.Fatalf("closing: %v", err)
	}
	return keys, obj
}

func TestHandlerEnvelopeFirstInOrder(t *testing.T) {
	w := &lineSink{}
	handle(t, newTestHandler(w, nil), context.Background(), slog.LevelWarn, "stock_low", slog.Int("qty", 3))
	lines := w.lines()
	if len(lines) != 1 || !strings.HasSuffix(lines[0], "}\n") || strings.Count(lines[0], "\n") != 1 {
		t.Fatalf("want one newline-terminated line per write, got %q", lines)
	}
	want := `{"time":"2026-10-02T00:00:00.120000000Z","level":"warn","msg":"stock_low","component_id":"erp/sales","component_version":"2.0.0","qty":3}` + "\n"
	if lines[0] != want {
		t.Fatalf("got  %s\nwant %s", lines[0], want)
	}
}

func TestHandlerTraceFromActiveSpan(t *testing.T) {
	w := &lineSink{}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{0x4b, 0xf9, 1}, SpanID: trace.SpanID{0, 0xf0, 2}, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	handle(t, newTestHandler(w, nil), ctx, slog.LevelInfo, "m", slog.String("a", "b"))
	keys, obj := keysOf(t, w.lines()[0])
	if got := strings.Join(keys, ","); got != "time,level,msg,component_id,component_version,trace_id,span_id,a" {
		t.Fatalf("keys %s", got)
	}
	if obj["trace_id"] != "4bf90100000000000000000000000000" || obj["span_id"] != "00f0020000000000" {
		t.Fatalf("ids %v %v", obj["trace_id"], obj["span_id"])
	}
}

func TestHandlerNoTraceWithoutSpan(t *testing.T) {
	w := &lineSink{}
	handle(t, newTestHandler(w, nil), context.Background(), slog.LevelInfo, "m")
	_, obj := keysOf(t, w.lines()[0])
	if _, ok := obj["trace_id"]; ok {
		t.Fatal("trace_id without a span")
	}
}

func TestHandlerContextFieldsBeforeAttrsLaterWins(t *testing.T) {
	w := &lineSink{}
	ctx := WithFields(context.Background(), slog.String("request_id", "r-1"), slog.String("sub", "u1"))
	ctx = WithFields(ctx, slog.String("perm", "erp.sales.view"), slog.String("sub", "u2"))
	h := newTestHandler(w, nil).WithAttrs([]slog.Attr{slog.String("h", "1")})
	handle(t, h, ctx, slog.LevelInfo, "m", slog.String("x", "y"), slog.String("x", "z"))
	keys, obj := keysOf(t, w.lines()[0])
	if got := strings.Join(keys[5:], ","); got != "request_id,sub,perm,h,x" {
		t.Fatalf("keys %s", got)
	}
	if obj["sub"] != "u2" || obj["x"] != "z" {
		t.Fatalf("later value must win: %v", obj)
	}
}

func TestWithFieldsDoesNotLeakIntoParent(t *testing.T) {
	parent := WithFields(context.Background(), slog.String("a", "1"))
	_ = WithFields(parent, slog.String("b", "2"))
	child2 := WithFields(parent, slog.String("c", "3"))
	got := ContextFields(child2)
	if len(got) != 2 || got[0].Key != "a" || got[1].Key != "c" {
		t.Fatalf("got %v", got)
	}
	if len(ContextFields(parent)) != 1 || len(ContextFields(context.Background())) != 0 {
		t.Fatal("parent changed")
	}
}

func TestHandlerGroupsBecomeDottedKeys(t *testing.T) {
	w := &lineSink{}
	h := newTestHandler(w, nil).WithAttrs([]slog.Attr{slog.String("top", "t")}).WithGroup("http").WithAttrs([]slog.Attr{slog.String("route", "/o/:id")})
	handle(t, h, context.Background(), slog.LevelInfo, "m",
		slog.Group("response", slog.Int("status_code", 200)), slog.Group("empty"), slog.Group("", slog.Int("inl", 1)))
	keys, obj := keysOf(t, w.lines()[0])
	if got := strings.Join(keys[5:], ","); got != "top,http.route,http.response.status_code,http.inl" {
		t.Fatalf("keys %s", got)
	}
	if obj["http.route"] != "/o/:id" {
		t.Fatal(obj)
	}
}

func TestHandlerErrorGroupFieldNames(t *testing.T) {
	w := &lineSink{}
	handle(t, newTestHandler(w, nil), context.Background(), slog.LevelError, "failed",
		slog.Any("error", errors.New("boom")), slog.Group("error", slog.String("code", "INTERNAL"), slog.String("reason", "X")))
	keys, obj := keysOf(t, w.lines()[0])
	if got := strings.Join(keys[5:], ","); got != "error,error.code,error.reason" || obj["error"] != "boom" {
		t.Fatalf("keys %s obj %v", got, obj)
	}
}

func TestHandlerRedactsNonEnvelopeFields(t *testing.T) {
	w := &lineSink{}
	ctx := WithFields(context.Background(), slog.String("request_id", "token-r1"), slog.String("access_token", "eyJ"))
	handle(t, newTestHandler(w, nil), ctx, slog.LevelInfo, "phone 138 changed",
		slog.String("contactPhone", "138"), slog.Group("user", slog.String("email", "a@x.cn"), slog.String("name", "A")),
		slog.Any("payload", map[string]any{"items": []any{map[string]any{"token": "t", "n": 1}}}),
		slog.String("note", "call 138"))
	_, obj := keysOf(t, w.lines()[0])
	want := map[string]any{
		"msg": "phone 138 changed", "request_id": "token-r1", "access_token": RedactedMark,
		"contactPhone": RedactedMark, "user.email": RedactedMark, "user.name": "A", "note": "call 138",
	}
	for k, v := range want {
		if obj[k] != v {
			t.Errorf("%s = %v, want %v", k, obj[k], v)
		}
	}
	items := obj["payload"].(map[string]any)["items"].([]any)[0].(map[string]any)
	if items["token"] != RedactedMark || items["n"] != json.Number("1") {
		t.Errorf("nested: %v", items)
	}
}

func TestHandlerReservedKeysCannotBeOverridden(t *testing.T) {
	w := &lineSink{}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	ctx := trace.ContextWithSpanContext(WithFields(context.Background(), slog.String("component_id", "evil")), sc)
	handle(t, newTestHandler(w, nil), ctx, slog.LevelInfo, "m",
		slog.String("msg", "x"), slog.String("level", "x"), slog.String("time", "x"), slog.String("component_version", "x"),
		slog.String("trace_id", "x"), slog.String("span_id", "x"))
	keys, obj := keysOf(t, w.lines()[0])
	if len(keys) != 7 || obj["msg"] != "m" || obj["component_id"] != "erp/sales" || obj["trace_id"] != "01000000000000000000000000000000" {
		t.Fatalf("keys %v obj %v", keys, obj)
	}
}

func TestHandlerLevelFilteringIsDynamic(t *testing.T) {
	w := &lineSink{}
	var lv slog.LevelVar
	lv.Set(slog.LevelWarn)
	h := newTestHandler(w, &lv)
	if h.Enabled(context.Background(), slog.LevelInfo) || !h.Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("warn threshold")
	}
	lv.Set(slog.LevelDebug)
	if !h.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("debug after change")
	}
	if newTestHandler(w, nil).Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("default level is info")
	}
}

func TestHandlerValueKinds(t *testing.T) {
	w := &lineSink{}
	handle(t, newTestHandler(w, nil), context.Background(), slog.LevelDebug-4, "m",
		slog.Bool("b", true), slog.Float64("f", 1.5), slog.Uint64("u", 18446744073709551615),
		slog.Int64("i", -9007199254740993), slog.Duration("d", 1500*time.Millisecond),
		slog.Time("t", fixedTime), slog.Any("nan", nanValue()), slog.Any("nil", nil),
		slog.Any("bytes", []byte("hi")), slog.Any("ch", make(chan int)), slog.String("html", "<a&b>"))
	line := w.lines()[0]
	for _, frag := range []string{`"level":"debug"`, `"b":true`, `"f":1.5`, `"u":18446744073709551615`,
		`"i":-9007199254740993`, `"d":1500000000`, `"t":"2026-10-02T00:00:00.12Z"`, `"nan":"NaN"`, `"nil":null`,
		`"bytes":"aGk="`, `"html":"<a&b>"`} {
		if !strings.Contains(line, frag) {
			t.Errorf("missing %s in %s", frag, line)
		}
	}
	if !json.Valid([]byte(line)) {
		t.Fatalf("invalid JSON %s", line)
	}
}

func nanValue() float64 { z := 0.0; return z / z }

type valuer struct{}

func (valuer) LogValue() slog.Value {
	return slog.GroupValue(slog.String("phone", "1"), slog.Int("n", 2))
}

func TestHandlerResolvesLogValuer(t *testing.T) {
	w := &lineSink{}
	handle(t, newTestHandler(w, nil), context.Background(), slog.LevelInfo, "m", slog.Any("v", valuer{}))
	_, obj := keysOf(t, w.lines()[0])
	if obj["v.phone"] != RedactedMark || obj["v.n"] != json.Number("2") {
		t.Fatal(obj)
	}
}

func TestNewLoggerConcurrentLinesStayWhole(t *testing.T) {
	var buf safeBuffer
	log := New(Options{ComponentID: "a/b", ComponentVersion: "1", Writer: &buf})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				log.Info("tick", "g", g, "pad", strings.Repeat("x", 100))
			}
		}(g)
	}
	wg.Wait()
	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != 1600 {
		t.Fatalf("%d lines", len(lines))
	}
	for _, l := range lines {
		if !json.Valid([]byte(l)) {
			t.Fatalf("broken line %q", l)
		}
	}
}

type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestHandlersSharingAWriterSerialise(t *testing.T) {
	// Two members' handlers on one stdout: each Write is still one whole line.
	w := &lineSink{}
	a, b := newTestHandler(w, nil), NewHandler(Options{ComponentID: "x/y", ComponentVersion: "1", Writer: w})
	handle(t, a, context.Background(), slog.LevelInfo, "a")
	handle(t, b, context.Background(), slog.LevelInfo, "b")
	for _, l := range w.lines() {
		if strings.Count(l, "\n") != 1 || !json.Valid([]byte(l)) {
			t.Fatalf("line %q", l)
		}
	}
}

func TestHandlerWithAttrsInlineGroupUnderGroup(t *testing.T) {
	w := &lineSink{}
	h := newTestHandler(w, nil).WithGroup("g").WithAttrs([]slog.Attr{slog.Group("", slog.Int("x", 1)), slog.Group("s", slog.Int("y", 2))})
	handle(t, h, context.Background(), slog.LevelInfo, "m")
	keys, _ := keysOf(t, w.lines()[0])
	if got := strings.Join(keys[5:], ","); got != "g.x,g.s.y" {
		t.Fatalf("keys %s", got)
	}
}
