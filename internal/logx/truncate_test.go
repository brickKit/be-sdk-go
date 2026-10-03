package logx

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"unicode/utf8"

	"pgregory.net/rapid"
)

const marker = "…[TRUNCATED]"

func smallHandler(w *lineSink, maxLine int) slog.Handler {
	return NewHandler(Options{ComponentID: "erp/sales", ComponentVersion: "2.0.0", Writer: w, MaxLine: maxLine})
}

func TestStringCostMatchesEncoder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := rapid.OneOf(rapid.String(), rapid.StringOf(rapid.Map(rapid.Byte(), func(b byte) rune { return rune(b) })),
			rapid.Custom(func(t *rapid.T) string { return string(rapid.SliceOf(rapid.Byte()).Draw(t, "raw")) })).Draw(t, "s")
		var out bytes.Buffer
		newLineEncoder().value(&out, s)
		if got, want := stringCost(s), out.Len()-2; got != want {
			t.Fatalf("stringCost(%q) = %d, encoder wrote %d (%s)", s, got, want, out.String())
		}
	})
	for _, s := range []string{"  ", "\b\f\x01\x7f", "<>&", "\xff\xfe", "中文"} {
		var out bytes.Buffer
		newLineEncoder().value(&out, s)
		if stringCost(s) != out.Len()-2 {
			t.Errorf("%q: %d vs %d", s, stringCost(s), out.Len()-2)
		}
	}
}

func TestShortLineIsNotTruncated(t *testing.T) {
	w := &lineSink{}
	handle(t, smallHandler(w, 4096), context.Background(), slog.LevelInfo, "m", slog.String("a", strings.Repeat("x", 100)))
	if strings.Contains(w.lines()[0], "truncated") {
		t.Fatal(w.lines()[0])
	}
}

func TestLineOfExactlyMaxIsKept(t *testing.T) {
	w := &lineSink{}
	big := NewHandler(Options{ComponentID: "erp/sales", ComponentVersion: "2.0.0", Writer: w, MaxLine: 1 << 20})
	handle(t, big, context.Background(), slog.LevelInfo, "m", slog.String("a", strings.Repeat("x", 300)))
	n := len(w.lines()[0])
	w2 := &lineSink{}
	handle(t, smallHandler(w2, n), context.Background(), slog.LevelInfo, "m", slog.String("a", strings.Repeat("x", 300)))
	if w2.lines()[0] != w.lines()[0] {
		t.Fatalf("a line of exactly MaxLine bytes must stay as is: %s", w2.lines()[0])
	}
	w3 := &lineSink{}
	handle(t, smallHandler(w3, n-1), context.Background(), slog.LevelInfo, "m", slog.String("a", strings.Repeat("x", 300)))
	if l := w3.lines()[0]; len(l) > n-1 || !strings.Contains(l, `"truncated":true`) {
		t.Fatalf("one byte over must be truncated: %d %s", len(l), l)
	}
}

func TestTruncateCutsLongestStringFirst(t *testing.T) {
	w := &lineSink{}
	short := strings.Repeat("s", 200)
	handle(t, smallHandler(w, 2048), context.Background(), slog.LevelInfo, "m",
		slog.String("short", short), slog.String("long", strings.Repeat("L", 5000)), slog.Int("n", 7))
	line := w.lines()[0]
	keys, obj := keysOf(t, line)
	if len(line) > 2048 {
		t.Fatalf("len %d", len(line))
	}
	if obj["short"] != short || obj["n"] != json.Number("7") || obj["truncated"] != true {
		t.Fatalf("obj %v", obj)
	}
	long := obj["long"].(string)
	if !strings.HasSuffix(long, marker) || !strings.HasPrefix(long, "LLLL") {
		t.Fatalf("long %q", long)
	}
	if keys[len(keys)-1] != "truncated" {
		t.Fatalf("keys %v", keys)
	}
	if len(line) < 2048-8 {
		t.Fatalf("cut more than needed: %d", len(line))
	}
}

func TestTruncateSeveralStrings(t *testing.T) {
	w := &lineSink{}
	handle(t, smallHandler(w, 600), context.Background(), slog.LevelInfo, "m",
		slog.String("a", strings.Repeat("a", 900)), slog.String("b", strings.Repeat("b", 800)), slog.String("c", "keep"))
	line := w.lines()[0]
	_, obj := keysOf(t, line)
	if len(line) > 600 || obj["c"] != "keep" || obj["a"] != marker || !strings.HasSuffix(obj["b"].(string), marker) {
		t.Fatalf("%d %v", len(line), obj)
	}
}

func TestTruncateAtCharacterBoundary(t *testing.T) {
	w := &lineSink{}
	handle(t, smallHandler(w, 400), context.Background(), slog.LevelInfo, "m", slog.String("zh", strings.Repeat("中文\"\n", 400)))
	line := w.lines()[0]
	_, obj := keysOf(t, line)
	s := obj["zh"].(string)
	if !utf8.ValidString(s) || !strings.HasSuffix(s, marker) || len(line) > 400 {
		t.Fatalf("%d %q", len(line), s)
	}
}

func TestTruncateNestedStrings(t *testing.T) {
	w := &lineSink{}
	handle(t, smallHandler(w, 500), context.Background(), slog.LevelInfo, "m",
		slog.Any("order", map[string]any{"lines": []any{map[string]any{"note": strings.Repeat("n", 3000)}}, "id": 9}))
	line := w.lines()[0]
	_, obj := keysOf(t, line)
	order := obj["order"].(map[string]any)
	note := order["lines"].([]any)[0].(map[string]any)["note"].(string)
	if len(line) > 500 || !strings.HasSuffix(note, marker) || order["id"] != json.Number("9") {
		t.Fatalf("%d %s", len(line), line)
	}
}

func TestTruncateBulkWithoutLongStrings(t *testing.T) {
	// Thousands of numbers: no string to cut; the line must still fit and stay valid JSON.
	nums := make([]int, 2000)
	w := &lineSink{}
	handle(t, smallHandler(w, 700), context.Background(), slog.LevelInfo, "m", slog.Any("nums", nums), slog.String("k", "v"))
	line := w.lines()[0]
	_, obj := keysOf(t, line)
	if len(line) > 700 || obj["truncated"] != true || obj["msg"] != "m" {
		t.Fatalf("%d %s", len(line), line)
	}
}

func TestEnvelopeIsNeverCut(t *testing.T) {
	w := &lineSink{}
	msg := strings.Repeat("M", 3000)
	ctx := WithFields(context.Background(), slog.String("request_id", strings.Repeat("r", 100)))
	handle(t, smallHandler(w, 2048), ctx, slog.LevelInfo, msg, slog.String("a", strings.Repeat("x", 500)))
	_, obj := keysOf(t, w.lines()[0])
	if obj["msg"] != msg || obj["request_id"] != strings.Repeat("r", 100) || obj["truncated"] != true {
		t.Fatalf("envelope changed: %v", obj)
	}
}

// Property (P18.2): every line is one valid JSON object of at most MaxLine bytes whenever the envelope
// alone fits; envelope fields are unchanged; an uncut string keeps its value.
func TestTruncationProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		maxLine := rapid.IntRange(150, 3000).Draw(t, "max")
		msg := rapid.StringN(0, 40, -1).Draw(t, "msg")
		reqID := rapid.StringN(0, 20, -1).Draw(t, "req")
		n := rapid.IntRange(0, 12).Draw(t, "n")
		attrs := make([]slog.Attr, n)
		for i := range attrs {
			attrs[i] = drawAttr(t, i)
		}
		ctx := WithFields(context.Background(), slog.String("request_id", reqID))
		w, ref := &lineSink{}, &lineSink{}
		logOnce(t, smallHandler(w, maxLine), ctx, msg, attrs)
		logOnce(t, smallHandler(ref, 1<<30), ctx, msg, attrs)
		envOnly := &lineSink{}
		logOnce(t, smallHandler(envOnly, 1<<30), ctx, msg, nil)
		line, full := w.lines()[0], ref.lines()[0]
		var got, want map[string]any
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("invalid JSON %q: %v", line, err)
		}
		_ = json.Unmarshal([]byte(full), &want)
		for _, k := range []string{"time", "level", "msg", "component_id", "component_version", "request_id"} {
			if got[k] != want[k] {
				t.Fatalf("envelope %s changed: %v vs %v", k, got[k], want[k])
			}
		}
		fits := len(envOnly.lines()[0])+len(`,"truncated":true`) <= maxLine
		if fits && len(line) > maxLine {
			t.Fatalf("line of %d bytes > %d: %s", len(line), maxLine, line)
		}
		if len(full) <= maxLine && line != full {
			t.Fatalf("a fitting line was changed:\n%s\n%s", line, full)
		}
		for k, v := range got {
			if s, ok := v.(string); ok && !strings.HasSuffix(s, marker) && k != "truncated" {
				if ws, ok := want[k].(string); ok && ws != s {
					t.Fatalf("field %s changed without a marker: %q vs %q", k, s, ws)
				}
			}
		}
	})
}

func logOnce(t *rapid.T, h slog.Handler, ctx context.Context, msg string, attrs []slog.Attr) {
	r := slog.NewRecord(fixedTime, slog.LevelInfo, msg, 0)
	r.AddAttrs(attrs...)
	if err := h.Handle(ctx, r); err != nil {
		t.Fatal(err)
	}
}

func drawAttr(t *rapid.T, i int) slog.Attr {
	key := rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "key") + string(rune('0'+i%10))
	switch rapid.IntRange(0, 4).Draw(t, "kind") {
	case 0:
		return slog.String(key, rapid.StringN(0, 2000, -1).Draw(t, "s"))
	case 1:
		return slog.Int(key, rapid.Int().Draw(t, "i"))
	case 2:
		return slog.Any(key, rapid.SliceOfN(rapid.StringN(0, 300, -1), 0, 20).Draw(t, "arr"))
	case 3:
		return slog.Any(key, rapid.MapOfN(rapid.StringMatching(`[a-z]{1,4}`), rapid.StringN(0, 500, -1), 0, 8).Draw(t, "obj"))
	}
	return slog.Any(key, rapid.SliceOfN(rapid.Int(), 0, 200).Draw(t, "nums"))
}
