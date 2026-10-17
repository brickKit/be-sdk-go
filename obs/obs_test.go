package obs

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"regexp"
	"testing"
	"time"
)

func TestLogLinesCarryUTCTimeAndTheMember(t *testing.T) { // P10.1, P10.2
	saved := time.Local
	time.Local = time.FixedZone("CEST", 2*3600) // the host's zone must not show
	defer func() { time.Local = saved }()
	var buf bytes.Buffer
	NewLogger(Member{ID: "demo/a", Version: "3.0.0"}, slog.LevelInfo, &buf).Info("hello", "k", "v")
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$`).MatchString(line["time"].(string)) {
		t.Errorf("time %q is not RFC 3339 UTC to the millisecond", line["time"])
	}
	for k, want := range map[string]string{"level": "INFO", "msg": "hello", "component": "demo/a", "version": "3.0.0"} {
		if line[k] != want {
			t.Errorf("%s = %v, want %s", k, line[k], want)
		}
	}
}
