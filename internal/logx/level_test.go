package logx

import (
	"log/slog"
	"testing"
)

func TestParseLevel(t *testing.T) {
	ok := map[string]slog.Level{"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn, "error": slog.LevelError}
	for s, want := range ok {
		got, err := ParseLevel(s)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	for _, s := range []string{"", "INFO", "warning", "trace", " info", "fatal"} {
		if _, err := ParseLevel(s); err == nil {
			t.Errorf("ParseLevel(%q): want an error", s)
		}
	}
}

func TestLevelName(t *testing.T) {
	cases := map[slog.Level]string{
		slog.LevelDebug - 4: "debug", slog.LevelDebug: "debug", slog.LevelInfo - 1: "debug",
		slog.LevelInfo: "info", slog.LevelInfo + 2: "info", slog.LevelWarn: "warn",
		slog.LevelError - 1: "warn", slog.LevelError: "error", slog.LevelError + 8: "error",
	}
	for l, want := range cases {
		if got := levelName(l); got != want {
			t.Errorf("levelName(%d) = %q, want %q", l, got, want)
		}
	}
}
