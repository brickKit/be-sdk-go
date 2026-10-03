package logx

import (
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/vectors"
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

// The P4.6 table; errors/levels.json is owned by the errors lane, run here too so both tables agree.
func TestLevelForCodeVectors(t *testing.T) {
	vectors.Run(t, "errors", "levels", map[string]func(*testing.T, vectors.Case){
		"log_level": func(t *testing.T, c vectors.Case) {
			var in struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(c.Input, &in); err != nil {
				t.Fatal(err)
			}
			lvl, logged := LevelForCode(in.Code)
			name := "none"
			if logged {
				name = levelName(lvl)
			}
			vectors.RequireJSON(t, c, map[string]string{"level": name})
		},
	})
}

func TestLevelForCodeUnknownNameIsCallerError(t *testing.T) {
	lvl, logged := LevelForCode("SOMETHING_NEW")
	if !logged || lvl != slog.LevelInfo {
		t.Fatalf("got %v %v", lvl, logged)
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
