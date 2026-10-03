package logx

import (
	"fmt"
	"log/slog"
)

// ParseLevel parses a LOG_LEVEL value: exactly one of debug, info, warn, error (P18.2).
func ParseLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("log level %q: want one of debug, info, warn, error", s)
}

// levelName maps a slog level to the four P18.2 level names; levels between two names take the lower.
func levelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "error"
	case l >= slog.LevelWarn:
		return "warn"
	case l >= slog.LevelInfo:
		return "info"
	}
	return "debug"
}
