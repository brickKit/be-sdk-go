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

// LevelForCode returns the level at which an error with this canonical gRPC code name is logged (P4.6):
// INTERNAL, UNKNOWN, DATA_LOSS → error; UNAVAILABLE, DEADLINE_EXCEEDED → warn; OK and CANCELLED (also a
// cancel during shutdown) → not logged (false); every other code, a caller error → info.
func LevelForCode(code string) (slog.Level, bool) {
	switch code {
	case "OK", "CANCELLED":
		return 0, false
	case "INTERNAL", "UNKNOWN", "DATA_LOSS":
		return slog.LevelError, true
	case "UNAVAILABLE", "DEADLINE_EXCEEDED":
		return slog.LevelWarn, true
	}
	return slog.LevelInfo, true
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
