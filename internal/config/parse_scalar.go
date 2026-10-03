package config

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// maxSafeInt is 2^53-1: every language holds integers in this range exactly (vectors config, integer).
const maxSafeInt = 1<<53 - 1

var intRe = regexp.MustCompile(`^-?[0-9]+$`)

// ParseInt parses an int value strictly (P2.3): ^-?[0-9]+$ within ±(2^53-1); no "+", spaces,
// separators, hex, exponent or non-ASCII digits. Leading zeros are decimal.
func ParseInt(key, raw string) (int64, error) {
	if !intRe.MatchString(raw) {
		return 0, newErr(ReasonInvalid, key, "not a decimal integer")
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n > maxSafeInt || n < -maxSafeInt {
		return 0, newErr(ReasonInvalid, key, "integer outside ±(2^53-1)")
	}
	return n, nil
}

// ParseBool parses a bool value strictly (P2.3): exactly true, false, 1 or 0.
func ParseBool(key, raw string) (bool, error) {
	switch raw {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	}
	return false, newErr(ReasonInvalid, key, "not true, false, 1 or 0")
}

// ParseDuration parses a duration value (P2.3): Go time.ParseDuration syntax, not negative.
func ParseDuration(key, raw string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, newErr(ReasonInvalid, key, "not a Go duration")
	}
	if d < 0 {
		return 0, newErr(ReasonInvalid, key, "negative duration")
	}
	return d, nil
}

// ParseDurations parses a durations value (P2.3, EVENTS_BACKOFF): comma-separated durations, no
// spaces, no empty element, each positive.
func ParseDurations(key, raw string) ([]time.Duration, error) {
	parts := strings.Split(raw, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, p := range parts {
		if p == "" || strings.TrimSpace(p) != p {
			return nil, newErr(ReasonInvalid, key, "empty element or spaces in duration list")
		}
		d, err := ParseDuration(key, p)
		if err != nil {
			return nil, err
		}
		if d == 0 {
			return nil, newErr(ReasonInvalid, key, "duration list element is zero")
		}
		out = append(out, d)
	}
	return out, nil
}

// ParseEnum checks an enum value (P2.3): exact, case-sensitive member of allowed.
func ParseEnum(key, raw string, allowed []string) (string, error) {
	if !slices.Contains(allowed, raw) {
		return "", newErr(ReasonInvalid, key, "not one of "+strings.Join(allowed, ", "))
	}
	return raw, nil
}
