// Package schedule parses and evaluates the two schedule forms of be-protocol P14.6, with the standard
// library only:
//
//   - a five-field cron expression "minute hour day-of-month month day-of-week", evaluated in an IANA
//     zone (BUSINESS_TIMEZONE unless the job declares another);
//   - "@every <Go duration>", at least 1 s, whose slots are the multiples of the duration since the
//     Unix epoch, so every replica computes the same slots whatever its zone.
//
// Nothing else is accepted: no seconds field, no @daily / @hourly / @reboot, no L, W, # or ?.
//
// Cron syntax per field: a comma-separated list of items; an item is "*", "*/step", "a", "a-b" or
// "a-b/step" (a <= b, step >= 1; "a/step" is rejected). Month and day-of-week also take three-letter
// English names (JAN–DEC, SUN–SAT, any case) wherever a number may stand, except in a step. Day of
// week is 0–7, 0 and 7 both Sunday. Day rule (Vixie cron): when the day-of-month or the day-of-week
// field begins with "*", a day must match both fields; when both are restricted, a day matching
// either runs. An expression whose month and day-of-month can never coincide ("0 0 30 2 *") is
// rejected.
//
// Daylight-saving time (the deterministic rule every SDK applies): a cron slot is a wall-clock time in
// the zone. A wall time that does not exist (inside a spring-forward gap) runs at the first instant
// after the gap, so 02:30 Europe/Berlin on the spring-forward day runs at 03:00 CEST, and several gap
// slots collapse into that one instant. A wall time that occurs twice (fall-back overlap) runs once,
// at its first occurrence.
package schedule

import (
	"errors"
	"fmt"
	"strings"
	"time"
	_ "time/tzdata" // zones load in any image, including one without a zone database
)

// MinEvery is the shortest @every duration (P14.6).
const MinEvery = time.Second

// Schedule is a parsed schedule (P14.6). Slots are instants; both methods accept a time in any zone.
type Schedule interface {
	// Prev is the latest slot at or before t.
	Prev(t time.Time) time.Time
	// Next is the earliest slot strictly after t.
	Next(t time.Time) time.Time
	// String is the expression as parsed.
	String() string
}

// Parse parses a P14.6 schedule. loc is the zone a cron expression is evaluated in; it is required
// for a cron expression and ignored by @every.
func Parse(expr string, loc *time.Location) (Schedule, error) {
	if rest, ok := strings.CutPrefix(expr, "@every "); ok {
		return parseEvery(expr, rest)
	}
	if strings.HasPrefix(expr, "@") {
		return nil, fmt.Errorf("schedule %q: only a five-field cron expression or @every <duration> is accepted", expr)
	}
	if loc == nil {
		return nil, errors.New("schedule: a cron expression needs a time zone")
	}
	c, err := parseCron(expr)
	if err != nil {
		return nil, err
	}
	c.loc = loc
	return c, nil
}

// every is "@every d": slots at the multiples of d since the Unix epoch.
type every struct {
	expr string
	d    int64 // nanoseconds
}

func parseEvery(expr, rest string) (Schedule, error) {
	d, err := time.ParseDuration(rest)
	if err != nil {
		return nil, fmt.Errorf("schedule %q: %v", expr, err)
	}
	if d < MinEvery {
		return nil, fmt.Errorf("schedule %q: @every needs at least 1s", expr)
	}
	return every{expr: expr, d: int64(d)}, nil
}

func (e every) Prev(t time.Time) time.Time {
	n := t.UnixNano()
	slot := n - mod(n, e.d)
	return time.Unix(0, slot).UTC()
}

func (e every) Next(t time.Time) time.Time {
	return time.Unix(0, e.Prev(t).UnixNano()+e.d).UTC()
}

func (e every) String() string { return e.expr }

// mod is the non-negative remainder, so instants before the epoch floor correctly.
func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
