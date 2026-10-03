package schedule

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cron is a parsed five-field expression; each field is a bit set of the values it matches.
type cron struct {
	expr                          string
	loc                           *time.Location
	minute, hour, dom, month, dow uint64
	domStar, dowStar              bool // the field begins with "*" (Vixie day rule)
}

// field describes one of the five fields.
type field struct {
	name     string
	min, max int
	names    []string // index = value - nameBase
	nameBase int
}

var (
	fMinute = field{name: "minute", min: 0, max: 59}
	fHour   = field{name: "hour", min: 0, max: 23}
	fDom    = field{name: "day-of-month", min: 1, max: 31}
	fMonth  = field{name: "month", min: 1, max: 12, nameBase: 1,
		names: []string{"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"}}
	fDow = field{name: "day-of-week", min: 0, max: 7, nameBase: 0,
		names: []string{"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"}}
)

// daysIn is the most days a month can have (February in a leap year).
var daysIn = [13]int{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func parseCron(expr string) (*cron, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return nil, fmt.Errorf("schedule %q: a cron expression has exactly five fields", expr)
	}
	c := &cron{expr: expr, domStar: strings.HasPrefix(parts[2], "*"), dowStar: strings.HasPrefix(parts[4], "*")}
	var err error
	for i, f := range []struct {
		dst *uint64
		def field
	}{{&c.minute, fMinute}, {&c.hour, fHour}, {&c.dom, fDom}, {&c.month, fMonth}, {&c.dow, fDow}} {
		if *f.dst, err = f.def.parse(parts[i]); err != nil {
			return nil, fmt.Errorf("schedule %q: %v", expr, err)
		}
	}
	if c.dow&(1<<7) != 0 { // 7 is Sunday
		c.dow = c.dow&^(1<<7) | 1
	}
	if !c.daysPossible() {
		return nil, fmt.Errorf("schedule %q: the day of month never occurs in the months given", expr)
	}
	return c, nil
}

// daysPossible: some selected month has some selected day, unless the day-of-week can select days.
func (c *cron) daysPossible() bool {
	if !c.domStar && !c.dowStar {
		return true // OR rule: the weekdays alone select days
	}
	for m := 1; m <= 12; m++ {
		if c.month&(1<<m) == 0 {
			continue
		}
		for d := 1; d <= daysIn[m]; d++ {
			if c.dom&(1<<d) != 0 {
				return true
			}
		}
	}
	return false
}

// parse turns one field into its bit set.
func (f field) parse(s string) (uint64, error) {
	var bits uint64
	for _, item := range strings.Split(s, ",") {
		b, err := f.item(item)
		if err != nil {
			return 0, err
		}
		bits |= b
	}
	return bits, nil
}

// item parses "*", "*/n", "a", "a-b" or "a-b/n".
func (f field) item(s string) (uint64, error) {
	rng, stepText, hasStep := strings.Cut(s, "/")
	lo, hi := f.min, f.max
	switch {
	case rng == "*":
		if f.name == fDow.name {
			hi = 6 // "*" in day-of-week is 0–6; 7 only as an explicit Sunday
		}
	case strings.Contains(rng, "-"):
		a, b, _ := strings.Cut(rng, "-")
		var err error
		if lo, err = f.value(a); err != nil {
			return 0, err
		}
		if hi, err = f.value(b); err != nil {
			return 0, err
		}
		if lo > hi {
			return 0, fmt.Errorf("%s range %q runs backwards", f.name, rng)
		}
	default:
		if hasStep {
			return 0, fmt.Errorf("%s %q: a step needs * or a range", f.name, s)
		}
		v, err := f.value(rng)
		if err != nil {
			return 0, err
		}
		lo, hi = v, v
	}
	step := 1
	if hasStep {
		n, err := strconv.Atoi(stepText)
		if err != nil || n < 1 || n > f.max-f.min+1 || !digits(stepText) {
			return 0, fmt.Errorf("%s step %q is not a number between 1 and %d", f.name, stepText, f.max-f.min+1)
		}
		step = n
	}
	var bits uint64
	for v := lo; v <= hi; v += step {
		bits |= 1 << v
	}
	return bits, nil
}

// value parses one number or name within the field's range.
func (f field) value(s string) (int, error) {
	for i, n := range f.names {
		if strings.EqualFold(s, n) {
			return i + f.nameBase, nil
		}
	}
	if !digits(s) {
		return 0, fmt.Errorf("%s value %q is not a number", f.name, s)
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < f.min || v > f.max {
		return 0, fmt.Errorf("%s value %q is outside %d–%d", f.name, s, f.min, f.max)
	}
	return v, nil
}

// digits reports whether s is a non-empty run of ASCII digits (no sign, no space).
func digits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
