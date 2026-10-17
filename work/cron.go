package work

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Schedule is a parsed 5-field cron expression (P7.15).
type Schedule struct {
	minutes, hours, doms, months, dows map[int]bool
	domRestricted, dowRestricted       bool
}

// Slot is one time slot: its identity (local date and time) and the instant it runs.
type Slot struct {
	ID string    // YYYY-MM-DDTHH:MM, local
	At time.Time // UTC
}

// ParseCron parses minute, hour, day of month, month and day of week; "*", lists, ranges and steps; 0 and 7 are
// both Sunday.
func ParseCron(expr string) (Schedule, error) {
	f := strings.Fields(expr)
	if len(f) != 5 {
		return Schedule{}, fmt.Errorf("cron %q: five fields", expr)
	}
	var s Schedule
	var err error
	parse := func(spec string, lo, hi int) map[int]bool {
		if err != nil {
			return nil
		}
		var m map[int]bool
		m, err = field(spec, lo, hi)
		return m
	}
	s.minutes, s.hours, s.doms, s.months = parse(f[0], 0, 59), parse(f[1], 0, 23), parse(f[2], 1, 31), parse(f[3], 1, 12)
	dows := parse(f[4], 0, 7)
	if err != nil {
		return Schedule{}, fmt.Errorf("cron %q: %w", expr, err)
	}
	s.dows = map[int]bool{}
	for d := range dows {
		s.dows[d%7] = true
	}
	s.domRestricted, s.dowRestricted = f[2] != "*", f[4] != "*"
	return s, nil
}

func field(spec string, lo, hi int) (map[int]bool, error) {
	out := map[int]bool{}
	for _, part := range strings.Split(spec, ",") {
		step := 1
		if i := strings.IndexByte(part, '/'); i >= 0 {
			n, err := strconv.Atoi(part[i+1:])
			if err != nil || n < 1 {
				return nil, fmt.Errorf("step %q", part)
			}
			step, part = n, part[:i]
		}
		a, b := lo, hi
		switch {
		case part == "*":
		case strings.Contains(part, "-"):
			x, y, _ := strings.Cut(part, "-")
			var e1, e2 error
			a, e1 = strconv.Atoi(x)
			b, e2 = strconv.Atoi(y)
			if e1 != nil || e2 != nil {
				return nil, fmt.Errorf("range %q", part)
			}
		default:
			n, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("value %q", part)
			}
			a, b = n, n
			if step != 1 {
				b = hi
			}
		}
		if a < lo || b > hi || a > b {
			return nil, fmt.Errorf("%q outside %d-%d", part, lo, hi)
		}
		for v := a; v <= b; v += step {
			out[v] = true
		}
	}
	return out, nil
}

func keys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// Slots returns the slots whose run instant is in (after, until], in order.
func (s Schedule) Slots(loc *time.Location, after, until time.Time) []Slot {
	var out []Slot
	day := after.In(loc).AddDate(0, 0, -1)
	last := until.In(loc).AddDate(0, 0, 1)
	d := time.Date(day.Year(), day.Month(), day.Day(), 12, 0, 0, 0, loc)
	end := time.Date(last.Year(), last.Month(), last.Day(), 12, 0, 0, 0, loc)
	hours, minutes := keys(s.hours), keys(s.minutes)
	for ; !d.After(end); d = d.AddDate(0, 0, 1) {
		dow := int(d.Weekday())
		dayOK := s.doms[d.Day()] && s.dows[dow]
		if s.domRestricted && s.dowRestricted {
			dayOK = s.doms[d.Day()] || s.dows[dow]
		}
		if !s.months[int(d.Month())] || !dayOK {
			continue
		}
		for _, h := range hours {
			for _, m := range minutes {
				at := wallInstant(d.Year(), d.Month(), d.Day(), h, m, loc)
				if at.After(after) && !at.After(until) {
					out = append(out, Slot{ID: fmt.Sprintf("%04d-%02d-%02dT%02d:%02d", d.Year(), d.Month(), d.Day(), h, m), At: at.UTC()})
				}
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

type wall struct {
	y        int
	mo       time.Month
	d, h, mi int
}

func wallOf(t time.Time, loc *time.Location) wall {
	l := t.In(loc)
	return wall{l.Year(), l.Month(), l.Day(), l.Hour(), l.Minute()}
}

func (w wall) less(o wall) bool {
	a := []int{w.y, int(w.mo), w.d, w.h, w.mi}
	b := []int{o.y, int(o.mo), o.d, o.h, o.mi}
	for i := range a {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// wallInstant is the run instant of a local wall time: its first occurrence, or in a gap the first instant after the
// jump.
func wallInstant(y int, mo time.Month, d, h, mi int, loc *time.Location) time.Time {
	want := wall{y, mo, d, h, mi}
	naive := time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
	var first time.Time
	found := false
	for off := -15 * 3600; off <= 15*3600; off += 900 { // zone offsets are whole quarter hours
		t := naive.Add(-time.Duration(off) * time.Second)
		if wallOf(t, loc) == want && t.Second() == 0 && (!found || t.Before(first)) {
			first, found = t, true
		}
	}
	if found {
		return first
	}
	lo, hi := naive.Add(-15*time.Hour), naive.Add(15*time.Hour)
	for hi.Sub(lo) > time.Second {
		mid := lo.Add(hi.Sub(lo) / 2).Truncate(time.Second)
		if wallOf(mid, loc).less(want) {
			lo = mid
		} else {
			hi = mid
		}
	}
	return hi
}

// Backfill is the policy for slots missed while stopped (P7.16).
type Backfill string

// Policies.
const (
	Latest Backfill = "latest"
	All    Backfill = "all"
	None   Backfill = "none"
)

// DefaultMaxBacklog bounds the "all" policy.
const DefaultMaxBacklog = 24

// Plan splits the slots due in (watermark, now] by policy: the ones to materialize as pending, the ones to
// materialize as paused (backlog_exceeded), and the new watermark.
func Plan(s Schedule, loc *time.Location, policy Backfill, maxBacklog int, watermark, now time.Time) (pending, paused []Slot, next time.Time) {
	due := s.Slots(loc, watermark, now)
	if len(due) == 0 {
		return nil, nil, watermark
	}
	next = due[len(due)-1].At
	switch policy {
	case All:
		if len(due) > maxBacklog {
			return due[:maxBacklog], due[maxBacklog:], next
		}
		return due, nil, next
	case None:
		for _, sl := range due {
			if now.Sub(sl.At) <= time.Minute {
				pending = append(pending, sl)
			}
		}
		return pending, nil, next
	}
	return due[len(due)-1:], nil, next
}
