package schedule

import "time"

// Wall-clock times are handled as "naive" time.Time values in UTC: calendar arithmetic without any
// zone transitions. instant maps a naive wall time onto the zone (the DST rule of the package doc).

const (
	// searchYears bounds a search for a matching wall time; the rarest satisfiable expression
	// (29 February) recurs within 8 years.
	searchYears = 10
	// maxShift is the largest possible distance between a wall time and wall(t) for instants around
	// t: the span of UTC offsets (−12 h … +14 h).
	maxShift = 26 * time.Hour
)

func (c *cron) String() string { return c.expr }

// Next is the earliest slot strictly after t. instant() is non-decreasing in the wall time and
// instant(wall(t)) <= t, so the search starts at wall(t) and skips the matches that map to <= t.
func (c *cron) Next(t time.Time) time.Time {
	w := c.nextMatch(naive(t.In(c.loc)).Truncate(time.Minute))
	for !w.IsZero() {
		if at := c.instant(w); at.After(t) {
			return at
		}
		w = c.nextMatch(w.Add(time.Minute))
	}
	return time.Time{}
}

// Prev is the latest slot at or before t: the largest matching wall time whose instant is <= t,
// searched downwards from wall(t) + maxShift (in an overlap, later wall times can still map before t).
func (c *cron) Prev(t time.Time) time.Time {
	w := c.prevMatch(naive(t.In(c.loc)).Truncate(time.Minute).Add(maxShift))
	for !w.IsZero() {
		if at := c.instant(w); !at.After(t) {
			return at
		}
		w = c.prevMatch(w.Add(-time.Minute))
	}
	return time.Time{}
}

// nextMatch is the earliest matching wall time >= w (w on a whole minute), zero past the horizon.
func (c *cron) nextMatch(w time.Time) time.Time {
	limit := w.AddDate(searchYears, 0, 0)
	for w.Before(limit) {
		switch {
		case !c.has(c.month, int(w.Month())):
			w = time.Date(w.Year(), w.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		case !c.dayMatches(w):
			w = time.Date(w.Year(), w.Month(), w.Day()+1, 0, 0, 0, 0, time.UTC)
		case !c.has(c.hour, w.Hour()):
			w = time.Date(w.Year(), w.Month(), w.Day(), w.Hour()+1, 0, 0, 0, time.UTC)
		case !c.has(c.minute, w.Minute()):
			w = w.Add(time.Minute)
		default:
			return w
		}
	}
	return time.Time{}
}

// prevMatch is the latest matching wall time <= w (w on a whole minute), zero past the horizon.
func (c *cron) prevMatch(w time.Time) time.Time {
	limit := w.AddDate(-searchYears, 0, 0)
	for w.After(limit) {
		switch {
		case !c.has(c.month, int(w.Month())):
			w = time.Date(w.Year(), w.Month(), 1, 0, 0, 0, 0, time.UTC).Add(-time.Minute)
		case !c.dayMatches(w):
			w = time.Date(w.Year(), w.Month(), w.Day(), 0, 0, 0, 0, time.UTC).Add(-time.Minute)
		case !c.has(c.hour, w.Hour()):
			w = time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), 0, 0, 0, time.UTC).Add(-time.Minute)
		case !c.has(c.minute, w.Minute()):
			w = w.Add(-time.Minute)
		default:
			return w
		}
	}
	return time.Time{}
}

func (c *cron) has(bits uint64, v int) bool { return bits&(1<<v) != 0 }

// dayMatches applies the Vixie day rule: AND when either day field begins with "*", else OR.
func (c *cron) dayMatches(w time.Time) bool {
	dom, dow := c.has(c.dom, w.Day()), c.has(c.dow, int(w.Weekday()))
	if c.domStar || c.dowStar {
		return dom && dow
	}
	return dom || dow
}

// instant maps the wall time w onto the zone: the earliest instant showing w; for a wall time inside
// a gap, the first instant after the gap.
func (c *cron) instant(w time.Time) time.Time {
	x := time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), 0, 0, c.loc)
	if naive(x).Equal(w) {
		// w exists; in an overlap time.Date may have chosen the later occurrence.
		if start, _ := x.ZoneBounds(); !start.IsZero() {
			_, prevOffset := start.Add(-time.Nanosecond).Zone()
			earlier := w.Add(-time.Duration(prevOffset) * time.Second)
			if earlier.Before(start) && naive(earlier.In(c.loc)).Equal(w) {
				return earlier
			}
		}
		return x
	}
	// w is inside a gap: x lies on one side of it; the gap ends at the transition.
	start, end := x.ZoneBounds()
	if naive(x).After(w) {
		return start
	}
	return end
}

// naive is t's wall clock in its own location, as a UTC time.
func naive(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}
