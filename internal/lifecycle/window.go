package lifecycle

import (
	"fmt"
	"time"
)

// OutboxTable is the platform's partitioned outbox (ddl/02-outbox.sql): weekly, kept ahead like a
// declared table with ahead 2, dropped 14 days after every row is PUBLISHED (P12.15).
const OutboxTable = "besdk_outbox"

// OutboxAhead is the outbox window's ahead (P16.6).
const OutboxAhead = 2

// Unit is one RANGE partition [From, To) of Table, named Name.
type Unit struct {
	Table, Name string
	From, To    time.Time
}

// unitStart is the start of the grain unit containing t, in UTC: weeks from Monday 00:00, months from
// the 1st, years from 1 January (foundations 09 "Partition windows").
func unitStart(g Grain, t time.Time) time.Time {
	t = t.UTC()
	switch g {
	case Week:
		day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		return day.AddDate(0, 0, -((int(day.Weekday()) + 6) % 7))
	case Month:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	}
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
}

func nextStart(g Grain, from time.Time) time.Time {
	switch g {
	case Week:
		return from.AddDate(0, 0, 7)
	case Month:
		return from.AddDate(0, 1, 0)
	}
	return from.AddDate(1, 0, 0)
}

// RangeWindow is the partition window of a range-partitioned table (P16.6, G1): the unit containing
// now and the ahead units after it (a negative ahead counts as 0). A partition is named from its lower
// bound (P16.10): <table>_<ISO week-year>w<WW> for week, <table>_<YYYY>m<MM> for month, <table>_<YYYY>
// for year; the name is the unit key, and the engine still recognises partitions by their bounds.
func RangeWindow(table string, g Grain, now time.Time, ahead int) []Unit {
	from := unitStart(g, now)
	out := make([]Unit, 0, max(ahead, 0)+1)
	for i := 0; i <= max(ahead, 0); i++ {
		to := nextStart(g, from)
		out = append(out, Unit{Table: table, Name: rangeName(table, g, from), From: from, To: to})
		from = to
	}
	return out
}

func rangeName(table string, g Grain, from time.Time) string {
	switch g {
	case Week:
		year, week := from.ISOWeek()
		return fmt.Sprintf("%s_%04dw%02d", table, year, week)
	case Month:
		return fmt.Sprintf("%s_%04dm%02d", table, from.Year(), int(from.Month()))
	}
	return fmt.Sprintf("%s_%04d", table, from.Year())
}

// OutboxWindow is the outbox's weekly window, besdk_outbox_<ISO week-year>w<WW> (P16.6, P16.10).
func OutboxWindow(now time.Time, ahead int) []Unit { return RangeWindow(OutboxTable, Week, now, ahead) }
