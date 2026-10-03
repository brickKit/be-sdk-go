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
// now and the ahead units after it (a negative ahead counts as 0). A partition is named
// <table>_YYYY_MM_DD after the first day of its range (foundations 09); names are for people only,
// the engine finds partitions by their bounds.
func RangeWindow(table string, g Grain, now time.Time, ahead int) []Unit {
	from := unitStart(g, now)
	out := make([]Unit, 0, max(ahead, 0)+1)
	for i := 0; i <= max(ahead, 0); i++ {
		to := nextStart(g, from)
		out = append(out, Unit{Table: table, Name: rangeName(table, from), From: from, To: to})
		from = to
	}
	return out
}

func rangeName(table string, from time.Time) string {
	return fmt.Sprintf("%s_%04d_%02d_%02d", table, from.Year(), int(from.Month()), from.Day())
}

// OutboxWindow is the outbox's weekly window, named besdk_outbox_<ISO year>w<ISO week, 2 digits>
// (controller ruling, P16.6).
func OutboxWindow(now time.Time, ahead int) []Unit {
	units := RangeWindow(OutboxTable, Week, now, ahead)
	for i := range units {
		year, week := units[i].From.ISOWeek()
		units[i].Name = fmt.Sprintf("%s_%dw%02d", OutboxTable, year, week)
	}
	return units
}
