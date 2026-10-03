package lifecycle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRangeWindowMonth(t *testing.T) {
	// 2026-10-31 23:30 at UTC+8 is 15:30 UTC on the 31st: still October
	now := time.Date(2026, 10, 31, 23, 30, 0, 0, time.FixedZone("CST", 8*3600))
	require.Equal(t, []Unit{
		{Table: "widgets", Name: "widgets_2026m10", From: utc("2026-10-01T00:00:00Z"), To: utc("2026-11-01T00:00:00Z")},
		{Table: "widgets", Name: "widgets_2026m11", From: utc("2026-11-01T00:00:00Z"), To: utc("2026-12-01T00:00:00Z")},
		{Table: "widgets", Name: "widgets_2026m12", From: utc("2026-12-01T00:00:00Z"), To: utc("2027-01-01T00:00:00Z")},
		{Table: "widgets", Name: "widgets_2027m01", From: utc("2027-01-01T00:00:00Z"), To: utc("2027-02-01T00:00:00Z")},
	}, RangeWindow("widgets", Month, now, 3))
}

func TestRangeWindowWeekStartsMondayUTC(t *testing.T) {
	w := RangeWindow("jobs", Week, utc("2026-10-03T10:00:00Z"), 1)
	require.Equal(t, []Unit{
		{Table: "jobs", Name: "jobs_2026w40", From: utc("2026-09-28T00:00:00Z"), To: utc("2026-10-05T00:00:00Z")},
		{Table: "jobs", Name: "jobs_2026w41", From: utc("2026-10-05T00:00:00Z"), To: utc("2026-10-12T00:00:00Z")},
	}, w)
	require.Equal(t, "jobs_2026w41", RangeWindow("jobs", Week, utc("2026-10-05T00:00:00Z"), 0)[0].Name, "Monday 00:00 opens a new week")
}

func TestRangeWindowYear(t *testing.T) {
	w := RangeWindow("audit", Year, utc("2026-12-31T23:59:59Z"), 1)
	require.Equal(t, "audit_2026", w[0].Name)
	require.Equal(t, utc("2027-01-01T00:00:00Z"), w[0].To)
	require.Equal(t, "audit_2027", w[1].Name)
	require.Len(t, RangeWindow("audit", Year, utc("2026-06-01T00:00:00Z"), -3), 1, "negative ahead is the current unit only")
}

// The outbox keeps the controller's name rule besdk_outbox_<ISO year>w<ISO week> (P16.6).
func TestOutboxWindow(t *testing.T) {
	w := OutboxWindow(utc("2027-01-01T12:00:00Z"), 1)
	require.Equal(t, []Unit{
		{Table: OutboxTable, Name: "besdk_outbox_2026w53", From: utc("2026-12-28T00:00:00Z"), To: utc("2027-01-04T00:00:00Z")},
		{Table: OutboxTable, Name: "besdk_outbox_2027w01", From: utc("2027-01-04T00:00:00Z"), To: utc("2027-01-11T00:00:00Z")},
	}, w)
	require.Equal(t, "besdk_outbox_2025w01", OutboxWindow(utc("2024-12-31T00:00:00Z"), 0)[0].Name)
}
