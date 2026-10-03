package pg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestOutboxWindowWeeksFromMondayUTC(t *testing.T) {
	// Saturday 2026-10-03 23:30 at UTC+8 is still Saturday 15:30 UTC: ISO week 40 of 2026
	now := time.Date(2026, 10, 3, 23, 30, 0, 0, time.FixedZone("CST", 8*3600))
	w := outboxWindow(now, 2)
	require.Equal(t, []weekPartition{
		{Name: "besdk_outbox_2026w40", From: utc("2026-09-28T00:00:00Z"), To: utc("2026-10-05T00:00:00Z")},
		{Name: "besdk_outbox_2026w41", From: utc("2026-10-05T00:00:00Z"), To: utc("2026-10-12T00:00:00Z")},
		{Name: "besdk_outbox_2026w42", From: utc("2026-10-12T00:00:00Z"), To: utc("2026-10-19T00:00:00Z")},
	}, w)
}

func TestOutboxWindowMondayMidnightBelongsToNewWeek(t *testing.T) {
	w := outboxWindow(utc("2026-10-05T00:00:00Z"), 0)
	require.Equal(t, []weekPartition{{Name: "besdk_outbox_2026w41", From: utc("2026-10-05T00:00:00Z"), To: utc("2026-10-12T00:00:00Z")}}, w)
}

func TestOutboxWindowUsesISOYearAcrossNewYear(t *testing.T) {
	// 2027-01-01 is a Friday: ISO week 53 of 2026, Monday 2026-12-28
	w := outboxWindow(utc("2027-01-01T12:00:00Z"), 1)
	require.Equal(t, "besdk_outbox_2026w53", w[0].Name)
	require.Equal(t, utc("2026-12-28T00:00:00Z"), w[0].From)
	require.Equal(t, "besdk_outbox_2027w01", w[1].Name)
	// 2024-12-30 is a Monday in ISO week 1 of 2025
	require.Equal(t, "besdk_outbox_2025w01", outboxWindow(utc("2024-12-31T00:00:00Z"), 0)[0].Name)
}

func TestOutboxWindowNegativeAheadIsCurrentWeekOnly(t *testing.T) {
	require.Len(t, outboxWindow(utc("2026-10-03T00:00:00Z"), -1), 1)
}
