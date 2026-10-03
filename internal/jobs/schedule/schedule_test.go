package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func zone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	require.NoError(t, err)
	return loc
}

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestParseAccepts(t *testing.T) {
	for _, expr := range []string{
		"* * * * *", "0 3 * * *", "*/15 9-17 * * 1-5", "0 0 1,15 * *", "0 9 * * MON-FRI",
		"0 0 1 jan *", "0 0 * * 7", "0-59/10 * * * *", "5 4 * * sun", "0 0 29 2 *", "0  3 * *  *",
		"@every 1s", "@every 2s", "@every 1h30m", "@every 1.5s",
	} {
		_, err := Parse(expr, time.UTC)
		require.NoError(t, err, expr)
	}
}

func TestParseRejects(t *testing.T) {
	for _, expr := range []string{
		"", "* * * *", "* * * * * *", "@daily", "@reboot", "@hourly", "@every 500ms", "@every 0s",
		"@every -1s", "@every", "@every 1x", "@every  2s", "60 * * * *", "* 24 * * *", "* * 0 * *",
		"* * 32 * *", "* * * 0 *", "* * * 13 *", "* * * * 8", "*/0 * * * *", "5-1 * * * *",
		"1,,2 * * * *", "a * * * *", "0 0 30 2 *", "0 0 31 4,6,9,11 *", "5/10 * * * *", "? * * * *",
		"0 0 L * *", "0 0 * * 1#2", "0 0 * * MON-", "0 0 * FOO *", "-1 * * * *", "*/61 * * * *",
	} {
		_, err := Parse(expr, time.UTC)
		require.Error(t, err, "%q must be rejected", expr)
	}
}

func TestParseNeedsZoneForCron(t *testing.T) {
	_, err := Parse("0 3 * * *", nil)
	require.Error(t, err)
	_, err = Parse("@every 2s", nil)
	require.NoError(t, err, "@every is zone-independent")
}

type slotCase struct {
	name, expr, zone string
	at               time.Time
	prev, next       time.Time
}

func TestPrevNext(t *testing.T) {
	cases := []slotCase{
		{"daily at 03:00 Shanghai", "0 3 * * *", "Asia/Shanghai", utc("2026-10-03T02:00:00Z"),
			utc("2026-10-02T19:00:00Z"), utc("2026-10-03T19:00:00Z")},
		{"exactly on a slot", "0 3 * * *", "Asia/Shanghai", utc("2026-10-02T19:00:00Z"),
			utc("2026-10-02T19:00:00Z"), utc("2026-10-03T19:00:00Z")},
		{"working hours over a weekend", "*/15 9-17 * * 1-5", "Asia/Shanghai", utc("2026-10-03T02:00:00Z"),
			utc("2026-10-02T09:45:00Z"), utc("2026-10-05T01:00:00Z")},
		{"names", "0 9 * * MON-FRI", "UTC", utc("2026-10-03T00:00:00Z"),
			utc("2026-10-02T09:00:00Z"), utc("2026-10-05T09:00:00Z")},
		{"leap day", "0 0 29 2 *", "Asia/Shanghai", utc("2026-03-01T00:00:00Z"),
			utc("2024-02-28T16:00:00Z"), utc("2028-02-28T16:00:00Z")},
		{"dom or dow when both restricted", "0 0 13 * 5", "UTC", utc("2026-10-10T00:00:00Z"),
			utc("2026-10-09T00:00:00Z"), utc("2026-10-13T00:00:00Z")},
		// 2026-10-12 is a Monday with an even day: OR would match it, AND (Vixie: a field starting
		// with * makes the day rule AND) does not.
		{"dom and dow when dom starts with *", "0 0 */2 * 1", "UTC", utc("2026-10-12T00:00:00Z"),
			utc("2026-10-05T00:00:00Z"), utc("2026-10-19T00:00:00Z")},
		{"sunday as 7", "0 12 * * 7", "UTC", utc("2026-10-03T00:00:00Z"),
			utc("2026-09-27T12:00:00Z"), utc("2026-10-04T12:00:00Z")},
		{"every minute", "* * * * *", "UTC", utc("2026-10-03T10:00:30Z"),
			utc("2026-10-03T10:00:00Z"), utc("2026-10-03T10:01:00Z")},
		{"year end", "0 0 1 1 *", "UTC", utc("2026-12-31T23:59:59Z"),
			utc("2026-01-01T00:00:00Z"), utc("2027-01-01T00:00:00Z")},
		// Europe/Berlin, 2026-03-29: 02:00 CET jumps to 03:00 CEST (01:00Z). A wall time inside the gap
		// runs at the first instant after it.
		{"Berlin gap: 02:30 runs at 03:00 CEST", "30 2 * * *", "Europe/Berlin", utc("2026-03-28T23:00:00Z"),
			utc("2026-03-28T01:30:00Z"), utc("2026-03-29T01:00:00Z")},
		{"Berlin gap: the day after", "30 2 * * *", "Europe/Berlin", utc("2026-03-29T01:00:00Z"),
			utc("2026-03-29T01:00:00Z"), utc("2026-03-30T00:30:00Z")},
		{"Berlin gap: four wall slots collapse into one", "*/15 2 * * *", "Europe/Berlin", utc("2026-03-28T23:30:00Z"),
			utc("2026-03-28T01:45:00Z"), utc("2026-03-29T01:00:00Z")},
		{"Berlin gap: collapsed slot is followed by the next day", "*/15 2 * * *", "Europe/Berlin", utc("2026-03-29T01:00:00Z"),
			utc("2026-03-29T01:00:00Z"), utc("2026-03-30T00:00:00Z")},
		// Europe/Berlin, 2026-10-25: 03:00 CEST falls back to 02:00 CET (01:00Z). A repeated wall time
		// runs once, at its first occurrence.
		{"Berlin overlap: first occurrence only", "30 2 * * *", "Europe/Berlin", utc("2026-10-24T22:00:00Z"),
			utc("2026-10-24T00:30:00Z"), utc("2026-10-25T00:30:00Z")},
		{"Berlin overlap: second occurrence skipped", "30 2 * * *", "Europe/Berlin", utc("2026-10-25T00:30:00Z"),
			utc("2026-10-25T00:30:00Z"), utc("2026-10-26T01:30:00Z")},
		{"Berlin overlap: half-hourly skips the repeated hour", "*/30 * * * *", "Europe/Berlin", utc("2026-10-25T00:30:00Z"),
			utc("2026-10-25T00:30:00Z"), utc("2026-10-25T02:00:00Z")},
		{"Berlin overlap: prev from inside the second occurrence", "*/30 * * * *", "Europe/Berlin", utc("2026-10-25T01:45:00Z"),
			utc("2026-10-25T00:30:00Z"), utc("2026-10-25T02:00:00Z")},
		// America/New_York, 2026-03-08: 02:00 EST jumps to 03:00 EDT (07:00Z); 2026-11-01: 02:00 EDT
		// falls back to 01:00 EST (06:00Z).
		{"New York gap", "30 2 * * *", "America/New_York", utc("2026-03-08T05:00:00Z"),
			utc("2026-03-07T07:30:00Z"), utc("2026-03-08T07:00:00Z")},
		{"New York overlap", "30 1 * * *", "America/New_York", utc("2026-11-01T05:00:00Z"),
			utc("2026-10-31T05:30:00Z"), utc("2026-11-01T05:30:00Z")},
		{"New York overlap: second 01:30 skipped", "30 1 * * *", "America/New_York", utc("2026-11-01T06:30:00Z"),
			utc("2026-11-01T05:30:00Z"), utc("2026-11-02T06:30:00Z")},
		{"every 2s", "@every 2s", "Asia/Shanghai", utc("2026-10-03T10:00:03.5Z"),
			utc("2026-10-03T10:00:02Z"), utc("2026-10-03T10:00:04Z")},
		{"every 2s on a slot", "@every 2s", "UTC", utc("2026-10-03T10:00:04Z"),
			utc("2026-10-03T10:00:04Z"), utc("2026-10-03T10:00:06Z")},
		{"every 7s counts from the epoch", "@every 7s", "UTC", time.Unix(100, 0),
			time.Unix(98, 0), time.Unix(105, 0)},
		{"every 90m counts from the epoch, not the zone", "@every 90m", "Asia/Shanghai", utc("2026-10-03T10:00:00Z"),
			utc("2026-10-03T09:00:00Z"), utc("2026-10-03T10:30:00Z")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := Parse(c.expr, zone(t, c.zone))
			require.NoError(t, err)
			require.True(t, c.prev.Equal(s.Prev(c.at)), "Prev: want %s, got %s", c.prev, s.Prev(c.at).UTC())
			require.True(t, c.next.Equal(s.Next(c.at)), "Next: want %s, got %s", c.next, s.Next(c.at).UTC())
		})
	}
}

// TestNextPrevConsistent: walking forward with Next and back with Prev visits the same slots, in
// increasing order, across both Berlin transitions of 2026.
func TestNextPrevConsistent(t *testing.T) {
	berlin := zone(t, "Europe/Berlin")
	for _, expr := range []string{"*/20 * * * *", "30 2 * * *", "0 1-3 * * *", "*/15 2 * * *"} {
		s, err := Parse(expr, berlin)
		require.NoError(t, err)
		for _, start := range []time.Time{utc("2026-03-28T20:00:00Z"), utc("2026-10-24T20:00:00Z")} {
			at := start
			for range 40 {
				n := s.Next(at)
				require.True(t, n.After(at), "%s: Next(%s) = %s is not after", expr, at, n)
				require.True(t, n.Equal(s.Prev(n)), "%s: Prev(Next) != Next at %s", expr, n)
				require.True(t, s.Prev(n.Add(-time.Nanosecond)).Before(n), "%s: a slot between", expr)
				at = n
			}
		}
	}
}

func TestString(t *testing.T) {
	s, err := Parse("0 3 * * *", time.UTC)
	require.NoError(t, err)
	require.Equal(t, "0 3 * * *", s.String())
	e, err := Parse("@every 2s", nil)
	require.NoError(t, err)
	require.Equal(t, "@every 2s", e.String())
}
