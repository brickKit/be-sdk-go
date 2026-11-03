package work

import (
	"encoding/json"
	"io/fs"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCronSlotVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/cron-slots.json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	ts := func(s string) time.Time { x, err := time.Parse(time.RFC3339, s); require.NoError(t, err); return x }
	for _, r := range v.Cases {
		var c struct {
			ID, Kind, Cron, TZ, Policy, After, Until, Watermark, Now string
			MaxBacklog                                               int `json:"max_backlog"`
			Expect                                                   json.RawMessage
		}
		require.NoError(t, json.Unmarshal(r, &c))
		s, err := ParseCron(c.Cron)
		require.NoError(t, err, c.ID)
		loc, err := time.LoadLocation(c.TZ)
		require.NoError(t, err)
		if c.Kind == "slots" {
			var want []struct{ Slot, At string }
			require.NoError(t, json.Unmarshal(c.Expect, &want))
			got := s.Slots(loc, ts(c.After), ts(c.Until))
			require.Len(t, got, len(want), c.ID)
			for i := range want {
				assert.Equal(t, want[i].Slot, got[i].ID, c.ID)
				assert.Equal(t, ts(want[i].At), got[i].At, "%s %s", c.ID, want[i].Slot)
			}
			continue
		}
		var want struct {
			Pending, Paused []string
			Watermark       string
		}
		require.NoError(t, json.Unmarshal(c.Expect, &want))
		pending, paused, next := Plan(s, loc, Backfill(c.Policy), c.MaxBacklog, ts(c.Watermark), ts(c.Now))
		ids := func(sl []Slot) []string {
			out := []string{}
			for _, x := range sl {
				out = append(out, x.ID)
			}
			return out
		}
		if want.Pending == nil {
			want.Pending = []string{}
		}
		if want.Paused == nil {
			want.Paused = []string{}
		}
		assert.Equal(t, want.Pending, ids(pending), c.ID)
		assert.Equal(t, want.Paused, ids(paused), c.ID)
		assert.Equal(t, ts(want.Watermark), next, c.ID)
	}
}

func TestCronRejectsBadExpressions(t *testing.T) {
	for _, e := range []string{"* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "*/0 * * * *", "a * * * *", "5-1 * * * *"} {
		_, err := ParseCron(e)
		assert.Error(t, err, e)
	}
}

func TestNextIsTheFirstSlotAfter(t *testing.T) { // P7.23
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	for _, c := range []struct {
		expr, after, want string
		loc               *time.Location
	}{
		{"*/5 * * * *", "2026-11-03T10:02:00Z", "2026-11-03T10:05", time.UTC},
		{"*/5 * * * *", "2026-11-03T10:05:00Z", "2026-11-03T10:10", time.UTC}, // after, not at
		{"0 0 1 1 *", "2026-11-03T10:02:00Z", "2027-01-01T00:00", time.UTC},
		{"0 0 29 2 *", "2026-03-01T00:00:00Z", "2028-02-29T00:00", time.UTC},
		{"30 2 * * *", "2026-03-28T12:00:00Z", "2026-03-29T02:30", berlin}, // in the spring gap: its identity is kept
	} {
		s, err := ParseCron(c.expr)
		require.NoError(t, err)
		after, err := time.Parse(time.RFC3339, c.after)
		require.NoError(t, err)
		slot, ok := s.Next(c.loc, after)
		require.True(t, ok, c.expr)
		assert.Equal(t, c.want, slot.ID, c.expr)
		assert.Equal(t, s.Slots(c.loc, after, slot.At), []Slot{slot}, "%s: no slot in between", c.expr)
	}
	never, err := ParseCron("0 0 31 2 *")
	require.NoError(t, err)
	_, ok := never.Next(time.UTC, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.False(t, ok, "31 February never comes")
}
