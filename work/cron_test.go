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
