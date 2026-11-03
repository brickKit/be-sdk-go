package projection

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vectorCases(t *testing.T, topic string) []json.RawMessage {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/"+topic+".json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	return v.Cases
}

func TestWatermarkJudgeVectors(t *testing.T) { // P9.24
	for _, raw := range vectorCases(t, "watermark-judge") {
		var c struct {
			ID    string
			Query struct {
				WatermarkSource bool   `json:"watermark_source"`
				SourceState     string `json:"source_state"`
				ProjectionState string `json:"projection_state"`
				Quarantined     bool
				Watermark       *float64
				Now, Freshness  float64
				Row             *JudgedRow
			}
			Expect string
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		q := c.Query
		in := JudgeInput{WatermarkSource: q.WatermarkSource, SourceState: q.SourceState, ProjectionState: q.ProjectionState,
			Quarantined: q.Quarantined, HasWatermark: q.Watermark != nil, Freshness: q.Freshness, Row: q.Row}
		if q.Watermark != nil {
			in.WatermarkAge = q.Now - *q.Watermark
		}
		assert.Equal(t, c.Expect, Judge(in), c.ID)
	}
}

func TestWatermarkEpochVectors(t *testing.T) { // P9.27
	type id struct {
		Deployment string
		Impl       *string
		SystemID   *string `json:"system_id"`
		Epoch      *string
		Primary    bool
		Purpose    string
	}
	conv := func(i id) Identity {
		s := func(p *string) string {
			if p == nil {
				return ""
			}
			return *p
		}
		return Identity{Deployment: i.Deployment, Impl: s(i.Impl), SystemID: s(i.SystemID), Epoch: s(i.Epoch),
			Primary: i.Primary, Purpose: i.Purpose}
	}
	for _, raw := range vectorCases(t, "watermark-epoch") {
		var c struct {
			ID                string
			Adopted, Response id
			Expect            string
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		assert.Equal(t, c.Expect, DecideEpoch(conv(c.Adopted), conv(c.Response)), c.ID)
	}
}

func TestWatermarkPullOutcomeVectors(t *testing.T) { // P9.22, P9.29
	for _, raw := range vectorCases(t, "watermark-pull-outcome") {
		var c struct {
			ID       string
			Consumer struct{ Requested *int64 }
			Response struct {
				Unreachable  bool
				NeedSnapshot string `json:"need_snapshot"`
				EchoSeq      int64  `json:"echo_seq"`
			}
			Expect string
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		var req int64
		if c.Consumer.Requested != nil {
			req = *c.Consumer.Requested
		}
		r := c.Response
		assert.Equal(t, c.Expect, PullOutcome(req, r.Unreachable, r.NeedSnapshot, r.EchoSeq), c.ID)
	}
}

func TestWatermarkValueVectors(t *testing.T) { // P9.20, P9.23
	for _, raw := range vectorCases(t, "watermark-value") {
		var c struct {
			ID    string
			State struct {
				Now           float64
				MaxSeen       float64 `json:"max_seen"`
				ConsumerEpoch string  `json:"consumer_epoch"`
				RecordedEpoch string  `json:"recorded_epoch"`
				Partitions    []struct {
					Contiguous int64
					Echoes     []struct {
						Seq      int64
						MP       float64 `json:"m_p"`
						Recorded bool
					}
				}
			}
			Expect *float64
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		s := c.State
		var got *float64
		if !ClockReset(s.Now, s.MaxSeen, s.ConsumerEpoch, s.RecordedEpoch) {
			var marks []float64
			var have []bool
			for _, p := range s.Partitions {
				var es []Echo
				for _, e := range p.Echoes {
					es = append(es, Echo{Seq: e.Seq, MP: e.MP, Recorded: e.Recorded})
				}
				m, ok := LastEcho(p.Contiguous, es)
				marks, have = append(marks, m), append(have, ok)
			}
			if w, ok := Watermark(marks, have); ok {
				got = &w
			}
		}
		assert.Equal(t, c.Expect, got, c.ID)
	}
}

func TestWatermarkSourceStateVectors(t *testing.T) { // P9.26
	type fields struct {
		State      string
		Confirmed  bool
		Rollback   bool
		StalledGen *int64 `json:"stalled_gen"`
		BlockedGen *int64 `json:"blocked_gen"`
		Prior      *string
		Action     string
	}
	for _, raw := range vectorCases(t, "watermark-source-state") {
		var c struct {
			ID      string
			Current fields
			Event   struct {
				Kind     string
				Gen      int64
				Matches  bool
				RowCheck string `json:"row_check"`
			}
			Expect fields
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		cur := SourceState{State: c.Current.State, Confirmed: c.Current.Confirmed, Rollback: c.Current.Rollback,
			StalledGen: c.Current.StalledGen, BlockedGen: c.Current.BlockedGen}
		if c.Current.Prior != nil {
			cur.Prior = *c.Current.Prior
		}
		next, action := NextState(cur, SourceEvent{Kind: c.Event.Kind, Gen: c.Event.Gen, Matches: c.Event.Matches,
			RowCheck: c.Event.RowCheck})
		got := fields{State: next.State, Confirmed: next.Confirmed, Rollback: next.Rollback, StalledGen: next.StalledGen,
			BlockedGen: next.BlockedGen, Action: action}
		if next.Prior != "" {
			p := next.Prior
			got.Prior = &p
		}
		assert.Equal(t, c.Expect, got, c.ID)
	}
}
