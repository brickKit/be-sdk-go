package events

import (
	"encoding/json"
	"io/fs"
	"strconv"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cases(t *testing.T, topic string) []json.RawMessage {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/"+topic+".json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	return v.Cases
}

func TestPartitionVectors(t *testing.T) {
	for _, raw := range cases(t, "partition") {
		var c struct {
			ID         string
			RecordID   string `json:"record_id"`
			Partitions int
			Expect     int
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		assert.Equal(t, c.Expect, Stream{Name: "s", Partitions: c.Partitions}.PartitionOf(c.RecordID), c.ID)
	}
}

func TestEnvelopeVectors(t *testing.T) {
	for _, raw := range cases(t, "envelope") {
		var c struct {
			ID    string
			Event struct {
				EventID     string `json:"event_id"`
				Source      string
				Type        string
				Time        string
				RecordID    string `json:"record_id"`
				Stream      string
				Partition   int
				Impl        string
				Epoch       [2]int64
				Seq         int64
				Version     int64
				Projection  string
				Hops        int
				Payload     string
				Causation   string
				LegalEntity string `json:"legal_entity"`
				Subjects    []string
				Claim       bool
				Traceparent string
			}
			Expect struct {
				Headers map[string]string
				Digest  string
				Subject string
			}
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		e := c.Event
		at, err := time.Parse(time.RFC3339Nano, e.Time)
		require.NoError(t, err)
		r := Row{EventID: e.EventID, Stream: e.Stream, Partition: e.Partition, Type: e.Type, OccurredAt: at,
			RecordID: e.RecordID, Version: e.Version, Projection: Projection(e.Projection), LegalEntity: e.LegalEntity,
			Subjects: e.Subjects, Causation: e.Causation, Hops: e.Hops, Payload: []byte(e.Payload), Claim: e.Claim,
			Seq: e.Seq, Epoch: Epoch{e.Epoch[0], e.Epoch[1]}}
		if e.Traceparent != "" {
			r.Trace = map[string]string{"traceparent": e.Traceparent}
		}
		r.Digest = Digest(r.EventID, r.Payload)
		assert.Equal(t, c.Expect.Digest, r.Digest, c.ID)
		assert.Equal(t, c.Expect.Headers, r.Headers(e.Source, e.Impl), c.ID)
		assert.Equal(t, c.Expect.Subject, Subject(r.Stream, r.Partition, r.Type), c.ID)
	}
}

func TestWatermarkPullCheckVectors(t *testing.T) { // P9.15
	type ident struct {
		EchoNonce string `json:"echo_nonce"`
		Record    *struct {
			RecordID string `json:"record_id"`
			Version  int64
			Digest   string
		}
	}
	toRow := func(i ident) RowIdentity {
		if i.Record != nil {
			return RowIdentity{RecordID: i.Record.RecordID, Version: i.Record.Version, Digest: i.Record.Digest}
		}
		return RowIdentity{Echo: i.EchoNonce}
	}
	for _, raw := range cases(t, "watermark-pull-check") {
		var c struct {
			ID     string
			Source struct {
				Impl, Epoch string
				Counter     int64
				Rows        map[string]ident
			}
			Request struct {
				Position *Position
				Row      ident
			}
			Expect string
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		var at *RowIdentity
		if p := c.Request.Position; p != nil {
			if r, ok := c.Source.Rows[strconv.FormatInt(p.Seq, 10)]; ok {
				x := toRow(r)
				at = &x
			}
		}
		got := PullCheck(c.Source.Impl, c.Source.Epoch, c.Source.Counter, c.Request.Position, toRow(c.Request.Row), at)
		assert.Equal(t, c.Expect, got, c.ID)
	}
}

// The progress of P6.37 against be-protocol's inbox-progress vector: the cut by runs and the delivery lifetime, the
// largest position handled at or before it.
func TestProgressVectors(t *testing.T) {
	base := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	at := func(s int64) time.Time { return base.Add(time.Duration(s) * time.Second) }
	for _, raw := range cases(t, "inbox-progress") {
		var c struct {
			ID    string
			Input struct {
				Runs []int64
				Rows []struct {
					Key string
					Pos []int64
					At  int64
				}
				Lifetime int64
			}
			Expected struct{ Progress map[string][]int64 }
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		runs := make([]time.Time, len(c.Input.Runs))
		for i, s := range c.Input.Runs {
			runs[i] = at(s)
		}
		var rows []Handled
		for _, r := range c.Input.Rows {
			h := Handled{Key: SourceKey{Component: "c", Impl: "i", Stream: "s", Partition: partitionOfKey(t, r.Key)}, At: at(r.At)}
			if r.Pos != nil {
				h.Pos = &Point{Epoch: Epoch{Generation: r.Pos[0], Timeline: r.Pos[1]}, Seq: r.Pos[2]}
			}
			rows = append(rows, h)
		}
		got := ProgressOf(runs, rows, time.Duration(c.Input.Lifetime)*time.Second)
		want := map[SourceKey]Point{}
		for k, p := range c.Expected.Progress {
			want[SourceKey{Component: "c", Impl: "i", Stream: "s", Partition: partitionOfKey(t, k)}] =
				Point{Epoch: Epoch{Generation: p[0], Timeline: p[1]}, Seq: p[2]}
		}
		assert.Equal(t, want, got, c.ID)
	}
}

func partitionOfKey(t *testing.T, key string) int {
	n, err := strconv.Atoi(key[1:]) // p0, p1
	require.NoError(t, err)
	return n
}
