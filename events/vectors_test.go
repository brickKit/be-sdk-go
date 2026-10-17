package events

import (
	"encoding/json"
	"io/fs"
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
