package runprofile

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The run-profile vectors (P12.1-P12.5): the bytes to sign without the signature, the key, each kind's retention,
// a generation's current value from the valid records, and its advance.
func TestTheRunProfileVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/run-profile.json")
	require.NoError(t, err)
	var doc struct {
		Cases []struct {
			ID       string
			Input    json.RawMessage
			Expected map[string]any
		}
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.NotEmpty(t, doc.Cases)
	for _, c := range doc.Cases {
		t.Run(c.ID, func(t *testing.T) {
			var in struct {
				Record  *Record
				Kind    string
				Records []struct {
					Body           json.RawMessage
					SignatureHolds bool `json:"signature_holds"`
				}
				RunProfile *int64  `json:"run_profile"`
				Evidence   int64   `json:"evidence"`
				Databases  []int64 `json:"databases"`
			}
			require.NoError(t, json.Unmarshal(c.Input, &in))
			switch {
			case in.Record != nil:
				b, err := SignedBytes(*in.Record)
				require.NoError(t, err)
				assert.Equal(t, c.Expected["signed_bytes"], string(b))
				if k, ok := c.Expected["key"]; ok {
					assert.Equal(t, k, Key(in.Record.Deployment, in.Record.Kind, in.Record.WrittenAt, in.Record.ID))
				}
			case in.Kind != "":
				assert.EqualValues(t, c.Expected["min_retention_days"], Retention(in.Kind).Hours()/24)
			case in.RunProfile != nil:
				assert.EqualValues(t, c.Expected["next"], NextGeneration(*in.RunProfile, append([]int64{in.Evidence}, in.Databases...)...))
			default:
				var valid []Record
				for _, r := range in.Records {
					if r.SignatureHolds {
						valid = append(valid, Record{Body: r.Body})
					}
				}
				assert.EqualValues(t, c.Expected["current"], currentOf(valid))
			}
		})
	}
}
