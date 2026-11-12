package flow

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTheProtocolsVector: Decide answers every case of be-protocol's vector flow-kernel (P17.2, P17.6, P17.10, P17.11,
// P17.13).
func TestTheProtocolsVector(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/flow-kernel.json")
	require.NoError(t, err)
	var v struct {
		Cases []struct {
			ID   string
			Case struct {
				Task, Instance, Qualification, Decision, Form string
				Version, Self, Candidate, Operation           bool
				Revision, Replay, Others                      bool
				LastStep                                      bool   `json:"last_step"`
				RejectOutcome                                 string `json:"reject_outcome"`
			}
			Expect struct {
				Answer, Task, Instance, Others string
				NextStep                       bool `json:"next_step"`
				Changed                        bool
			}
		}
	}
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	for _, c := range v.Cases {
		k := c.Case
		d := Decide(Situation{Task: TaskState(k.Task), Instance: InstanceState(k.Instance), VersionMatches: k.Version,
			Self: k.Self, Candidate: k.Candidate, Operation: k.Operation, Qualification: Qualification(k.Qualification),
			RevisionMatches: k.Revision, Replay: k.Replay}, Choice(k.Decision), k.Others, k.LastStep, InstanceState(k.RejectOutcome))
		answer := "ok"
		switch {
		case d.Replay:
			answer = "replay"
		case d.Refused != "":
			answer = d.Refused
		}
		others := "unchanged"
		if d.SupersedeOthers {
			others = "superseded"
		}
		assert.Equal(t, [6]any{c.Expect.Answer, c.Expect.Task, c.Expect.Instance, c.Expect.Others, c.Expect.NextStep, c.Expect.Changed},
			[6]any{answer, string(d.Task), string(d.Instance), others, d.NextStep, d.Changed}, c.ID)
	}
}
