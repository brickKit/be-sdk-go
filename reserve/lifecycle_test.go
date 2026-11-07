package reserve

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTheProtocolsVector: Decide answers every case of be-protocol's vector reserve-participant (P16.1-P16.8).
func TestTheProtocolsVector(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/reserve-participant.json")
	require.NoError(t, err)
	var v struct {
		Cases []struct {
			ID      string
			Current *struct {
				State string
				Past  bool
			}
			Command string
			Expect  struct {
				State    string
				Answer   string
				Resource string
				Changed  bool
			}
		}
	}
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	for _, c := range v.Cases {
		s, past := Absent, false
		if c.Current != nil {
			s, past = State(c.Current.State), c.Current.Past
		}
		d := Decide(s, past, Command(c.Command))
		answer := "ok"
		switch {
		case d.Conflict:
			answer = "conflict"
		case d.Refused != "":
			answer = "refused:" + string(d.Refused)
		}
		resource := map[Resource]string{Untouched: "none", Takes: "take", GivesBack: "give_back", Consumes: "consume"}[d.Resource]
		assert.Equal(t, [4]any{c.Expect.State, c.Expect.Answer, c.Expect.Resource, c.Expect.Changed},
			[4]any{string(d.To), answer, resource, d.Changed}, c.ID)
	}
}

// TestEndedStatesStayEnded: nothing moves a branch out of a final state (P16.1).
func TestEndedStatesStayEnded(t *testing.T) {
	for _, s := range States {
		if !s.Ended() {
			continue
		}
		for _, c := range []Command{OnQuery, OnConfirm, OnCancel, OnExpire, OnHoldSame, OnHoldOther, OnConsumeWithin,
			OnConsumeExact, OnConsumeBeyond, OnConsumeUsedSame, OnConsumeUsedOther} {
			d := Decide(s, false, c)
			assert.Equal(t, s, d.To, "%s moved %s", c, s)
			assert.False(t, d.Changed, "%s changed %s", c, s)
		}
	}
}
