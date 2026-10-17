package projection

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjectionApplyVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/projection-apply.json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	for _, r := range v.Cases {
		var c struct {
			ID      string
			Current *struct {
				State       string
				Version     int64
				NeedVersion int64 `json:"need_version"`
				Old         bool
			}
			Input struct {
				Kind    string
				Version int64
			}
			Expect struct {
				State       string
				Version     int64
				NeedVersion int64 `json:"need_version"`
				Action      string
			}
		}
		require.NoError(t, json.Unmarshal(r, &c))
		var cur *Row
		if c.Current != nil {
			cur = &Row{State: c.Current.State, Version: c.Current.Version, NeedVersion: c.Current.NeedVersion, Old: c.Current.Old}
		}
		next, action := Decide(cur, c.Input.Kind, c.Input.Version)
		assert.Equal(t, c.Expect.Action, action, c.ID)
		assert.Equal(t, c.Expect.State, next.State, c.ID)
		assert.Equal(t, c.Expect.Version, next.Version, c.ID)
		assert.Equal(t, c.Expect.NeedVersion, next.NeedVersion, c.ID)
	}
}
