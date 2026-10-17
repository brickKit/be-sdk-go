package search

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/search.json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	for _, r := range v.Cases {
		var c struct {
			ID, Kind, Q string
			Values      []string
			Expect      json.RawMessage
		}
		require.NoError(t, json.Unmarshal(r, &c))
		var got any
		switch c.Kind {
		case "text":
			got = Text(c.Values...)
		case "query":
			got = Query(c.Q)
		case "match":
			got = Match(Text(c.Values...), c.Q)
		}
		b, _ := json.Marshal(got)
		assert.JSONEq(t, string(c.Expect), string(b), c.ID)
	}
}
