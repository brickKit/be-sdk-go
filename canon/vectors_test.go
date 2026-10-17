package canon

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"math/big"
	"strings"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fromJSON turns decoded JSON into the encoder's types, keeping integers exact and marking any fraction as a float.
func fromJSON(raw json.RawMessage) any {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	_ = d.Decode(&v)
	return convert(v)
}

func convert(v any) any {
	switch x := v.(type) {
	case json.Number:
		if strings.ContainsAny(x.String(), ".eE") {
			f, _ := x.Float64()
			return f
		}
		n, _ := new(big.Int).SetString(x.String(), 10)
		return n
	case []any:
		for i := range x {
			x[i] = convert(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = convert(x[k])
		}
	}
	return v
}

func TestCanonicalVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/canonical.json")
	require.NoError(t, err)
	var file struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &file))
	for _, r := range file.Cases {
		var c struct {
			ID, Kind   string
			RecordType string `json:"record_type"`
			Major      int
			In         json.RawMessage
			Expect     struct{ Out, Refused string }
		}
		require.NoError(t, json.Unmarshal(r, &c))
		var s string
		_ = json.Unmarshal(c.In, &s)
		var got string
		var err error
		switch c.Kind {
		case "decimal":
			got, err = Decimal(s)
		case "instant":
			got, err = Instant(s)
		case "text":
			got = Text(s)
		case "json":
			var b []byte
			b, err = Encode(fromJSON(c.In))
			got = string(b)
		case "digest":
			got, err = Digest(c.RecordType, c.Major, fromJSON(c.In))
		case "segment":
			continue // segment normalization is the authz package's (its vector test)
		}
		if c.Expect.Refused != "" {
			assert.ErrorIs(t, err, ErrRefused, c.ID)
			continue
		}
		require.NoError(t, err, c.ID)
		assert.Equal(t, c.Expect.Out, got, c.ID)
	}
}
