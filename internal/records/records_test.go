package records

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSecurityRecordVectors: the generated schemas accept and refuse what the vector says, and an accepted record's
// canonical bytes and digest are the vector's (P8.8, D76).
func TestSecurityRecordVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/security-records.json")
	require.NoError(t, err)
	var v struct {
		Cases []struct {
			ID         string
			RecordType string `json:"record_type"`
			Record     map[string]any
			Valid      bool
			Canonical  string
			Digest     string
		}
	}
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	for _, c := range v.Cases {
		body, digest, err := Seal(c.RecordType, numbers(c.Record).(map[string]any))
		if !c.Valid {
			if err == nil {
				_, err = Open(c.RecordType, body, digest)
			}
			assert.Error(t, err, c.ID)
			continue
		}
		require.NoError(t, err, c.ID)
		assert.Equal(t, c.Canonical, string(body), c.ID)
		assert.Equal(t, c.Digest, digest, c.ID)
		_, err = Open(c.RecordType, body, digest)
		assert.NoError(t, err, c.ID)
	}
}

func TestOpenRefusesWhatIsNotExactlyCanonical(t *testing.T) {
	rec := map[string]any{"component": "demo/order", "version": "3.0.0", "definitions": []any{}}
	body, digest, err := Seal("be.security.definition_set", rec)
	require.NoError(t, err)
	_, err = Open("be.security.definition_set", body, digest)
	require.NoError(t, err)
	spaced := []byte(`{"component": "demo/order","definitions":[],"version":"3.0.0"}`)
	_, err = Open("be.security.definition_set", spaced, "")
	assert.ErrorIs(t, err, ErrInvalid, "whitespace")
	reordered := []byte(`{"version":"3.0.0","component":"demo/order","definitions":[]}`)
	_, err = Open("be.security.definition_set", reordered, "")
	assert.ErrorIs(t, err, ErrInvalid, "key order")
	_, err = Open("be.security.definition_set", body, "sha256:"+string(make([]byte, 0)))
	assert.ErrorIs(t, err, ErrInvalid, "another digest")
	decomposed := []byte("{\"component\":\"demo/order\",\"definitions\":[],\"version\":\"é\"}")
	_, err = Open("be.security.definition_set", decomposed, "")
	assert.ErrorIs(t, err, ErrInvalid, "text not in NFC")
}

// numbers turns the float64 of encoding/json into json.Number, as Open decodes them.
func numbers(v any) any {
	switch x := v.(type) {
	case float64:
		return json.Number(fmtFloat(x))
	case []any:
		for i := range x {
			x[i] = numbers(x[i])
		}
	case map[string]any:
		for k := range x {
			x[k] = numbers(x[k])
		}
	}
	return v
}

func fmtFloat(f float64) string {
	b, _ := json.Marshal(f)
	return string(b)
}
