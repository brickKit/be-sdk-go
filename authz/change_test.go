package authz

import (
	"encoding/json"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCandidateDigest(t *testing.T) {
	raw := json.RawMessage(`{"record_type":"management_unit","name":"east","max_subdelegation_depth":2,"managed_scope":[]}`)
	got, err := CandidateDigest("management_unit", raw)
	require.NoError(t, err)
	want, err := canon.Digest("be.security.management_unit", 1, map[string]any{"record_type": "management_unit", "name": "east",
		"max_subdelegation_depth": json.Number("2"), "managed_scope": []any{}})
	require.NoError(t, err)
	assert.Equal(t, want, got, "the canonical form under be.security.<type>, v1; an integer stays a number")

	reordered, err := CandidateDigest("management_unit", json.RawMessage(`{ "name": "east", "managed_scope": [],
		"max_subdelegation_depth": 2, "record_type": "management_unit" }`))
	require.NoError(t, err)
	assert.Equal(t, got, reordered, "key order and whitespace do not matter")
	other, err := CandidateDigest("grant", raw)
	require.NoError(t, err)
	assert.NotEqual(t, got, other, "the record type is part of it")

	for name, bad := range map[string]string{"a float": `{"x":1.5}`, "a null": `{"x":null}`, "not an object": `[1]`, "not JSON": `{`} {
		_, err := CandidateDigest("grant", json.RawMessage(bad))
		assert.Error(t, err, name)
	}
}
