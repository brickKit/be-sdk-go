package idem

import (
	"encoding/json"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyVectors(t *testing.T) { // P4.21, P4.22
	raw, err := protocol.Vectors.ReadFile("vectors/idempotency.json")
	require.NoError(t, err)
	var f struct {
		Cases []struct {
			ID, Kind, Key, Principal, Operation, Target, Body, Fingerprint string
			Valid                                                          bool
		}
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Cases)
	for _, c := range f.Cases {
		switch c.Kind {
		case "key":
			assert.Equal(t, c.Valid, ValidKey(c.Key), c.ID)
		case "fingerprint":
			got, err := Request{Principal: c.Principal, Operation: c.Operation, Target: c.Target, Body: []byte(c.Body)}.Fingerprint()
			require.NoError(t, err, c.ID)
			assert.Equal(t, c.Fingerprint, got, c.ID)
		default:
			t.Errorf("%s: unknown kind %q", c.ID, c.Kind)
		}
	}
}
