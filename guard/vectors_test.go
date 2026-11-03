package guard_test

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/guard"
)

// The token-stale vector (P8.25), with the subject entry decoded as the view's own type: an absent subject or an
// absent field both count as 0.
func TestTokenStaleVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/token-stale.json")
	require.NoError(t, err)
	var v struct {
		Cases []struct {
			ID      string
			TokenSV int64               `json:"token_sv"`
			Entry   *authz.SubjectEntry `json:"entry"`
			Expect  struct{ Stale bool }
		}
	}
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	for _, c := range v.Cases {
		var viewSV int64
		if c.Entry != nil {
			viewSV = c.Entry.SecurityVersion
		}
		assert.Equal(t, c.Expect.Stale, guard.Stale(c.TokenSV, viewSV), c.ID)
	}
}
