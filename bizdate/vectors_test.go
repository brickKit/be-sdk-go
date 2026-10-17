package bizdate

import (
	"encoding/json"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBusinessDateVectors(t *testing.T) { // P5.29
	raw, err := protocol.Vectors.ReadFile("vectors/business-date.json")
	require.NoError(t, err)
	var f struct {
		Cases []struct{ ID, Instant, TZ, Date string }
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Cases)
	for _, c := range f.Cases {
		at, err := time.Parse(time.RFC3339, c.Instant)
		require.NoError(t, err)
		got, err := Of(at, c.TZ)
		require.NoError(t, err, c.ID)
		assert.Equal(t, c.Date, got, c.ID)
	}
	for _, tz := range []string{"", "Local", "Mars/Olympus"} {
		_, err := Of(time.Now(), tz)
		assert.Error(t, err, "%q has no implicit default", tz)
	}
}
