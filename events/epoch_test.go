package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseEpochReadsWhatStringWrites(t *testing.T) {
	for _, e := range []Epoch{{1, 1}, {3, 4294967295}, {0, 0}} {
		got, err := ParseEpoch(e.String())
		assert.NoError(t, err)
		assert.Equal(t, e, got)
	}
	for _, bad := range []string{"", "1", "1.", ".1", "1.2.3", "a.1", "-1.2", "01.2", "1.+2", " 1.2"} {
		_, err := ParseEpoch(bad)
		assert.Error(t, err, bad)
	}
}
