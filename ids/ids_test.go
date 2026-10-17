package ids

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIsVersion7AndOrdered(t *testing.T) {
	prev := New()
	assert.Equal(t, 7, int(prev.Version()))
	for i := 0; i < 1000; i++ {
		id := New()
		assert.True(t, id.String() > prev.String(), "ids are ordered within one process")
		prev = id
	}
}
