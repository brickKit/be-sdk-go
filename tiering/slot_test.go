package tiering

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// P15.26: the member's cold reads at once are bounded (TIERING_COLD_CONCURRENCY); a read waiting for a slot past its
// limit gives up, and a slot given back is taken again.
func TestColdReadsAreBoundedPerMember(t *testing.T) {
	c := &Catalog{ColdConcurrency: 2}
	ctx := context.Background()
	first, err := c.slot(ctx)
	require.NoError(t, err)
	second, err := c.slot(ctx)
	require.NoError(t, err)
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = c.slot(short)
	assert.ErrorIs(t, err, context.DeadlineExceeded, "a third read waits, and its limit runs out")
	first()
	third, err := c.slot(ctx)
	require.NoError(t, err, "a slot given back")
	second()
	third()
}
