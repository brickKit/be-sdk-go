package events

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPermanentWrapsAndUnwraps(t *testing.T) {
	base := errors.New("bad payload")
	p := Permanent(base)
	require.True(t, IsPermanent(p))
	require.True(t, IsPermanent(fmt.Errorf("handler: %w", p)))
	require.ErrorIs(t, p, base)
	require.False(t, IsPermanent(base))
	require.NoError(t, Permanent(nil))
	require.False(t, IsPermanent(nil))
}
