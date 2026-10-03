package pg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrefixCannotCloseTheComment(t *testing.T) {
	require.Equal(t, "/* be:s_abc */ ", newTx(nil, "s_abc").prefix)
	require.Equal(t, "/* be:a*_/b */ ", newTx(nil, "a*/b").prefix)
}
