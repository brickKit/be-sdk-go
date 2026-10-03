package lifecycle

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func str(s string) *string { return &s }

// The canonical unit digest of P16: text values, NULL as \N, 0x1F between fields, 0x1E between rows.
// Expected values computed independently (Python hashlib).
func TestUnitDigest(t *testing.T) {
	d := NewDigest()
	d.Row([]*string{str("1"), str("a"), nil})
	d.Row([]*string{str("2"), str("b\tc"), str("t")})
	require.Equal(t, 2, d.Rows())
	unit := d.Sum()
	require.Equal(t, "9d58e32ab9045be11702cde6f97f38172b9217e8a1982e42b9a2f9b45afe7d6a", hex.EncodeToString(unit))

	require.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		hex.EncodeToString(NewDigest().Sum()), "an empty unit is SHA-256 of nothing")

	c1 := Chain(nil, unit)
	require.Equal(t, "88f602e732562cb6365903a3e49d50abeea91260387a1534adfb15edd26e4072", hex.EncodeToString(c1), "the first link has no predecessor")
	require.Equal(t, "080e420aa2da0c1181094039eec873b618a4af5fbba593eda5211e6d18906979", hex.EncodeToString(Chain(c1, unit)))
}
