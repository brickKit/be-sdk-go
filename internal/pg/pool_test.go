package pg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseServerVersion(t *testing.T) {
	cases := map[string]int{
		"16.4 (Debian 16.4-1.pgdg120+1)": 160004,
		"14.13":                          140013,
		"17beta1":                        170000,
		"18rc1":                          180000,
		"16.15":                          160015,
		"":                               0,
		"garbage":                        0,
	}
	for in, want := range cases {
		require.Equal(t, want, parseServerVersion(in), in)
	}
}

func TestOpenPoolValidates(t *testing.T) {
	_, err := OpenPool(PoolConfig{Host: "h", Database: "d", User: "u"})
	require.Error(t, err, "a missing Password func is refused")
	_, err = OpenPool(PoolConfig{Database: "d", User: "u", Password: func() string { return "" }})
	require.Error(t, err, "a missing host is refused")
}

func TestOpenPoolDoesNotConnect(t *testing.T) {
	calls := 0
	p, err := OpenPool(PoolConfig{Host: "127.0.0.1", Port: 1, Database: "d", User: "u",
		Password: func() string { calls++; return "x" }})
	require.NoError(t, err)
	require.Zero(t, calls, "no connection is opened by OpenPool")
	require.NoError(t, p.Close())
}
