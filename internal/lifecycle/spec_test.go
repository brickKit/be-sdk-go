package lifecycle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestParseAfter(t *testing.T) {
	a, ok := parseAfter("18mo after closed")
	require.True(t, ok)
	require.Equal(t, After{Span: Span{N: 18, Unit: "mo"}, Anchor: "closed"}, a)
	a, ok = parseAfter("30d after created")
	require.True(t, ok)
	require.Equal(t, After{Span: Span{N: 30, Unit: "d"}, Anchor: "created"}, a)
	for _, bad := range []string{"", "forever", "never", "0d after created", "3x after created", "3d after born", "3d"} {
		_, ok := parseAfter(bad)
		require.False(t, ok, bad)
	}
}

func TestSpanAddCalendarUnits(t *testing.T) {
	base := utc("2026-01-31T00:00:00Z")
	require.Equal(t, utc("2026-02-03T00:00:00Z"), Span{3, "d"}.AddTo(base))
	require.Equal(t, utc("2026-02-14T00:00:00Z"), Span{2, "w"}.AddTo(base))
	require.Equal(t, utc("2026-03-03T00:00:00Z"), Span{1, "mo"}.AddTo(base), "Go's AddDate normalises Feb 31")
	require.Equal(t, utc("2036-01-31T00:00:00Z"), Span{10, "y"}.AddTo(base))
}

// An override may only lengthen: comparable when both are day-based or both month-based; across
// families the comparison is conservative (a month counts 28 days when it must be long, 31 when short).
func TestSpanAtLeast(t *testing.T) {
	cases := []struct {
		a, b Span
		want bool
	}{
		{Span{15, "y"}, Span{10, "y"}, true},
		{Span{10, "y"}, Span{120, "mo"}, true},
		{Span{119, "mo"}, Span{10, "y"}, false},
		{Span{2, "w"}, Span{14, "d"}, true},
		{Span{13, "d"}, Span{2, "w"}, false},
		{Span{1, "mo"}, Span{28, "d"}, true},
		{Span{1, "mo"}, Span{30, "d"}, false},
		{Span{31, "d"}, Span{1, "mo"}, true},
		{Span{30, "d"}, Span{1, "mo"}, false},
	}
	for _, c := range cases {
		require.Equal(t, c.want, c.a.AtLeast(c.b), "%v >= %v", c.a, c.b)
	}
}

func TestParseSeal(t *testing.T) {
	for in, want := range map[string]Seal{
		"":                  {Kind: SealNone},
		"never":             {Kind: SealNone},
		"immediate":         {Kind: SealImmediate},
		"on_signal":         {Kind: SealOnSignal},
		"18mo after closed": {Kind: SealAfter, After: After{Span{18, "mo"}, "closed"}},
		"1w after created":  {Kind: SealAfter, After: After{Span{1, "w"}, "created"}},
	} {
		got, err := parseSeal(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	_, err := parseSeal("soon")
	require.Error(t, err)
}

// Calendar arithmetic is UTC whatever zone t carries: a database driver may hand back local times,
// and a local AddDate across a DST change would move the instant by an hour.
func TestSpanAddIsUTC(t *testing.T) {
	cet, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	local := utc("2026-12-01T00:00:00Z").In(cet)
	require.Equal(t, utc("2028-06-01T00:00:00Z"), Span{18, "mo"}.AddTo(local))
}
