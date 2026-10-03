package idem

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Inputs the vectors do not cover; each must be JSON_INVALID.
func TestCanonicalizeRejectsNonIJSON(t *testing.T) {
	for name, in := range map[string]string{
		"invalid utf-8":          "{\"a\":\"\xff\"}",
		"lone high surrogate":    `{"a":"\ud83d"}`,
		"lone low surrogate":     `{"a":"\ude00"}`,
		"high then non-low":      `{"a":"\ud83d\u0041"}`,
		"raw control in string":  "{\"a\":\"x\x01\"}",
		"bad escape":             `{"a":"\x41"}`,
		"short unicode escape":   `{"a":"\u12"}`,
		"unterminated":           `{"a":"x`,
		"two values":             `{} {}`,
		"bare minus":             `-`,
		"dot without digits":     `1.`,
		"exponent sans digits":   `1e+`,
		"duplicate after decode": `{"a":1,"\u0061":2}`,
		"too deep":               strings.Repeat("[", maxDepth+2) + strings.Repeat("]", maxDepth+2),
		"truncated literal":      `tru`,
		"missing colon":          `{"a" 1}`,
		"missing comma":          `[1 2]`,
		"non-string name":        `{1:2}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Canonicalize([]byte(in))
			require.Error(t, err)
			require.Equal(t, "JSON_INVALID", reasonOf(t, err))
		})
	}
}

func TestCanonicalizeAccepts(t *testing.T) {
	for in, want := range map[string]string{
		`1e-400`:               `0`, // underflow: the nearest double, as ECMAScript
		`"\u003c\u0026\u003e"`: `"<&>"`,
		`[[[]]]`:               `[[[]]]`,
		`"a\u0000b"`:           `"a\u0000b"`,
		`123456789`:            `123456789`,
		`-1.5e-7`:              `-1.5e-7`,
		`0.1`:                  `0.1`,
		strings.Repeat("[", maxDepth) + strings.Repeat("]", maxDepth): strings.Repeat("[", maxDepth) + strings.Repeat("]", maxDepth),
	} {
		got, err := Canonicalize([]byte(in))
		require.NoError(t, err, in)
		require.Equal(t, want, string(got), in)
	}
}

// A Go value and the raw JSON text of the same fields hash alike (Command.Request), whatever
// encoding/json escapes (HTML characters) or orders.
func TestFingerprintValueMatchesText(t *testing.T) {
	type req struct {
		Price string `json:"price"`
		Name  string `json:"name"`
		Qty   int64  `json:"qty"`
	}
	v, err := FingerprintValue(req{Price: "12.50", Name: "<A&B>", Qty: 9007199254740993})
	require.NoError(t, err)
	txt, err := Fingerprint([]byte(`{"name":"<A&B>","qty":9007199254740992,"price":"12.50"}`))
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(txt), hex.EncodeToString(v))
	require.Len(t, v, 32)

	m, err := FingerprintValue(map[string]any{"qty": 9007199254740993, "price": "12.50", "name": "<A&B>"})
	require.NoError(t, err)
	require.Equal(t, v, m)
}

func TestFingerprintValueRefusesUnencodable(t *testing.T) {
	_, err := FingerprintValue(map[string]any{"c": make(chan int)})
	require.Error(t, err)
}

func TestNamespaceNeedsIdentity(t *testing.T) {
	for _, c := range []Caller{{Kind: CallerUser}, {Kind: CallerSystemCall}, {Kind: "robot", Sub: "x"}} {
		_, err := c.Namespace()
		require.Error(t, err, "%+v", c)
	}
}
