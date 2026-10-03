package authn

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func jwksBody(t *testing.T, keys ...map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"keys": keys})
	require.NoError(t, err)
	return b
}

func with(k map[string]any, kv ...any) map[string]any {
	out := maps.Clone(k)
	for i := 0; i < len(kv); i += 2 {
		if kv[i+1] == nil {
			delete(out, kv[i].(string))
			continue
		}
		out[kv[i].(string)] = kv[i+1]
	}
	return out
}

func TestParseJWKSKeepsUsableKeys(t *testing.T) {
	rs, ec, ed := newRSAKey(t, "rs", 2048), newECKey(t, "ec"), newEdKey(t, "ed")
	small := newRSAKey(t, "small", 1024)
	other := newECKey(t, "x")
	cases := []map[string]any{
		rs.jwk, ec.jwk, ed.jwk,
		small.jwk, // RSA below 2048 bits
		with(rs.jwk, "kid", "no-alg", "alg", nil),       // the iam contract requires alg on every key
		with(rs.jwk, "kid", "mismatch", "alg", "ES256"), // alg does not fit kty
		with(ec.jwk, "kid", "p384", "crv", "P-384"),
		with(ec.jwk, "kid", "off-curve", "y", other.jwk["x"]),
		with(ed.jwk, "kid", "short-ed", "x", "AAAA"),
		with(rs.jwk, "kid", "private", "d", "AAAA"),
		with(rs.jwk, "kid", "enc", "use", "enc"),
		with(rs.jwk, "kid", nil),
		with(rs.jwk, "kid", "hs", "kty", "oct", "alg", "HS256", "k", "c2VjcmV0"),
		with(ed.jwk, "kid", "x448", "crv", "Ed448"),
		with(rs.jwk, "kid", "rs-no-use", "use", nil), // use is optional for a verifier
	}
	keys, err := parseJWKS(jwksBody(t, cases...))
	require.NoError(t, err)
	got := slices.Sorted(maps.Keys(keys))
	require.Equal(t, []string{"ec", "ed", "rs", "rs-no-use"}, got)
	require.Equal(t, "RS256", keys["rs"].alg)
	require.Equal(t, "ES256", keys["ec"].alg)
	require.Equal(t, "EdDSA", keys["ed"].alg)
}

func TestParseJWKSDuplicateKidKeepsTheFirst(t *testing.T) {
	a, b := newRSAKey(t, "same", 2048), newECKey(t, "same")
	keys, err := parseJWKS(jwksBody(t, a.jwk, b.jwk))
	require.NoError(t, err)
	require.Equal(t, "RS256", keys["same"].alg)
}

func TestParseJWKSRefusesUnusableSets(t *testing.T) {
	small := newRSAKey(t, "small", 1024)
	for name, body := range map[string][]byte{
		"not JSON":       []byte("<html>"),
		"keys missing":   []byte(`{}`),
		"keys not array": []byte(`{"keys":{}}`),
		"no usable key":  jwksBody(t, small.jwk),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseJWKS(body)
			require.Error(t, err)
		})
	}
}
