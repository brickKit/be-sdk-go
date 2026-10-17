package identity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixture struct {
	JWKS     json.RawMessage
	Issuer   string
	Audience string
	Now      int64
}

func loadJWT(t *testing.T) (fixture, []json.RawMessage) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/jwt-verify.json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	var f fixture
	require.NoError(t, json.Unmarshal(v.Cases[0], &f))
	return f, v.Cases[1:]
}

func jwksServer(t *testing.T, body json.RawMessage, hits *atomic.Int32) *httptest.Server {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestJWTVerifyVectors(t *testing.T) {
	f, cases := loadJWT(t)
	var hits atomic.Int32
	keys, err := NewKeySet(context.Background(), jwksServer(t, f.JWKS, &hits).URL)
	require.NoError(t, err)
	v := &Verifier{Issuer: f.Issuer, Audience: f.Audience, Keys: keys, Now: func() time.Time { return time.Unix(f.Now, 0) }}
	for _, raw := range cases {
		var c struct {
			ID, Token string
			Expect    struct {
				Valid   bool
				Problem string
			}
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		keys.lastUnknown = time.Time{} // each case may refresh once
		u, err := v.Verify(c.Token)
		if c.Expect.Valid {
			require.NoError(t, err, c.ID)
			assert.Equal(t, "0190f3a1-0000-7000-8000-0000000000a1", u.Subject, c.ID)
			continue
		}
		var te *TokenError
		require.True(t, errors.As(err, &te), "%s: %v", c.ID, err)
		assert.Equal(t, c.Expect.Problem, string(te.Problem), c.ID)
		assert.True(t, errs.Is(Refusal(err), "be", errs.ReasonTokenInvalid), c.ID)
		assert.Equal(t, "the credential is missing or not valid", Refusal(err).(*errs.Error).Detail, "the reason stays out of the answer")
	}
}

func TestUnknownKIDRefreshIsRateLimited(t *testing.T) { // CK-ID-01
	f, cases := loadJWT(t)
	var unknown string
	for _, raw := range cases {
		var c struct{ ID, Token string }
		_ = json.Unmarshal(raw, &c)
		if c.ID == "kid-unknown" {
			unknown = c.Token
		}
	}
	var hits atomic.Int32
	keys, err := NewKeySet(context.Background(), jwksServer(t, f.JWKS, &hits).URL)
	require.NoError(t, err)
	v := &Verifier{Issuer: f.Issuer, Audience: f.Audience, Keys: keys, Now: func() time.Time { return time.Unix(f.Now, 0) }}
	for i := 0; i < 5; i++ {
		_, _ = v.Verify(unknown)
	}
	assert.Equal(t, int32(2), hits.Load(), "the first fetch and one refresh within 30 s")
}

func TestStaleKeysAreNotUsed(t *testing.T) { // P8.2
	f, cases := loadJWT(t)
	var good string
	_ = json.Unmarshal(cases[2], &struct{ Token *string }{&good})
	var hits atomic.Int32
	s := jwksServer(t, f.JWKS, &hits)
	keys, err := NewKeySet(context.Background(), s.URL)
	require.NoError(t, err)
	v := &Verifier{Issuer: f.Issuer, Audience: f.Audience, Keys: keys, Now: func() time.Time { return time.Unix(f.Now, 0) }}
	_, err = v.Verify(good)
	require.NoError(t, err)
	s.Close() // the identity member is gone
	keys.now = func() time.Time { return time.Now().Add(KeyMaxAge + time.Minute) }
	_, err = v.Verify(good)
	assert.True(t, errs.Is(err, "be", errs.ReasonDependencyUnavailable), "%v", err)
}

func validToken(t *testing.T, cases []json.RawMessage, id string) string {
	for _, raw := range cases {
		var c struct{ ID, Token string }
		require.NoError(t, json.Unmarshal(raw, &c))
		if c.ID == id {
			return c.Token
		}
	}
	t.Fatalf("no case %s", id)
	return ""
}

func TestKeysThisCodeDoesNotUnderstandAreSkipped(t *testing.T) { // P8.2, RFC 7517 section 5
	f, cases := loadJWT(t)
	var set struct{ Keys []json.RawMessage }
	require.NoError(t, json.Unmarshal(f.JWKS, &set))
	odd := append([]json.RawMessage{json.RawMessage(`{"kty":"XYZ","kid":"future-1"}`),
		json.RawMessage(`{"kty":"RSA","kid":"broken"}`)}, set.Keys...)
	body, _ := json.Marshal(map[string]any{"keys": odd})
	var hits atomic.Int32
	keys, err := NewKeySet(context.Background(), jwksServer(t, body, &hits).URL)
	require.NoError(t, err, "one key of a new type does not make the set unusable")
	v := &Verifier{Issuer: f.Issuer, Audience: f.Audience, Keys: keys, Now: func() time.Time { return time.Unix(f.Now, 0) }}
	_, err = v.Verify(validToken(t, cases, "valid-es256"))
	assert.NoError(t, err)
}

func TestHeaderNamesAreExact(t *testing.T) { // RFC 7515: "TYP" is not "typ"
	f, cases := loadJWT(t)
	tok := validToken(t, cases, "valid-eddsa")
	parts := strings.Split(tok, ".")
	raw, _ := base64.RawURLEncoding.DecodeString(parts[0])
	upper := strings.Replace(string(raw), `"typ"`, `"TYP"`, 1)
	parts[0] = base64.RawURLEncoding.EncodeToString([]byte(upper))
	var hits atomic.Int32
	keys, err := NewKeySet(context.Background(), jwksServer(t, f.JWKS, &hits).URL)
	require.NoError(t, err)
	v := &Verifier{Issuer: f.Issuer, Audience: f.Audience, Keys: keys, Now: func() time.Time { return time.Unix(f.Now, 0) }}
	_, err = v.Verify(strings.Join(parts, "."))
	var te *TokenError
	require.True(t, errors.As(err, &te))
	assert.Equal(t, Problem("typ"), te.Problem)
}
