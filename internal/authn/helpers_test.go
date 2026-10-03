package authn

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

const (
	testIssuer   = "urn:be:t-test:iam"
	testAudience = "t-test"
)

// testKey is a signing key with its public JWK.
type testKey struct {
	kid    string
	alg    string
	method jwt.SigningMethod
	priv   crypto.Signer
	jwk    map[string]any
}

var b64 = base64.RawURLEncoding

func newRSAKey(t *testing.T, kid string, bits int) testKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, bits)
	require.NoError(t, err)
	return testKey{kid: kid, alg: "RS256", method: jwt.SigningMethodRS256, priv: k, jwk: map[string]any{
		"kty": "RSA", "kid": kid, "alg": "RS256", "use": "sig",
		"n": b64.EncodeToString(k.N.Bytes()), "e": b64.EncodeToString(big.NewInt(int64(k.E)).Bytes()),
	}}
}

func newECKey(t *testing.T, kid string) testKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	raw, err := k.PublicKey.Bytes() // 0x04 || X || Y
	require.NoError(t, err)
	return testKey{kid: kid, alg: "ES256", method: jwt.SigningMethodES256, priv: k, jwk: map[string]any{
		"kty": "EC", "kid": kid, "alg": "ES256", "use": "sig", "crv": "P-256",
		"x": b64.EncodeToString(raw[1:33]), "y": b64.EncodeToString(raw[33:]),
	}}
}

func newEdKey(t *testing.T, kid string) testKey {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return testKey{kid: kid, alg: "EdDSA", method: jwt.SigningMethodEdDSA, priv: priv, jwk: map[string]any{
		"kty": "OKP", "kid": kid, "alg": "EdDSA", "use": "sig", "crv": "Ed25519", "x": b64.EncodeToString(pub),
	}}
}

// sign builds a compact JWS over the given header and payload (any JSON, including wrong types).
func sign(t *testing.T, k testKey, header map[string]any, payload any) string {
	t.Helper()
	h, err := json.Marshal(header)
	require.NoError(t, err)
	p, err := json.Marshal(payload)
	require.NoError(t, err)
	signing := b64.EncodeToString(h) + "." + b64.EncodeToString(p)
	sig, err := k.method.Sign(signing, k.priv)
	require.NoError(t, err)
	return signing + "." + b64.EncodeToString(sig)
}

// testNow is the fixed clock of the unit tests.
var testNow = time.Unix(1790000000, 0)

// claims returns a valid access-token payload at testNow; mut edits it.
func claims(mut func(c map[string]any)) map[string]any {
	c := map[string]any{
		"iss": testIssuer, "aud": []any{testAudience}, "sub": "u-1", "typ": "access",
		"iat": testNow.Unix() - 30, "nbf": testNow.Unix() - 30, "exp": testNow.Unix() + 570,
		"jti": "j-1", "tenant_id": testAudience, "roles": []any{"sales.rep"}, "dept_path": "/1/",
		"azp": "pc", "locale": "zh-CN",
	}
	if mut != nil {
		mut(c)
	}
	return c
}

// token signs claims(mut) with k and the standard header.
func token(t *testing.T, k testKey, mut func(c map[string]any)) string {
	t.Helper()
	return sign(t, k, map[string]any{"alg": k.alg, "kid": k.kid, "typ": "JWT"}, claims(mut))
}

// jwksServer serves a mutable JWKS and counts fetches.
type jwksServer struct {
	mu      sync.Mutex
	keys    []map[string]any
	status  int
	hang    chan struct{} // when non-nil, requests block until it closes or the client gives up
	fetches atomic.Int32
	times   []time.Time // guarded by mu
	srv     *httptest.Server
}

func newJWKSServer(t *testing.T, keys ...testKey) *jwksServer {
	s := &jwksServer{status: http.StatusOK}
	s.set(keys...)
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.fetches.Add(1)
		s.mu.Lock()
		s.times = append(s.times, time.Now())
		status, hang := s.status, s.hang
		body, _ := json.Marshal(map[string]any{"keys": s.keys})
		s.mu.Unlock()
		if hang != nil {
			select {
			case <-hang:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *jwksServer) set(keys ...testKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = nil
	for _, k := range keys {
		s.keys = append(s.keys, k.jwk)
	}
}

func (s *jwksServer) setRaw(keys ...map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = keys
}

func (s *jwksServer) setStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
}

func (s *jwksServer) setHang(ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hang = ch
}

func (s *jwksServer) fetchTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.times...)
}

func (s *jwksServer) url() string { return s.srv.URL + "/.well-known/jwks.json" }

// fakeClock is a settable clock for Config.Now.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock(t time.Time) *fakeClock {
	c := &fakeClock{}
	c.ns.Store(t.UnixNano())
	return c
}

func (c *fakeClock) now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// newTestVerifier builds a verifier on s with the fixed test clock.
func newTestVerifier(s *jwksServer, clock *fakeClock) *Verifier {
	return NewVerifier(Config{JWKSURL: s.url(), Issuer: testIssuer, Audience: testAudience, Now: clock.now})
}

// requireInvalid asserts err is TOKEN_INVALID for rule.
func requireInvalid(t *testing.T, err error, rule string) {
	t.Helper()
	require.ErrorIs(t, err, ErrTokenInvalid)
	var ie *InvalidError
	require.ErrorAs(t, err, &ie)
	require.Equal(t, rule, ie.Rule, "detail: %s", ie.Detail)
}

func bg() context.Context { return context.Background() }
