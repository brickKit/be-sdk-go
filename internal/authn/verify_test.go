package authn

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func TestVerifyValidTokenEveryAlgorithm(t *testing.T) {
	rs, ec, ed := newRSAKey(t, "rs", 2048), newECKey(t, "ec"), newEdKey(t, "ed")
	v := newTestVerifier(newJWKSServer(t, rs, ec, ed), newFakeClock(testNow))
	for _, k := range []testKey{rs, ec, ed} {
		t.Run(k.alg, func(t *testing.T) {
			c, err := v.Verify(bg(), "Bearer "+token(t, k, nil))
			require.NoError(t, err)
			require.Equal(t, &Claims{
				Sub: "u-1", TenantID: testAudience, DeptPath: "/1/", AZP: "pc", Locale: "zh-CN", JTI: "j-1",
				Roles: []string{"sales.rep"}, IssuedAt: testNow.Add(-30 * time.Second), ExpiresAt: testNow.Add(570 * time.Second),
			}, c)
		})
	}
}

func TestVerifyReadsDelegationClaims(t *testing.T) {
	rs := newRSAKey(t, "rs", 2048)
	v := newTestVerifier(newJWKSServer(t, rs), newFakeClock(testNow))
	c, err := v.Verify(bg(), "Bearer "+token(t, rs, func(c map[string]any) {
		c["act"] = map[string]any{"sub": "u-admin", "kind": "user", "act": map[string]any{"sub": "svc:edi", "kind": "svc"}}
		c["ceil"] = []any{"view_as_ro"}
		c["dg"] = "dg_5"
		c["org_id"] = "legacy"
		c["x_unknown"] = map[string]any{"a": 1}
	}))
	require.NoError(t, err)
	require.Equal(t, &Act{Sub: "u-admin", Kind: "user", Act: &Act{Sub: "svc:edi", Kind: "svc"}}, c.Act)
	require.Equal(t, []string{"view_as_ro"}, c.Ceil)
	require.Equal(t, "dg_5", c.DG)
	require.Equal(t, "legacy", c.OrgID)
}

// P5.1: the header form.
func TestVerifyAuthorizationHeaderForm(t *testing.T) {
	rs := newRSAKey(t, "rs", 2048)
	v := newTestVerifier(newJWKSServer(t, rs), newFakeClock(testNow))
	good := token(t, rs, nil)
	_, err := v.Verify(bg(), "bearer "+good)
	require.NoError(t, err, "the auth-scheme is case-insensitive (RFC 9110 §11.1)")
	parts := strings.Split(good, ".")
	for name, h := range map[string]string{
		"missing":          "",
		"another scheme":   "Basic dXNlcjpwYXNz",
		"no token":         "Bearer ",
		"two segments":     "Bearer " + parts[0] + "." + parts[1],
		"four segments":    "Bearer " + good + ".x",
		"padding":          "Bearer " + parts[0] + "=." + parts[1] + "." + parts[2],
		"header not JSON":  "Bearer " + b64.EncodeToString([]byte("nope")) + "." + parts[1] + "." + parts[2],
		"payload an array": "Bearer " + parts[0] + "." + b64.EncodeToString([]byte("[1]")) + "." + parts[2],
		"oversized":        "Bearer " + strings.Repeat("a", maxTokenBytes) + "." + parts[1] + "." + parts[2],
	} {
		t.Run(name, func(t *testing.T) {
			_, err := v.Verify(bg(), h)
			requireInvalid(t, err, RuleFormat)
		})
	}
}

// P5.2: algorithm allow-list, the key's alg, kid.
func TestVerifyAlgorithmRules(t *testing.T) {
	rs, ec := newRSAKey(t, "rs", 2048), newECKey(t, "ec")
	v := newTestVerifier(newJWKSServer(t, rs, ec), newFakeClock(testNow))

	t.Run("none", func(t *testing.T) {
		h := b64.EncodeToString([]byte(`{"alg":"none","kid":"rs"}`))
		p := b64.EncodeToString([]byte(`{"sub":"x"}`))
		_, err := v.Verify(bg(), "Bearer "+h+"."+p+".")
		requireInvalid(t, err, RuleAlg)
	})
	t.Run("HS256 keyed with the RSA public key", func(t *testing.T) {
		hs := testKey{kid: "rs", alg: "HS256", method: jwt.SigningMethodHS256}
		h := map[string]any{"alg": "HS256", "kid": "rs"}
		hb, pb := b64.EncodeToString(mustJSON(t, h)), b64.EncodeToString(mustJSON(t, claims(nil)))
		sig, err := hs.method.Sign(hb+"."+pb, []byte(rs.jwk["n"].(string)))
		require.NoError(t, err)
		_, err = v.Verify(bg(), "Bearer "+hb+"."+pb+"."+b64.EncodeToString(sig))
		requireInvalid(t, err, RuleAlg)
	})
	t.Run("PS256 is not in the allow-list", func(t *testing.T) {
		_, err := v.Verify(bg(), "Bearer "+sign(t, rs, map[string]any{"alg": "PS256", "kid": "rs"}, claims(nil)))
		requireInvalid(t, err, RuleAlg)
	})
	t.Run("header alg differs from the key's alg", func(t *testing.T) {
		_, err := v.Verify(bg(), "Bearer "+sign(t, rs, map[string]any{"alg": "RS256", "kid": "ec"}, claims(nil)))
		requireInvalid(t, err, RuleAlg)
	})
	t.Run("missing kid", func(t *testing.T) {
		_, err := v.Verify(bg(), "Bearer "+sign(t, rs, map[string]any{"alg": "RS256"}, claims(nil)))
		requireInvalid(t, err, RuleKid)
	})
	t.Run("kid not a string", func(t *testing.T) {
		_, err := v.Verify(bg(), "Bearer "+sign(t, rs, map[string]any{"alg": "RS256", "kid": 7}, claims(nil)))
		requireInvalid(t, err, RuleKid)
	})
	t.Run("crit header", func(t *testing.T) {
		_, err := v.Verify(bg(), "Bearer "+sign(t, rs, map[string]any{"alg": "RS256", "kid": "rs", "crit": []any{"b64"}, "b64": false}, claims(nil)))
		requireInvalid(t, err, RuleFormat)
	})
	t.Run("signature by another key under a known kid", func(t *testing.T) {
		other := newRSAKey(t, "rs", 2048)
		_, err := v.Verify(bg(), "Bearer "+token(t, other, nil))
		requireInvalid(t, err, RuleSignature)
	})
	t.Run("payload changed after signing", func(t *testing.T) {
		parts := strings.Split(token(t, rs, nil), ".")
		parts[1] = b64.EncodeToString(mustJSON(t, claims(func(c map[string]any) { c["roles"] = []any{"superuser"} })))
		_, err := v.Verify(bg(), "Bearer "+strings.Join(parts, "."))
		requireInvalid(t, err, RuleSignature)
	})
}

// P5.3 and P5.9: the claims.
func TestVerifyClaimRules(t *testing.T) {
	rs := newRSAKey(t, "rs", 2048)
	v := newTestVerifier(newJWKSServer(t, rs), newFakeClock(testNow))
	now := testNow.Unix()
	cases := []struct {
		name string
		mut  func(c map[string]any)
		rule string // "" means valid
	}{
		{"refresh token refused", func(c map[string]any) { c["typ"] = "refresh" }, RuleTyp},
		{"typ missing", func(c map[string]any) { delete(c, "typ") }, RuleTyp},
		{"typ another value", func(c map[string]any) { c["typ"] = "Bearer" }, RuleTyp},
		{"wrong iss", func(c map[string]any) { c["iss"] = "urn:be:other:iam" }, RuleIss},
		{"missing iss", func(c map[string]any) { delete(c, "iss") }, RuleIss},
		{"wrong aud", func(c map[string]any) { c["aud"] = []any{"t-other"} }, RuleAud},
		{"aud string", func(c map[string]any) { c["aud"] = testAudience }, ""},
		{"aud array with others", func(c map[string]any) { c["aud"] = []any{"other-api", testAudience} }, ""},
		{"aud array with a non-string", func(c map[string]any) { c["aud"] = []any{testAudience, 7} }, RuleAud},
		{"missing aud", func(c map[string]any) { delete(c, "aud") }, RuleAud},
		{"empty sub", func(c map[string]any) { c["sub"] = "" }, RuleSub},
		{"sub not a string", func(c map[string]any) { c["sub"] = 7 }, RuleSub},
		{"service account sub", func(c map[string]any) { c["sub"] = "svc:edi" }, ""},
		{"no exp", func(c map[string]any) { delete(c, "exp") }, RuleExp},
		{"exp a string", func(c map[string]any) { c["exp"] = "1790000570" }, RuleExp},
		{"exp a fraction", func(c map[string]any) { c["exp"] = 1790000570.5 }, RuleExp},
		{"expired beyond the skew", func(c map[string]any) { c["exp"] = now - 61 }, RuleExp},
		{"exactly at exp + skew", func(c map[string]any) { c["exp"] = now - 60 }, RuleExp},
		{"expired within the skew", func(c map[string]any) { c["exp"] = now - 59 }, ""},
		{"nbf future beyond the skew", func(c map[string]any) { c["nbf"] = now + 61 }, RuleNbf},
		{"nbf future within the skew", func(c map[string]any) { c["nbf"] = now + 60 }, ""},
		{"nbf absent", func(c map[string]any) { delete(c, "nbf") }, ""},
		{"nbf a string", func(c map[string]any) { c["nbf"] = "0" }, RuleNbf},
		{"iat future beyond the skew", func(c map[string]any) { c["iat"] = now + 61 }, RuleIat},
		{"iat missing", func(c map[string]any) { delete(c, "iat") }, RuleIat},
		{"jti missing", func(c map[string]any) { delete(c, "jti") }, RuleJti},
		{"jti empty", func(c map[string]any) { c["jti"] = "" }, RuleJti},
		{"roles a string", func(c map[string]any) { c["roles"] = "sales.rep" }, RuleClaimType},
		{"roles with a number", func(c map[string]any) { c["roles"] = []any{"a", 1} }, RuleClaimType},
		{"roles null", func(c map[string]any) { c["roles"] = nil }, RuleClaimType},
		{"ceil a string", func(c map[string]any) { c["ceil"] = "p" }, RuleClaimType},
		{"dept_path an array", func(c map[string]any) { c["dept_path"] = []any{"/1/"} }, RuleClaimType},
		{"tenant_id a number", func(c map[string]any) { c["tenant_id"] = 1 }, RuleClaimType},
		{"azp an object", func(c map[string]any) { c["azp"] = map[string]any{} }, RuleClaimType},
		{"locale a bool", func(c map[string]any) { c["locale"] = true }, RuleClaimType},
		{"dg a number", func(c map[string]any) { c["dg"] = 5 }, RuleClaimType},
		{"org_id a number", func(c map[string]any) { c["org_id"] = 5 }, RuleClaimType},
		{"act a string", func(c map[string]any) { c["act"] = "u" }, RuleClaimType},
		{"act without sub", func(c map[string]any) { c["act"] = map[string]any{"kind": "user"} }, RuleClaimType},
		{"act kind outside the enum", func(c map[string]any) { c["act"] = map[string]any{"sub": "x", "kind": "robot"} }, RuleClaimType},
		{"nested act invalid", func(c map[string]any) {
			c["act"] = map[string]any{"sub": "x", "kind": "user", "act": map[string]any{"sub": "", "kind": "svc"}}
		}, RuleClaimType},
		{"agent act is a valid shape", func(c map[string]any) { c["act"] = map[string]any{"sub": "bot", "kind": "agent"} }, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := v.Verify(bg(), "Bearer "+token(t, rs, c.mut))
			if c.rule == "" {
				require.NoError(t, err)
				return
			}
			requireInvalid(t, err, c.rule)
		})
	}
}

func TestVerifyRefusesWhenIssuerOrAudienceUnset(t *testing.T) {
	rs := newRSAKey(t, "rs", 2048)
	s := newJWKSServer(t, rs)
	tok := token(t, rs, func(c map[string]any) { c["iss"] = ""; c["aud"] = "" })
	_, err := NewVerifier(Config{JWKSURL: s.url(), Audience: testAudience, Now: newFakeClock(testNow).now}).Verify(bg(), "Bearer "+tok)
	requireInvalid(t, err, RuleIss)
	tok = token(t, rs, func(c map[string]any) { c["aud"] = "" })
	_, err = NewVerifier(Config{JWKSURL: s.url(), Issuer: testIssuer, Now: newFakeClock(testNow).now}).Verify(bg(), "Bearer "+tok)
	requireInvalid(t, err, RuleAud)
}

func TestInvalidErrorNeverCarriesTheToken(t *testing.T) {
	rs := newRSAKey(t, "rs", 2048)
	v := newTestVerifier(newJWKSServer(t, rs), newFakeClock(testNow))
	tok := token(t, rs, func(c map[string]any) { c["typ"] = "refresh" })
	_, err := v.Verify(bg(), "Bearer "+tok)
	require.Error(t, err)
	for _, part := range strings.Split(tok, ".") {
		require.NotContains(t, err.Error(), part)
	}
	require.True(t, errors.Is(err, ErrTokenInvalid))
	require.Equal(t, "TOKEN_INVALID", ErrTokenInvalid.Error())
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
