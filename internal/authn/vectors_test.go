package authn

import (
	"encoding/json"
	iamcontract "github.com/brickKit/contract-infra-iam"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// vectors/tokens/access-token.json of the pinned contract-infra-iam module (v1.0.0-rc.2, 56 cases)
// is the component-side verification of TOKENS.md. The signature and claim checks are this package's;
// the checks that need the bundle (stale, revoked grant, delegation, the act chain) run in
// internal/authz (iam_vectors_test.go there). For a case whose failing rule is one of those, the expectation here is
// that the token verifies.
type iamVectors struct {
	Defaults struct {
		Now      int64           `json:"now"`
		Issuer   string          `json:"issuer"`
		TenantID string          `json:"tenant_id"`
		JWKS     json.RawMessage `json:"jwks"`
	} `json:"defaults"`
	Cases []struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Token   string `json:"token"`
		Context struct {
			Issuer string `json:"issuer"`
		} `json:"context"`
		Expect struct {
			Valid  bool   `json:"valid"`
			Status int    `json:"status"`
			Reason string `json:"reason"`
			Domain string `json:"domain"`
			Rule   string `json:"rule"`
			Sub    string `json:"sub"`
			ActSub string `json:"act_sub"`
		} `json:"expect"`
	} `json:"cases"`
}

func TestIAMAccessTokenVectors(t *testing.T) {
	raw, err := fs.ReadFile(iamcontract.FS, "vectors/tokens/access-token.json")
	require.NoError(t, err)
	var f iamVectors
	require.NoError(t, json.Unmarshal(raw, &f))
	require.Len(t, f.Cases, 56)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(f.Defaults.JWKS)
	}))
	t.Cleanup(srv.Close)
	now := time.Unix(f.Defaults.Now, 0)

	for _, c := range f.Cases {
		t.Run(c.ID, func(t *testing.T) {
			issuer := f.Defaults.Issuer
			if c.Context.Issuer != "" {
				issuer = c.Context.Issuer
			}
			v := NewVerifier(Config{JWKSURL: srv.URL, Issuer: issuer, Audience: f.Defaults.TenantID,
				Now: func() time.Time { return now }})
			claims, err := v.Verify(bg(), "Bearer "+c.Token)

			bundleRule := slices.Contains([]string{"delegation", "agents", "impersonation", "stale", "revoked_grant"}, c.Expect.Rule)
			if c.Expect.Valid || bundleRule {
				require.NoError(t, err, c.Name)
				if c.Expect.Sub != "" {
					require.Equal(t, c.Expect.Sub, claims.Sub)
				}
				if c.Expect.ActSub != "" {
					require.Equal(t, c.Expect.ActSub, claims.Act.Sub)
				}
				return
			}
			require.Equal(t, "TOKEN_INVALID", c.Expect.Reason)
			require.Equal(t, 401, c.Expect.Status)
			require.Equal(t, "be", c.Expect.Domain)
			requireInvalid(t, err, c.Expect.Rule)
		})
	}
}
