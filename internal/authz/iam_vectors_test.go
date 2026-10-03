package authz

import (
	"encoding/base64"
	"encoding/json"
	iamcontract "github.com/brickKit/contract-infra-iam"
	"io/fs"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The iam access-token vectors (contract-infra-iam, pinned module) end with the checks that need the
// bundle, in the order of TOKENS.md rc.2 and EVALUATION.md E2: stale, revoked grant, delegation, the act
// chain (agents, impersonation). Signature and claim checks are authn's; here every case that passes them is
// run through CheckToken with a bundle built from the case's context.
type iamVectorFile struct {
	Defaults iamContext `json:"defaults"`
	Cases    []struct {
		ID      string          `json:"id"`
		Name    string          `json:"name"`
		Token   string          `json:"token"`
		Context json.RawMessage `json:"context"`
		Expect  struct {
			Valid  bool   `json:"valid"`
			Reason string `json:"reason"`
			Rule   string `json:"rule"`
		} `json:"expect"`
	} `json:"cases"`
}

type iamContext struct {
	Capabilities  map[string]bool  `json:"capabilities"`
	StaleSince    map[string]int64 `json:"stale_since"`
	RevokedGrants map[string]int64 `json:"revoked_grants"`
}

func TestIAMAccessTokenVectorsBundleChecks(t *testing.T) {
	raw, err := fs.ReadFile(iamcontract.FS, "vectors/tokens/access-token.json")
	require.NoError(t, err)
	var f iamVectorFile
	require.NoError(t, json.Unmarshal(raw, &f))
	ran := 0
	for _, c := range f.Cases {
		bundleRule := slices.Contains([]string{"delegation", "agents", "impersonation", "stale", "revoked_grant"}, c.Expect.Rule)
		if !c.Expect.Valid && !bundleRule {
			continue // decided by authn before the bundle is consulted
		}
		ran++
		t.Run(c.ID, func(t *testing.T) {
			ctx := iamContext{Capabilities: maps.Clone(f.Defaults.Capabilities), StaleSince: maps.Clone(f.Defaults.StaleSince),
				RevokedGrants: maps.Clone(f.Defaults.RevokedGrants)} // json.Unmarshal writes into shared maps
			require.NoError(t, json.Unmarshal(c.Context, &ctx))
			b := iamBundle(t, ctx)
			want := ""
			if !c.Expect.Valid {
				want = c.Expect.Reason
			}
			require.Equal(t, want, CheckToken(b, iamToken(t, c.Token)), c.Name)
		})
	}
	require.Equal(t, 24, ran) // valid cases + those whose failing rule needs the bundle
}

func iamBundle(t *testing.T, ctx iamContext) *Bundle {
	t.Helper()
	doc := map[string]any{
		"contract": "authz/2.0", "revision": "1", "roles": map[string]any{}, "grants": map[string]any{},
		"capabilities": map[string]any{"core": true, "delegation": ctx.Capabilities["delegation"],
			"agents": ctx.Capabilities["agents"], "impersonation": ctx.Capabilities["impersonation"]},
		"stale_since": ctx.StaleSince, "revoked_grants": ctx.RevokedGrants,
	}
	raw, err := json.Marshal(doc)
	require.NoError(t, err)
	b, err := ParseBundle(raw)
	require.NoError(t, err)
	return b
}

// iamToken reads the payload of a vector token without verifying it (authn's job) into a Token.
func iamToken(t *testing.T, compact string) Token {
	t.Helper()
	parts := strings.Split(compact, ".")
	require.Len(t, parts, 3)
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)
	var c struct {
		Sub   string   `json:"sub"`
		Iat   int64    `json:"iat"`
		Roles []string `json:"roles"`
		Act   *Act     `json:"act"`
		Ceil  []string `json:"ceil"`
		DG    string   `json:"dg"`
	}
	require.NoError(t, json.Unmarshal(payload, &c))
	return Token{Sub: c.Sub, IssuedAt: time.Unix(c.Iat, 0), Roles: c.Roles, Act: c.Act, Ceil: c.Ceil, DG: c.DG}
}
