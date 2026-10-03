package authz

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The iam access-token vectors (copied once, into ../authn/testdata; source and tag are named there)
// end with two checks that need the bundle: delegation against the capabilities (TOKENS.md check 13)
// and stale (check 14). Signature and claim checks are authn's; here every case that passes them is
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
	Capabilities map[string]bool  `json:"capabilities"`
	StaleSince   map[string]int64 `json:"stale_since"`
}

// iamConflicts are the cases where TOKENS.md (iam/1.0-rc.1) and EVALUATION.md E2 (authz/2.0-rc.1)
// disagree; Decide follows E2 and be-protocol P6.2. Each is asserted against E2's outcome.
var iamConflicts = map[string]string{
	// E2 step 4: an act of kind user needs the capability impersonation, which the case's context
	// does not list (vector token-impersonation-not-offered). TOKENS.md check 13 only names agents
	// and delegation.
	"AT-040": ReasonUnsupportedDelegation,
	// E2 checks stale (step 1) before delegation (steps 3–4), as P6.2 orders the stale check before
	// "delegation and ceilings"; TOKENS.md puts delegation (13) before stale (14).
	"AT-050": ReasonTokenStale,
}

func TestIAMAccessTokenVectorsBundleChecks(t *testing.T) {
	raw, err := os.ReadFile("../authn/testdata/iam-access-token.json")
	require.NoError(t, err)
	var f iamVectorFile
	require.NoError(t, json.Unmarshal(raw, &f))
	ran := 0
	for _, c := range f.Cases {
		bundleRule := c.Expect.Rule == "delegation" || c.Expect.Rule == "agents" || c.Expect.Rule == "stale"
		if !c.Expect.Valid && !bundleRule {
			continue // decided by authn before the bundle is consulted
		}
		ran++
		t.Run(c.ID, func(t *testing.T) {
			ctx := f.Defaults
			require.NoError(t, json.Unmarshal(c.Context, &ctx))
			b := iamBundle(t, ctx)
			want := ""
			if !c.Expect.Valid {
				want = c.Expect.Reason
			}
			if conflict, ok := iamConflicts[c.ID]; ok {
				want = conflict
			}
			require.Equal(t, want, CheckToken(b, iamToken(t, c.Token)), c.Name)
		})
	}
	require.Equal(t, 19, ran) // 14 valid cases + 5 whose failing rule is delegation, agents or stale
}

func iamBundle(t *testing.T, ctx iamContext) *Bundle {
	t.Helper()
	doc := map[string]any{
		"contract": "authz/2.0", "revision": "1", "roles": map[string]any{}, "grants": map[string]any{},
		"capabilities": map[string]any{"core": true, "delegation": ctx.Capabilities["delegation"], "agents": ctx.Capabilities["agents"]},
		"stale_since":  ctx.StaleSince,
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
