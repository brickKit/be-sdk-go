package authz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testdata/decision/*.json is a verbatim copy of vectors/decision/ from
// github.com/brickKit/contract-infra-authz at tag v2.0.0-rc.1 (commit b2e1a8e). This wave computes the
// members E1–E5 decide: expected.bundle, expected.token and expected.has_key. The other members
// (level, scope_params, fields, decision, explain) belong to the levels/dimensions wave.
type decisionVector struct {
	ID    string `json:"id"`
	Input struct {
		Bundle json.RawMessage `json:"bundle"`
		Claims struct {
			Sub   string   `json:"sub"`
			Iat   int64    `json:"iat"`
			Roles []string `json:"roles"`
			Act   *Act     `json:"act"`
			Ceil  []string `json:"ceil"`
			DG    string   `json:"dg"`
		} `json:"claims"`
		Now int64  `json:"now"`
		Key string `json:"key"`
	} `json:"input"`
	Expected struct {
		Bundle string `json:"bundle"`
		Token  string `json:"token"`
		HasKey *bool  `json:"has_key"`
	} `json:"expected"`
}

func loadDecisionVectors(t *testing.T) []decisionVector {
	t.Helper()
	files, err := filepath.Glob("testdata/decision/*.json")
	require.NoError(t, err)
	sort.Strings(files)
	require.Len(t, files, 62, "the copied vector set changed size")
	out := make([]decisionVector, 0, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		require.NoError(t, err)
		var v decisionVector
		require.NoError(t, json.Unmarshal(raw, &v), f)
		out = append(out, v)
	}
	return out
}

func TestDecisionVectorsE1toE5(t *testing.T) {
	for _, v := range loadDecisionVectors(t) {
		t.Run(v.ID, func(t *testing.T) {
			b, err := ParseBundle(v.Input.Bundle)
			if v.Expected.Bundle == "refused" {
				require.ErrorIs(t, err, ErrBundleRefused)
				return
			}
			require.Equal(t, "accepted", v.Expected.Bundle)
			require.NoError(t, err)
			c := v.Input.Claims
			tok := Token{Sub: c.Sub, IssuedAt: time.Unix(c.Iat, 0), Roles: c.Roles, Act: c.Act, Ceil: c.Ceil, DG: c.DG}
			now := time.Unix(v.Input.Now, 0)

			want := v.Expected.Token
			if want == "OK" {
				want = ""
			}
			require.Equal(t, want, CheckToken(b, tok), "E2 token check")

			d := Decide(b, tok, v.Input.Key, now)
			if v.Expected.Token != "OK" {
				require.Equal(t, Decision{Allow: false, Reason: v.Expected.Token}, d)
				return
			}
			require.NotNil(t, v.Expected.HasKey)
			require.Equal(t, *v.Expected.HasKey, HasKey(b, tok, v.Input.Key, now), "E5 has(K)")
			if *v.Expected.HasKey {
				require.Equal(t, Decision{Allow: true}, d)
			} else {
				require.Equal(t, Decision{Allow: false, Reason: ReasonMissingPermission}, d)
			}
		})
	}
}
