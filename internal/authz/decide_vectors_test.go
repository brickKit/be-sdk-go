package authz

import (
	"encoding/json"
	"io/fs"
	"sort"
	"testing"
	"time"

	authzcontract "github.com/brickKit/contract-infra-authz/v2"
	"github.com/stretchr/testify/require"
)

// The decision vectors are read from the pinned family contract module
// github.com/brickKit/contract-infra-authz/v2 (go.mod), vectors/decision/*.json. Every member of
// `expected` is computed and compared exactly (EVALUATION.md "Vector file format"): bundle, token,
// has_key, level, scope_params, degraded, fields, decision and explain. A member the vector leaves out
// (after a refusal or a token failure, fields for a type without field sets, decision and explain
// without a row) must not be produced either.
type decisionVector struct {
	ID    string `json:"id"`
	Input struct {
		Bundle json.RawMessage `json:"bundle"`
		Claims struct {
			Sub      string   `json:"sub"`
			Iat      int64    `json:"iat"`
			Roles    []string `json:"roles"`
			DeptPath string   `json:"dept_path"`
			Act      *Act     `json:"act"`
			Ceil     []string `json:"ceil"`
			DG       string   `json:"dg"`
		} `json:"claims"`
		Now          int64           `json:"now"`
		Key          string          `json:"key"`
		ResourceType json.RawMessage `json:"resource_type"`
		Row          *struct {
			ID       string            `json:"id"`
			Owner    string            `json:"owner"`
			DeptPath string            `json:"dept_path"`
			Values   map[string]string `json:"values"`
		} `json:"row"`
		ACL []struct {
			RType     string     `json:"rtype"`
			RID       string     `json:"rid"`
			Relation  string     `json:"relation"`
			Subject   string     `json:"subject"`
			ExpiresAt *time.Time `json:"expires_at"`
		} `json:"acl"`
		GraphIDs []string `json:"graph_ids"`
	} `json:"input"`
	Expected map[string]json.RawMessage `json:"expected"`
}

func loadDecisionVectors(t *testing.T) []decisionVector {
	t.Helper()
	files, err := fs.Glob(authzcontract.FS, "vectors/decision/*.json")
	require.NoError(t, err)
	sort.Strings(files)
	require.Len(t, files, 62, "the pinned vector set changed size")
	out := make([]decisionVector, 0, len(files))
	for _, f := range files {
		raw, err := fs.ReadFile(authzcontract.FS, f)
		require.NoError(t, err)
		var v decisionVector
		require.NoError(t, json.Unmarshal(raw, &v), f)
		out = append(out, v)
	}
	return out
}

// computeVector evaluates one vector and returns every member it produces, as JSON.
func computeVector(t *testing.T, v decisionVector) map[string]any {
	t.Helper()
	got := map[string]any{}
	b, err := ParseBundle(v.Input.Bundle)
	if err != nil {
		require.ErrorIs(t, err, ErrBundleRefused)
		got["bundle"] = "refused"
		return got
	}
	got["bundle"] = "accepted"
	c := v.Input.Claims
	tok := Token{Sub: c.Sub, IssuedAt: time.Unix(c.Iat, 0), Roles: c.Roles, DeptPath: c.DeptPath, Act: c.Act, Ceil: c.Ceil, DG: c.DG}
	now := time.Unix(v.Input.Now, 0)
	if reason := CheckToken(b, tok); reason != "" {
		got["token"] = reason
		return got
	}
	got["token"] = "OK"
	rt, err := ParseResourceType(v.Input.ResourceType)
	require.NoError(t, err)
	graph := func(string) []string { return v.Input.GraphIDs }
	e := NewEvaluator(b, tok, now)
	ka := e.Key(rt, v.Input.Key, graph)
	got["has_key"] = ka.Has
	got["level"] = ka.Level.String()
	got["scope_params"] = ka.Params
	got["degraded"] = ka.Degraded
	if len(rt.Fields) > 0 {
		got["fields"] = e.Fields(rt)
	}
	if v.Input.Row == nil {
		return got
	}
	row := Row{ID: v.Input.Row.ID, Owner: v.Input.Row.Owner, DeptPath: v.Input.Row.DeptPath, Values: v.Input.Row.Values}
	acl := make([]ACLRow, 0, len(v.Input.ACL))
	for _, a := range v.Input.ACL {
		acl = append(acl, ACLRow{RType: a.RType, RID: a.RID, Relation: a.Relation, Subject: a.Subject, ExpiresAt: a.ExpiresAt})
	}
	got["decision"] = e.Decide(rt, v.Input.Key, row, acl, graph)
	got["explain"] = e.Explain(rt, v.Input.Key, row, acl, graph)
	return got
}

func TestDecisionVectors(t *testing.T) {
	members := 0
	for _, v := range loadDecisionVectors(t) {
		t.Run(v.ID, func(t *testing.T) {
			got := computeVector(t, v)
			want := make([]string, 0, len(v.Expected))
			for k := range v.Expected {
				want = append(want, k)
			}
			have := make([]string, 0, len(got))
			for k := range got {
				have = append(have, k)
			}
			sort.Strings(want)
			sort.Strings(have)
			require.Equal(t, want, have, "the members of expected")
			for _, k := range want {
				raw, err := json.Marshal(got[k])
				require.NoError(t, err)
				require.JSONEq(t, string(v.Expected[k]), string(raw), "expected.%s", k)
				members++
			}
		})
	}
	t.Logf("compared %d expected members", members)
}
