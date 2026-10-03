package authz

import (
	"errors"
	"io/fs"
	"testing"

	authzcontract "github.com/brickKit/contract-infra-authz/v2"
	"github.com/stretchr/testify/require"
)

// loadExample reads examples/bundle.example.json from the pinned family contract module.
func loadExample(t *testing.T) []byte {
	t.Helper()
	raw, err := fs.ReadFile(authzcontract.FS, "examples/bundle.example.json")
	require.NoError(t, err)
	return raw
}

func TestParseBundleExample(t *testing.T) {
	b, err := ParseBundle(loadExample(t))
	require.NoError(t, err)
	require.Equal(t, "authz/2.0", b.Contract)
	require.Equal(t, "18234", b.Revision)
	require.True(t, b.Capabilities.Delegation)
	require.False(t, b.Capabilities.Agents)
	require.False(t, b.Capabilities.Impersonation)
	require.True(t, b.Capabilities.Has("sharing"))
	require.True(t, b.Capabilities.Has("list_objects"))
	require.False(t, b.Capabilities.Has("graph"))
	require.False(t, b.Capabilities.Has("no_such_capability"))
	require.Equal(t, []string{"erp.sales.view", "erp.sales.cancel", "erp.sales.pricing.read"}, b.Roles["dev_sales_rep"])
	require.Equal(t, "subtree", b.Grants["dev_sales_rep"].Levels["erp.sales.view"])
	require.Equal(t, "all", b.Grants["superuser"].DefaultLevel)
	require.Equal(t, []string{"/1/3/", "/1/7/"}, b.Grants["dev_east_mgr"].Values["org"])
	require.NotNil(t, b.Grants["dev_wh_south"].Until)
	require.Equal(t, int64(1760000000), *b.Grants["dev_wh_south"].Until)
	require.Nil(t, b.Grants["dev_wh_south"].FromTS)
	require.Equal(t, "all", b.Profiles["view_as_ro"].MaxLevel)
	require.Equal(t, []string{"viewer"}, b.Profiles["view_as_ro"].Relations)
	require.Len(t, b.Delegations, 1)
	require.Equal(t, Delegation{
		ID: "dg_91", Mode: "on_behalf", From: "u_A", To: "u_B",
		Keys:   []string{"infra.workflow.task.view", "infra.workflow.task.act"},
		FromTS: ptr(int64(1759276800)), Until: ptr(int64(1759881600)),
	}, b.Delegations[0])
	require.Equal(t, int64(1759400000), b.StaleSince["u_123"])
	require.Equal(t, int64(1759401000), b.RevokedGrants["dg_77"])
	require.JSONEq(t, string(loadExample(t)), string(b.Raw))
}

func TestParseBundleRefusesContract(t *testing.T) {
	for name, contract := range map[string]string{
		"missing":       ``,
		"v1":            `"contract":"authz/1.0",`,
		"v3":            `"contract":"authz/3.0",`,
		"leading zero":  `"contract":"authz/2.01",`,
		"no minor":      `"contract":"authz/2",`,
		"trailing text": `"contract":"authz/2.0\n",`,
		"not a string":  `"contract":2,`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseBundle([]byte(`{` + contract + `"revision":"1","capabilities":{"core":true},"roles":{},"grants":{},"stale_since":{}}`))
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrBundleRefused), "got %v", err)
		})
	}
}

func TestParseBundleAcceptsLaterMinorAndUnknownMembers(t *testing.T) {
	b, err := ParseBundle([]byte(`{"contract":"authz/2.17","revision":"9","future":{"x":1},
		"capabilities":{"core":true,"future_cap":true,"delegation":true},"roles":{},"grants":{},"stale_since":{}}`))
	require.NoError(t, err)
	require.Equal(t, "authz/2.17", b.Contract)
	require.True(t, b.Capabilities.Delegation)
	require.True(t, b.Capabilities.Has("future_cap"))
}

func TestParseBundleRefusesWrongTypes(t *testing.T) {
	for name, body := range map[string]string{
		"not an object":         `[]`,
		"capability not a bool": `{"contract":"authz/2.0","capabilities":{"core":true,"delegation":"yes"},"roles":{},"grants":{}}`,
		"roles not arrays":      `{"contract":"authz/2.0","capabilities":{"core":true},"roles":{"r":"k.x"},"grants":{}}`,
		"stale not integers":    `{"contract":"authz/2.0","capabilities":{"core":true},"roles":{},"grants":{},"stale_since":{"u":"1"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseBundle([]byte(body))
			require.ErrorIs(t, err, ErrBundleRefused)
		})
	}
}

func ptr[T any](v T) *T { return &v }
