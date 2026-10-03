package authz

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const catalogJSON = `{
  "keys": [], "dimensions": [],
  "resource_types": [
    {"type": "prj.timesheet.sheet", "owner_component": "prj/timesheet", "view_key": "prj.timesheet.view",
     "dimensions": ["owner"], "relations": {}, "derivation": "direct",
     "inherits": [{"from": "prj.project.project", "via": "project_id", "relation": "member", "as": "viewer"}]},
    {"type": "erp.sales.order", "owner_component": "erp/sales", "view_key": "erp.sales.view",
     "dimensions": ["owner", "org", "warehouse", "legal_entity"], "derivation": "direct",
     "relations": {
       "viewer": {"grants": ["erp.sales.view"]},
       "editor": {"grants": ["erp.sales.ship"], "includes": ["viewer"]},
       "team":   {"grants": ["erp.sales.confirm"], "includes": ["editor"], "owned_by": "component"}
     },
     "fields": [{"set": "erp.sales.pricing", "columns": ["unit_price"], "read": "erp.sales.pricing.read"}],
     "future_member": {"ignored": true}}
  ]
}`

func TestParseCatalog(t *testing.T) {
	c, err := ParseCatalog([]byte(catalogJSON))
	require.NoError(t, err)
	require.Equal(t, []string{"erp.sales.order", "prj.timesheet.sheet"}, c.Types())
	require.Equal(t, []string{"erp.sales.order", "prj.project.project", "prj.timesheet.sheet"}, c.PulledTypes(), "own types plus inherited parents (P6.12)")

	rt, ok := c.Lookup("erp.sales.order")
	require.True(t, ok)
	require.Equal(t, "erp.sales.view", rt.ViewKey)
	require.Equal(t, []string{"legal_entity", "warehouse"}, rt.ResourceDimensions(), "owner and org are identity dimensions (E7)")
	require.True(t, rt.HasDimension(DimOrg))
	require.Equal(t, []string{"editor", "team", "viewer"}, rt.RelationsGiving("erp.sales.view"), "includes are transitive (E8)")
	require.Equal(t, []string{"editor", "team"}, rt.RelationsGiving("erp.sales.ship"))
	require.Equal(t, []string{"team"}, rt.RelationsGiving("erp.sales.confirm"))
	require.Empty(t, rt.RelationsGiving("erp.sales.cancel"))
	_, ok = c.Lookup("nope.nope.nope")
	require.False(t, ok)
}

func TestParseCatalogRejectsBadDeclarations(t *testing.T) {
	for name, raw := range map[string]string{
		"not json":           `{`,
		"bad type name":      `{"resource_types":[{"type":"sales","owner_component":"a/b","view_key":"a.b","relations":{},"derivation":"direct"}]}`,
		"no view key":        `{"resource_types":[{"type":"a.b.c","owner_component":"a/b","relations":{},"derivation":"direct"}]}`,
		"unknown derivation": `{"resource_types":[{"type":"a.b.c","owner_component":"a/b","view_key":"a.b","relations":{},"derivation":"magic"}]}`,
		"unknown include":    `{"resource_types":[{"type":"a.b.c","owner_component":"a/b","view_key":"a.b","relations":{"x":{"includes":["y"]}},"derivation":"direct"}]}`,
		"bad owned_by":       `{"resource_types":[{"type":"a.b.c","owner_component":"a/b","view_key":"a.b","relations":{"x":{"owned_by":"me"}},"derivation":"direct"}]}`,
		"duplicate type":     `{"resource_types":[{"type":"a.b.c","owner_component":"a/b","view_key":"a.b","relations":{},"derivation":"direct"},{"type":"a.b.c","owner_component":"a/b","view_key":"a.b","relations":{},"derivation":"direct"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseCatalog([]byte(raw))
			require.ErrorIs(t, err, ErrCatalogInvalid)
		})
	}
}

func TestParseCatalogIncludeCycleTerminates(t *testing.T) {
	rt, err := ParseResourceType([]byte(`{"type":"a.b.c","owner_component":"a/b","view_key":"a.b.view","derivation":"direct",
		"relations":{"x":{"includes":["y"],"grants":["a.b.x"]},"y":{"includes":["x"],"grants":["a.b.view"]}}}`))
	require.NoError(t, err)
	require.Equal(t, []string{"x", "y"}, rt.RelationsGiving("a.b.view"))
}

func TestEmptyCatalog(t *testing.T) {
	c, err := ParseCatalog([]byte(`{"keys":[]}`))
	require.NoError(t, err)
	require.Empty(t, c.Types())
	require.Empty(t, c.PulledTypes())
}
