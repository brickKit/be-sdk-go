package pg

import (
	"slices"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/testpg"
	"github.com/stretchr/testify/require"
)

// CP-DB-04: besdk_authz_acl / besdk_authz_cursor exist exactly when the component declares resource
// types. A component that starts declaring them later gets them on its next migrate up.
func TestAuthzProjectionOnlyWhenResourcesAreDeclared(t *testing.T) {
	id := testpg.New(t)
	c := migrateConfig(id, widgetFS(), nil)
	c.AuthzProjection = false
	_, err := MigrateUp(within(t, 60e9), c)
	require.NoError(t, err)
	tables := tablesOf(t, id, id.Schema)
	require.NotContains(t, tables, "besdk_authz_acl")
	require.NotContains(t, tables, "besdk_authz_cursor")
	require.Contains(t, tables, "besdk_outbox")

	c.AuthzProjection = true
	for range 2 { // idempotent
		_, err = MigrateUp(within(t, 60e9), c)
		require.NoError(t, err)
		tables = tablesOf(t, id, id.Schema)
		require.True(t, slices.Contains(tables, "besdk_authz_acl") && slices.Contains(tables, "besdk_authz_cursor"), "%v", tables)
	}
}
