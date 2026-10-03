package pg

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// CP-DB-04 (stage-B ruling): the authorization projection (ddl/07) is not part of the platform
// migration every component gets; it is a separate, idempotent step for components that declare
// resource types.
func TestPlatformSQLLeavesOutTheAuthzProjection(t *testing.T) {
	sql, err := platformSQL("erp/x")
	require.NoError(t, err)
	require.NotContains(t, sql, "07-authz-projection")
	require.NotContains(t, sql, "besdk_authz_acl")
	proj, err := authzProjectionSQL()
	require.NoError(t, err)
	require.Contains(t, proj, "CREATE TABLE IF NOT EXISTS besdk_authz_acl")
	require.Contains(t, proj, "CREATE TABLE IF NOT EXISTS besdk_authz_cursor")
	require.False(t, strings.Contains(proj, "besdk_outbox"))
}
