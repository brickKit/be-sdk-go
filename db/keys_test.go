package db

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServeNeverParsesTheOwnerRole(t *testing.T) { // P5.11
	for _, k := range ServeKeys() {
		assert.False(t, strings.HasPrefix(k.Name, "PG_OWNER_") || strings.HasPrefix(k.Name, "PG_MIGRATION_"), k.Name)
	}
	names := map[string]bool{}
	for _, k := range MigrateKeys() {
		names[k.Name] = true
	}
	for _, k := range []string{"PG_OWNER_USER", "PG_OWNER_PASSWORD", "PG_MIGRATION_HOST", "PG_MIGRATION_PORT", "PG_SCHEMA"} {
		assert.True(t, names[k], k)
	}
}
