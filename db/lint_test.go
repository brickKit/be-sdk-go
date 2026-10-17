package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The lint's behaviour is pinned by the protocol vector migration-lint (vectors_test.go).

func TestSDKMigrationsPassTheLint(t *testing.T) {
	assert.Empty(t, LintMigrations(SDKMigrations().Files, "any_schema"))
}
