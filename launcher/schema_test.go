package launcher

import (
	"testing"
	"testing/fstest"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/stretchr/testify/assert"
)

func TestConfigSchemaDeclaresWhatTheSDKReads(t *testing.T) {
	def := component.Definition{ID: "demo/alpha", Version: "3.0.0",
		Keys:       []config.Key{{Name: "GREETING", Type: config.String, Required: true}},
		Migrations: fstest.MapFS{"0001_x.up.sql": {Data: []byte("CREATE TABLE x (id int)")}},
		Streams:    []events.Stream{{Name: "demo.alpha", Partitions: 4}}}
	s := ConfigSchema(def, map[string]string{"GREETING": "what the module says"})
	props := s["properties"].(map[string]any)
	assert.Equal(t, map[string]any{"type": "string", "description": "what the module says"}, props["GREETING"])
	assert.Equal(t, map[string]any{"type": "string", "secret": true, "mount": "file",
		"description": "a file path, re-read on rotation"}, props["SERVICE_TLS_CA_FILE"])
	assert.Equal(t, map[string]any{"type": "string", "secret": true,
		"description": "process-level: the same for every member of a shell"}, props["BUS_URL"])
	assert.Equal(t, map[string]any{"type": "string", "description": "a duration (e.g. 2s, 500ms); SDK default 2s"},
		props["STOP_DRAIN_DELAY"])
	for _, k := range []string{"PG_OWNER_PASSWORD", "PG_MIGRATION_HOST", "IMPLEMENTATION_INSTANCE", "DEPLOYMENT_ID"} {
		assert.Contains(t, props, k)
	}
	assert.Contains(t, s["required"], "GREETING")
	assert.Contains(t, s["required"], "BUS_URL")

	bare := ConfigSchema(component.Definition{ID: "demo/beta", Version: "3.0.0"}, nil)["properties"].(map[string]any)
	assert.NotContains(t, bare, "PG_OWNER_PASSWORD", "no database, no migrate keys")
	assert.NotContains(t, bare, "BUS_URL", "no streams, no bus")
}

func TestManifestProblemsNamesEveryDifference(t *testing.T) {
	def := component.Definition{ID: "demo/beta", Version: "3.0.0",
		Keys: []config.Key{{Name: "GREETING", Type: config.String, Required: true}}}
	ok := []byte(ConfigSchemaYAML(def, nil))
	assert.Empty(t, ManifestProblems(ok, def, nil), "the generated block matches itself")
	bad := []byte(`configSchema:
  type: object
  required: [LOG_LEVEL]
  properties:
    LOG_LEVEL: {type: string}
    EXTRA: {type: string}
`)
	got := ManifestProblems(bad, def, nil)
	assert.Contains(t, got, "GREETING: not listed as required")
	assert.Contains(t, got, "LOG_LEVEL: listed as required, but the SDK does not require it")
	assert.Contains(t, got, "EXTRA: declared, but nothing reads it")
	assert.Contains(t, got, "GREETING: not declared")
}
