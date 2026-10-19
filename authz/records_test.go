package authz

import (
	"testing"

	"github.com/brickKit/be-sdk-go/v3/records"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDefinitionRecordsPassTheirSchemas: what a component submits opens as the authorization member opens it (D76),
// for a component ID with a hyphen too (found by the first cell of step 5: the revision domain kept the "-").
func TestDefinitionRecordsPassTheirSchemas(t *testing.T) {
	d := &Definitions{Component: "test/probe-go2", Resources: []Resource{{Type: "test.probe_go2", ViewOperation: "test.probe_go2.read",
		Dimensions: map[string]Dimension{"region": {Kind: ValueSet, Column: "region"}},
		Fields:     map[string]Field{"id": {Class: Ordinary}, "cost": {Class: Restricted, ReadKey: "test.probe_go2.cost"}}}},
		Operations: []Operation{{Key: "test.probe_go2.read", ResourceType: "test.probe_go2", Purpose: "read", RiskClass: "low"},
			{Key: "test.probe_go2.write", ResourceType: "test.probe_go2", Purpose: "action", RiskClass: "high",
				SideEffects: []string{"business_write"}, Audit: true}}}
	require.NoError(t, d.Validate())
	sealed, err := d.Records("0190f3a1-0000-7000-8000-00000000e001", "3.2.17")
	require.NoError(t, err)
	require.Len(t, sealed, 3)
	for _, s := range sealed {
		_, err := records.Open(s.RecordType, s.Raw, s.Digest)
		assert.NoError(t, err, s.Key)
	}
}
