package authz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An operation key is its resource type and one action segment: a grant's segment names the resource type, and the
// tools that build grants from operation keys (the acceptance suite's secure stage) take it by dropping the last
// segment.
func TestAnOperationKeyIsItsResourceTypeAndOneAction(t *testing.T) {
	defs := func(keys ...string) *Definitions {
		d := &Definitions{Component: "demo/c", Resources: []Resource{{Type: "demo.customer", ViewOperation: "demo.customer.read",
			Dimensions: map[string]Dimension{}, Fields: map[string]Field{}}}}
		for _, k := range keys {
			d.Operations = append(d.Operations, Operation{Key: k, ResourceType: "demo.customer", Purpose: "read", RiskClass: "low"})
		}
		return d
	}
	require.NoError(t, defs("demo.customer.read", "demo.customer.export_all").Validate())
	for _, bad := range []string{"demo.admin", "demo.customer.address.read", "demo.customers.read", "demo.customer"} {
		err := defs("demo.customer.read", bad).Validate()
		if assert.Error(t, err, bad) {
			assert.Contains(t, err.Error(), bad+": an operation key is its resource type and one action (demo.customer.<action>)")
		}
	}
}
