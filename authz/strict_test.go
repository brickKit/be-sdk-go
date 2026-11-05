package authz

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The strict audit tier (P8.19, be-protocol alpha.82): the record says strict; only a read or an export may be.
func TestTheStrictAuditTier(t *testing.T) {
	o := Operation{Key: "demo.customer.export", ResourceType: "demo.customer", Purpose: "export", RiskClass: "high", StrictAudit: true}
	assert.Equal(t, "strict", o.Record()["audit"])
	o.StrictAudit, o.Audit = false, true
	assert.Equal(t, "required", o.Record()["audit"])
	d := &Definitions{Component: "demo/c", Resources: []Resource{{Type: "demo.customer", ViewOperation: "demo.customer.read",
		Dimensions: map[string]Dimension{}, Fields: map[string]Field{}}},
		Operations: []Operation{{Key: "demo.customer.read", ResourceType: "demo.customer", Purpose: "read", RiskClass: "low"},
			{Key: "demo.customer.edit", ResourceType: "demo.customer", Purpose: "manage", RiskClass: "high", StrictAudit: true}}}
	err := d.Validate()
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "the strict audit tier is a sensitive read's or an export's")
	}
}
