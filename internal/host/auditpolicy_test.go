package host

import (
	"testing"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/stretchr/testify/assert"
)

// The auditor takes the restricted fields and the strict operations from the member's definitions (P8.19).
func TestTheAuditPolicyComesFromTheDefinitions(t *testing.T) {
	restricted, strict := auditPolicy(&authz.Definitions{Component: "demo/c",
		Resources: []authz.Resource{{Type: "demo.customer", Fields: map[string]authz.Field{
			"name": {Class: authz.Ordinary}, "tax_id": {Class: authz.Restricted, ReadKey: "demo.customer.tax_read"},
			"secret": {Class: authz.Secret}}}},
		Operations: []authz.Operation{{Key: "demo.customer.export", StrictAudit: true}, {Key: "demo.customer.read", Audit: true}}})
	assert.Equal(t, map[string]map[string]bool{"demo.customer": {"tax_id": true}}, restricted)
	assert.Equal(t, map[string]bool{"demo.customer.export": true}, strict)
	restricted, strict = auditPolicy(nil)
	assert.Empty(t, restricted)
	assert.Empty(t, strict)
}
