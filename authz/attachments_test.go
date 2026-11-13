package authz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withCategories(c map[string]AttachmentCategory) *Definitions {
	return &Definitions{Component: "demo/c", Resources: []Resource{{Type: "demo.order", ViewOperation: "demo.order.read",
		Dimensions: map[string]Dimension{}, Fields: map[string]Field{"id": {Class: Ordinary}, "contract_file": {Class: Ordinary}},
		AttachmentCategories: c}},
		Operations: []Operation{{Key: "demo.order.read", ResourceType: "demo.order", Purpose: "read", RiskClass: "low"}}}
}

var contract = AttachmentCategory{Field: "contract_file", MaxBytes: 20 << 20, AllowedTypes: []string{"image/png", "application/pdf"},
	Sensitivity: Normal, RetentionClass: "purchase_contract", Version: "1"}

// Security 09a: a resource's attachment categories are declared with their field, limits, sensitivity, retention
// class and version, and encoded in its definition record (types as a set, in order).
func TestAttachmentCategories(t *testing.T) {
	d := withCategories(map[string]AttachmentCategory{"contract_file": contract})
	require.NoError(t, d.Validate())
	rec := d.Resources[0].Record()
	cat := rec["attachment_categories"].(map[string]any)["contract_file"].(map[string]any)
	assert.Equal(t, []any{"application/pdf", "image/png"}, cat["allowed_types"])
	assert.Equal(t, int64(20<<20), cat["max_bytes"])
	_, err := d.Resources[0].Digest()
	require.NoError(t, err)
	_, err = d.Records("0190f3a1-0000-7000-8000-000000000001", "3.0.0")
	require.NoError(t, err, "the sealed record follows be.security.resource_definition")
	none := withCategories(nil)
	require.NoError(t, none.Validate())
	assert.NotContains(t, none.Resources[0].Record(), "attachment_categories", "no category: none in the record")

	for name, bad := range map[string]AttachmentCategory{
		"no such field":    {Field: "scan", MaxBytes: 1, AllowedTypes: []string{"a/b"}, Sensitivity: Normal, RetentionClass: "x", Version: "1"},
		"no limit":         {Field: "contract_file", AllowedTypes: []string{"a/b"}, Sensitivity: Normal, RetentionClass: "x", Version: "1"},
		"no types":         {Field: "contract_file", MaxBytes: 1, Sensitivity: Normal, RetentionClass: "x", Version: "1"},
		"a sensitivity":    {Field: "contract_file", MaxBytes: 1, AllowedTypes: []string{"a/b"}, Sensitivity: "secret", RetentionClass: "x", Version: "1"},
		"no version":       {Field: "contract_file", MaxBytes: 1, AllowedTypes: []string{"a/b"}, Sensitivity: Normal, RetentionClass: "x"},
		"no retention key": {Field: "contract_file", MaxBytes: 1, AllowedTypes: []string{"a/b"}, Sensitivity: Normal, Version: "1"},
	} {
		assert.Error(t, withCategories(map[string]AttachmentCategory{"contract_file": bad}).Validate(), name)
	}
	assert.Error(t, withCategories(map[string]AttachmentCategory{"Bad Name": contract}).Validate(), "a category name is a key")
}
