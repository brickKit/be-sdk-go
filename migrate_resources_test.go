package besdk

import "testing"

// CP-DB-04: the root asks for the authorization projection exactly when Spec.Catalog has a non-empty
// resource_types array (contract-infra-authz schemas/catalog.schema.json).
func TestDeclaresResources(t *testing.T) {
	cases := map[string]bool{
		``:                                   false,
		`{}`:                                 false,
		`{"keys": [], "resource_types": []}`: false,
		`{"resource_types": null}`:           false,
		`{"resource_types": [{"type": "erp.sales.order"}]}`: true,
	}
	for catalog, want := range cases {
		if got := declaresResources(catalog); got != want {
			t.Errorf("%q: %v, want %v", catalog, got, want)
		}
	}
}
