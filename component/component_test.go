package component

import "testing"

// P11.11: the default prefix is /api/<name>; a route is under a prefix by path segments.
func TestThePublicPrefix(t *testing.T) {
	if got := DefaultPrefix("erp/purchase"); got != "/api/purchase" {
		t.Fatalf("default prefix %q", got)
	}
	if got := (Definition{ID: "infra/iam-casdoor", PublicPrefix: "/api/iam"}).Prefix(); got != "/api/iam" {
		t.Fatalf("declared prefix %q", got)
	}
	for path, want := range map[string]bool{"/api/sales": true, "/api/sales/orders": true, "/api/salesman": false,
		"/be/flow/tasks": false, "/api": false} {
		if UnderPrefix("/api/sales", path) != want {
			t.Errorf("%s under /api/sales: want %v", path, want)
		}
	}
}
