package besdk

import (
	"testing"

	"github.com/brickKit/be-sdk-go/internal/telemetry"
)

// P18.1 (rc.2): service.namespace is the component's domain; deployment.environment.name is DEPLOY_ENV,
// "dev" when the component does not declare it or leaves it unset.
func TestMemberResource(t *testing.T) {
	got := memberResource("erp/sales", "3.0.0", "pod-1", "")
	want := telemetry.Resource{ComponentID: "erp/sales", ComponentVersion: "3.0.0", Namespace: "erp",
		InstanceID: "pod-1", Environment: "dev"}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if r := memberResource("erp/sales", "3.0.0", "pod-1", "prod"); r.Environment != "prod" {
		t.Fatalf("DEPLOY_ENV prod gave %q", r.Environment)
	}
}

// P4.1 / DEFAULT_LOCALE (rc.2): the primary subtag zh or en selects that catalogue language; anything
// else falls back to en (and the caller logs a WARN).
func TestCatalogueLocale(t *testing.T) {
	for in, want := range map[string]struct {
		loc string
		ok  bool
	}{"zh-CN": {"zh-CN", true}, "en": {"en", true}, "EN-us": {"EN-us", true}, "fr-FR": {"en", false}, "ja": {"en", false}} {
		loc, ok := catalogueLocale(in)
		if loc != want.loc || ok != want.ok {
			t.Errorf("%s: got %q %v, want %q %v", in, loc, ok, want.loc, want.ok)
		}
	}
}
