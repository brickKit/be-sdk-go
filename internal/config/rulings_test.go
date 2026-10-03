package config

import "testing"

// P12.5 (stage-B ruling): EVENTS_MAX_DELIVER and EVENTS_BACKOFF have no catalogue default, so a key
// counts as set only when present and a subscription's own values apply otherwise.
func TestCatalogueEventsKeysHaveNoDefault(t *testing.T) {
	cat := mustCatalogue(t)
	for _, name := range []string{"EVENTS_MAX_DELIVER", "EVENTS_BACKOFF"} {
		k, ok := cat.Key(name)
		if !ok || k.Default != nil {
			t.Fatalf("%s = %+v %v", name, k, ok)
		}
	}
	v, errs := Load(NewSchema("conformance/widget", "1.0.0", []Decl{
		mustKey(t, cat, "EVENTS_MAX_DELIVER").Decl(), mustKey(t, cat, "EVENTS_BACKOFF").Decl(),
	}), func(string) (string, bool) { return "", false })
	if len(errs) > 0 {
		t.Fatalf("load: %v", errs)
	}
	if v.Has("EVENTS_MAX_DELIVER") || v.Has("EVENTS_BACKOFF") {
		t.Fatal("an absent EVENTS_* key must not be set")
	}
}

// P10.5 / stage-B ruling: PG_POOL_MIN_IDLE is not a protocol key; how many idle connections a pool
// keeps is the SDK's own behaviour (Go: no idle floor, see README).
func TestCatalogueHasNoPoolMinIdle(t *testing.T) {
	if k, ok := mustCatalogue(t).Key("PG_POOL_MIN_IDLE"); ok {
		t.Fatalf("PG_POOL_MIN_IDLE is a catalogue key: %+v", k)
	}
}

func mustKey(t *testing.T, cat *Catalogue, name string) CatalogueKey {
	t.Helper()
	k, ok := cat.Key(name)
	if !ok {
		t.Fatalf("no catalogue key %s", name)
	}
	return k
}
