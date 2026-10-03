package config

import (
	"slices"
	"strings"
	"testing"
)

func mustCatalogue(t *testing.T) *Catalogue {
	t.Helper()
	cat, err := LoadCatalogue()
	if err != nil {
		t.Fatalf("LoadCatalogue: %v", err)
	}
	return cat
}

func TestCatalogueLoadsProtocolKeys(t *testing.T) {
	cat := mustCatalogue(t)
	if cat.Protocol != "1.0" {
		t.Fatalf("protocol = %q", cat.Protocol)
	}
	k, ok := cat.Key("PG_PORT")
	if !ok || k.Format != FormatInt || k.Type != "integer" || k.Default == nil || *k.Default != "5432" {
		t.Fatalf("PG_PORT = %+v", k)
	}
	if k, _ := cat.Key("PG_SCHEMA"); !k.Required || k.Default != nil {
		t.Fatalf("PG_SCHEMA must be required with no default: %+v", k)
	}
	if k, _ := cat.Key("PG_POOL_MAX"); k.ShellDefault == nil || *k.ShellDefault != "40" {
		t.Fatalf("PG_POOL_MAX shell default: %+v", k)
	}
	eb, _ := cat.Key("EVENT_BUS_URL")
	if eb.DefaultFrom != "NATS_URL" || !slices.Equal(eb.OneOf, []string{"EVENT_BUS_URL", "NATS_URL"}) ||
		!slices.Equal(eb.Schemes, []string{"nats", "postgres", "kafka"}) {
		t.Fatalf("EVENT_BUS_URL = %+v", eb)
	}
	if k, _ := cat.Key("LOG_LEVEL"); k.Format != FormatEnum || len(k.Enum) != 4 {
		t.Fatalf("LOG_LEVEL = %+v", k)
	}
	if k, _ := cat.Key("PG_PASSWORD_FILE"); !k.Secret || k.Mount != "file" {
		t.Fatalf("PG_PASSWORD_FILE = %+v", k)
	}
	if k, _ := cat.Key("JOBS_OVERRIDES"); k.JSONSchema != "jobs-overrides.schema.json" {
		t.Fatalf("JOBS_OVERRIDES = %+v", k)
	}
	if _, ok := cat.Key("WIDGET_NO_FORMAT"); ok {
		t.Fatal("a pattern key is not a catalogue key")
	}
}

func TestCataloguePatternsReservedRetired(t *testing.T) {
	cat := mustCatalogue(t)
	if p, ok := cat.Pattern("SALES_ORDER_NO_FORMAT"); !ok || p.Format != FormatString {
		t.Fatalf("pattern = %+v %v", p, ok)
	}
	if _, ok := cat.Pattern("NO_FORMAT"); ok {
		t.Fatal("NO_FORMAT alone does not match <SERIES>_NO_FORMAT")
	}
	if r, ok := cat.Retired("PG_PASSWORD"); !ok || r.ReplacedBy != "PG_PASSWORD_FILE" {
		t.Fatalf("retired = %+v %v", r, ok)
	}
	if cat.SecretRoot != "/run/brickkit/secrets" || cat.RereadSeconds != 30 {
		t.Fatalf("secrets block = %q %d", cat.SecretRoot, cat.RereadSeconds)
	}
}

// The package's hard-coded platform names must agree with the catalogue's reserved block, so the
// vectors and the catalogue cannot drift apart unnoticed.
func TestCatalogueReservedMatchesPlatformNames(t *testing.T) {
	cat := mustCatalogue(t)
	for _, k := range cat.ReservedExact {
		if CheckKeyName(k) == nil {
			t.Errorf("catalogue reserves %s but CheckKeyName accepts it", k)
		}
	}
	for _, s := range cat.ReservedSuffix {
		if CheckKeyName("X"+s) == nil {
			t.Errorf("catalogue reserves suffix %s but CheckKeyName accepts X%s", s, s)
		}
	}
}

// Every catalogue key must be declarable and parse its own default: a catalogue the SDK cannot use
// would otherwise only fail inside a component's start.
func TestCatalogueKeysAreSelfConsistent(t *testing.T) {
	cat := mustCatalogue(t)
	for _, k := range cat.Keys {
		if err := CheckKeyDeclaration(KeyDeclaration{Key: k.Name, Secret: k.Secret, Mount: k.Mount, Type: k.Type}); err != nil {
			t.Errorf("%s: %v", k.Name, err)
		}
		d := k.Decl()
		if _, err := resolve(d, "", false); err != nil && err.Reason != ReasonMissing {
			t.Errorf("%s default: %v", k.Name, err)
		}
		if strings.HasSuffix(k.Name, "_FILE") != k.Secret {
			t.Errorf("%s: _FILE <=> secret broken", k.Name)
		}
	}
}
