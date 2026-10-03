package config

import (
	"io/fs"
	"slices"
	"testing"

	beprotocol "github.com/brickKit/be-protocol"
)

func widgetYAML(t *testing.T) []byte {
	t.Helper()
	b, err := fs.ReadFile(beprotocol.FS, "fixtures/widget/component.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustDecl(t *testing.T, s Schema, key string) Decl {
	t.Helper()
	d, ok := s.Lookup(key)
	if !ok {
		t.Fatalf("%s not declared", key)
	}
	return d
}

func TestParseSchemaWidgetFixture(t *testing.T) {
	s, err := ParseSchema(widgetYAML(t), mustCatalogue(t))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if s.ID != "conformance/widget" || s.Version != "1.0.0" {
		t.Fatalf("identity = %q %q", s.ID, s.Version)
	}
	if len(s.Decls) != 44 || s.Decls[0].Name != "PG_HOST" || s.Decls[43].Name != "WIDGET_RESERVE_HOLD_SECONDS" {
		t.Fatalf("declaration order/count: %d first %s", len(s.Decls), s.Decls[0].Name)
	}
	if d := mustDecl(t, s, "PG_HOST"); !d.Required || !d.Protocol || d.Format != FormatString {
		t.Errorf("PG_HOST = %+v", d)
	}
	if d := mustDecl(t, s, "PG_PORT"); d.Format != FormatInt || d.Default == nil || *d.Default != "5432" || d.Required {
		t.Errorf("PG_PORT = %+v", d)
	}
	if d := mustDecl(t, s, "PG_PASSWORD_FILE"); !d.Secret || !d.Required {
		t.Errorf("PG_PASSWORD_FILE = %+v", d)
	}
	if d := mustDecl(t, s, "PG_MIGRATION_PORT"); d.DefaultFrom != "PG_PORT" || d.Format != FormatInt {
		t.Errorf("PG_MIGRATION_PORT = %+v", d)
	}
	eb := mustDecl(t, s, "EVENT_BUS_URL")
	if eb.Format != FormatURL || eb.DefaultFrom != "NATS_URL" || len(eb.OneOf) != 2 || len(eb.Schemes) != 3 {
		t.Errorf("EVENT_BUS_URL = %+v", eb)
	}
	if d := mustDecl(t, s, "EVENTS_BACKOFF"); d.Format != FormatDurations {
		t.Errorf("EVENTS_BACKOFF = %+v", d)
	}
	if d := mustDecl(t, s, "LOG_LEVEL"); d.Format != FormatEnum || !slices.Contains(d.Enum, "warn") {
		t.Errorf("LOG_LEVEL = %+v", d)
	}
	if d := mustDecl(t, s, "JOBS_OVERRIDES"); d.Format != FormatJSON || d.JSONKind != JSONObject || d.Default == nil || *d.Default != "" {
		t.Errorf("JOBS_OVERRIDES = %+v", d)
	}
	if d := mustDecl(t, s, "S3_FORCE_PATH_STYLE"); d.Format != FormatBool || *d.Default != "false" {
		t.Errorf("S3_FORCE_PATH_STYLE = %+v", d)
	}
	if d := mustDecl(t, s, "AUTHZ_GRPC_URL"); !d.Required || d.Format != FormatURL {
		t.Errorf("AUTHZ_GRPC_URL = %+v", d)
	}
	if d := mustDecl(t, s, "WIDGET_NO_FORMAT"); d.Protocol || d.Format != FormatString || *d.Default != "WG{yyyy}{mm}-{seq:05}" {
		t.Errorf("WIDGET_NO_FORMAT = %+v", d)
	}
	if d := mustDecl(t, s, "WIDGET_RESERVE_HOLD_SECONDS"); d.Protocol || d.Format != FormatInt || *d.Default != "900" {
		t.Errorf("WIDGET_RESERVE_HOLD_SECONDS = %+v", d)
	}
	if d := mustDecl(t, s, "WIDGET_APPROVE_TIMEOUT"); d.Format != FormatString || d.Type != "string" {
		t.Errorf("WIDGET_APPROVE_TIMEOUT = %+v", d)
	}
}

const ownKeysYAML = `
metadata: {id: erp/sales, version: 3.0.0}
configSchema:
  type: object
  properties:
    PG_POOL_MAX: {type: integer, default: 20}
    SALES_MODE: {type: string, enum: [fast, safe], default: safe}
    SALES_RULES: {type: object}
    SALES_TAGS: {type: array}
    SALES_RETRIES: {type: integer, minimum: 1, default: 3}
    APP_TOKEN_SIGNING_KEY_FILE: {type: string, secret: true, mount: file}
  required: [APP_TOKEN_SIGNING_KEY_FILE]
`

func TestParseSchemaOwnKeysAndComponentDefaultWins(t *testing.T) {
	s, err := ParseSchema([]byte(ownKeysYAML), mustCatalogue(t))
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	if d := mustDecl(t, s, "PG_POOL_MAX"); *d.Default != "20" || d.ShellDefault == nil || *d.ShellDefault != "40" {
		t.Errorf("PG_POOL_MAX = %+v", d)
	}
	if d := mustDecl(t, s, "SALES_MODE"); d.Format != FormatEnum || !slices.Equal(d.Enum, []string{"fast", "safe"}) {
		t.Errorf("SALES_MODE = %+v", d)
	}
	if d := mustDecl(t, s, "SALES_RULES"); d.Format != FormatJSON || d.JSONKind != JSONObject {
		t.Errorf("SALES_RULES = %+v", d)
	}
	if d := mustDecl(t, s, "SALES_TAGS"); d.Format != FormatJSON || d.JSONKind != JSONArray {
		t.Errorf("SALES_TAGS = %+v", d)
	}
	if d := mustDecl(t, s, "SALES_RETRIES"); d.Minimum == nil || *d.Minimum != 1 {
		t.Errorf("SALES_RETRIES = %+v", d)
	}
	if d := mustDecl(t, s, "APP_TOKEN_SIGNING_KEY_FILE"); !d.Secret || !d.Required || d.Protocol {
		t.Errorf("APP_TOKEN_SIGNING_KEY_FILE = %+v", d)
	}
}

const badDeclsYAML = `
metadata: {id: erp/sales, version: 3.0.0}
configSchema:
  properties:
    PORT: {type: integer}
    UPSTREAM_ENDPOINT: {type: string}
    SIGNING_KEY: {type: string, secret: true}
    RULES_FILE: {type: string}
    PG_PASSWORD: {type: string}
    PG_PORT: {type: string}
    PG_PASSWORD_FILE: {type: string}
    SALES_RATIO: {type: number}
    pg_host: {type: string}
  required: [NOT_DECLARED]
`

func TestParseSchemaCollectsEveryDeclarationError(t *testing.T) {
	_, err := ParseSchema([]byte(badDeclsYAML), mustCatalogue(t))
	var errs Errors
	if e, ok := err.(Errors); ok {
		errs = e
	} else {
		t.Fatalf("want Errors, got %T %v", err, err)
	}
	want := map[string]string{
		"PORT":              ReasonKeyReserved,
		"UPSTREAM_ENDPOINT": ReasonKeyReserved,
		"SIGNING_KEY":       ReasonSecretNotFile,
		"RULES_FILE":        ReasonFileSuffixRsv,
		"PG_PASSWORD":       ReasonKeyInvalid, // retired
		"PG_PORT":           ReasonKeyInvalid, // type differs from the catalogue
		"PG_PASSWORD_FILE":  ReasonFileSuffixRsv,
		"SALES_RATIO":       ReasonKeyInvalid, // number has no protocol format
		"pg_host":           ReasonKeyInvalid,
		"NOT_DECLARED":      ReasonManifestInvalid,
	}
	got := map[string]string{}
	for _, e := range errs {
		got[e.Key] = e.Reason
	}
	for k, r := range want {
		if got[k] != r {
			t.Errorf("%s: want %s, got %q", k, r, got[k])
		}
	}
	if len(errs) != len(want) {
		t.Errorf("want %d errors, got %d: %v", len(want), len(errs), errs)
	}
}

func TestParseSchemaManifestErrors(t *testing.T) {
	cat := mustCatalogue(t)
	for name, doc := range map[string]string{
		"not yaml":    "metadata: [",
		"no id":       "metadata: {version: 1.0.0}",
		"no version":  "metadata: {id: erp/sales}",
		"bad default": "metadata: {id: a/b, version: 1.0.0}\nconfigSchema: {properties: {A: {type: string, default: [x]}}}",
	} {
		_, err := ParseSchema([]byte(doc), cat)
		e, ok := AsError(err)
		if !ok {
			if es, isList := err.(Errors); isList && len(es) > 0 {
				e, ok = es[0], true
			}
		}
		if !ok || e.Reason != ReasonManifestInvalid {
			t.Errorf("%s: want MANIFEST_INVALID, got %v", name, err)
		}
	}
}

func TestParseSchemaWithoutConfigSchema(t *testing.T) {
	s, err := ParseSchema([]byte("metadata: {id: a/b, version: 1.0.0}"), mustCatalogue(t))
	if err != nil || len(s.Decls) != 0 {
		t.Fatalf("got %v %v", s, err)
	}
}
