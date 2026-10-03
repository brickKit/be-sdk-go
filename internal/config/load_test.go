package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const secretContent = "pa55-w0rd-NEVER-LOGGED"

// widgetEnv returns a Source holding every required widget key, secrets as files in a temp dir.
func widgetEnv(t *testing.T) map[string]string {
	t.Helper()
	dir := t.TempDir()
	env := map[string]string{
		"PG_HOST": "be-postgres", "PG_DATABASE": "brickkit_db", "PG_USER": "widget_rw",
		"PG_OWNER_USER": "widget", "PG_SCHEMA": "widget", "NATS_URL": "nats://be-nats:4222",
		"AUTHZ_URL": "http://infra-authz-3-0-0:8223", "AUTHZ_GRPC_URL": "http://infra-authz-3-0-0:9223",
		"IAM_URL": "http://infra-iam-casdoor-3-0-0:8200", "IAM_ISSUER": "urn:be:t1:iam", "TENANT_ID": "t1",
		"S3_URL": "http://be-rustfs:9000", "S3_BUCKET": "widget",
		"COMPONENT_ID": "conformance/widget", "PORT": "8080",
		"CONFORMANCE_PEER_GRPC_ENDPOINT": "http://conformance-peer-1-0-0:9090",
	}
	for _, k := range []string{"PG_PASSWORD_FILE", "PG_OWNER_PASSWORD_FILE", "S3_ACCESS_KEY_ID_FILE", "S3_SECRET_ACCESS_KEY_FILE"} {
		p := filepath.Join(dir, k)
		if err := os.WriteFile(p, []byte(secretContent+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		env[k] = p
	}
	return env
}

func mapSource(m map[string]string) Source {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func widgetSchema(t *testing.T) Schema {
	t.Helper()
	s, err := ParseSchema(widgetYAML(t), mustCatalogue(t))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustLoad(t *testing.T, s Schema, env map[string]string) *Values {
	t.Helper()
	v, errs := Load(s, mapSource(env))
	if len(errs) > 0 {
		t.Fatalf("Load: %v", Errors(errs))
	}
	return v
}

// errKeys renders errors as KEY=REASON, sorted, and checks that no error carries a secret.
func errKeys(t *testing.T, errs []*Error) []string {
	t.Helper()
	var out []string
	for _, e := range errs {
		if strings.Contains(e.Error(), secretContent) {
			t.Errorf("error leaks the secret: %v", e)
		}
		out = append(out, e.Key+"="+e.Reason)
	}
	slices.Sort(out)
	return out
}

func TestLoadWidgetAppliesDefaults(t *testing.T) {
	v := mustLoad(t, widgetSchema(t), widgetEnv(t))
	if n, _ := v.Int("PG_PORT"); n != 5432 {
		t.Errorf("PG_PORT = %d", n)
	}
	if s, _ := v.String("LOG_LEVEL"); s != "info" {
		t.Errorf("LOG_LEVEL = %q", s)
	}
	if v.Has("JOBS_OVERRIDES") || v.Has("OTEL_BASE_URL") {
		t.Error("an empty default of a typed key is not a value")
	}
	if v.Has("EVENTS_BACKOFF") || v.Has("EVENTS_MAX_DELIVER") {
		t.Error("rc.2 P12.5: EVENTS_* have no catalogue default; absent means the subscription's own value")
	}
}

func TestLoadCollectsAllErrors(t *testing.T) {
	env := map[string]string{"PG_PORT": "x", "LOG_LEVEL": "WARN", "PG_PASSWORD_FILE": "relative/path"}
	_, errs := Load(widgetSchema(t), mapSource(env))
	want := []string{
		"AUTHZ_GRPC_URL=CONFIG_MISSING", "AUTHZ_URL=CONFIG_MISSING", "EVENT_BUS_URL=CONFIG_MISSING",
		"IAM_ISSUER=CONFIG_MISSING", "IAM_URL=CONFIG_MISSING", "LOG_LEVEL=CONFIG_INVALID",
		"PG_DATABASE=CONFIG_MISSING", "PG_HOST=CONFIG_MISSING", "PG_OWNER_PASSWORD_FILE=CONFIG_MISSING",
		"PG_OWNER_USER=CONFIG_MISSING", "PG_PASSWORD_FILE=CONFIG_INVALID", "PG_PORT=CONFIG_INVALID",
		"PG_SCHEMA=CONFIG_MISSING", "PG_USER=CONFIG_MISSING", "S3_ACCESS_KEY_ID_FILE=CONFIG_MISSING",
		"S3_BUCKET=CONFIG_MISSING", "S3_SECRET_ACCESS_KEY_FILE=CONFIG_MISSING", "S3_URL=CONFIG_MISSING",
		"TENANT_ID=CONFIG_MISSING",
	}
	if got := errKeys(t, errs); !slices.Equal(got, want) {
		t.Errorf("errors:\n got  %v\n want %v", got, want)
	}
}

func TestLoadOneOfEventBus(t *testing.T) {
	s := widgetSchema(t)
	env := widgetEnv(t)
	v := mustLoad(t, s, env)
	if u, ok := v.String("EVENT_BUS_URL"); !ok || u != "nats://be-nats:4222" {
		t.Errorf("EVENT_BUS_URL falls back to NATS_URL: %q %v", u, ok)
	}
	env["EVENT_BUS_URL"] = "postgres://be-postgres:5432/brickkit_db?schema=be_bus"
	delete(env, "NATS_URL")
	v = mustLoad(t, s, env)
	if u, _ := v.String("EVENT_BUS_URL"); !strings.HasPrefix(u, "postgres://") || v.Has("NATS_URL") {
		t.Errorf("EVENT_BUS_URL alone: %q", u)
	}
	delete(env, "EVENT_BUS_URL")
	env["NATS_URL"] = ""
	_, errs := Load(s, mapSource(env))
	if got := errKeys(t, errs); !slices.Equal(got, []string{"EVENT_BUS_URL=CONFIG_MISSING"}) {
		t.Errorf("neither set: %v", got)
	}
}

func TestLoadOneOfWithOnlyOneMemberDeclared(t *testing.T) {
	cat := mustCatalogue(t)
	nats, _ := cat.Key("NATS_URL")
	s := NewSchema("a/b", "1.0.0", []Decl{nats.Decl()})
	_, errs := Load(s, mapSource(nil))
	if got := errKeys(t, errs); !slices.Equal(got, []string{"NATS_URL=CONFIG_MISSING"}) {
		t.Errorf("got %v", got)
	}
}

func TestLoadDefaultFrom(t *testing.T) {
	s := widgetSchema(t)
	env := widgetEnv(t)
	env["PG_MIGRATION_HOST"] = "" // brickKit's ${X:-}: empty falls back like absent
	env["S3_PUBLIC_URL"] = "https://files.example.com"
	v := mustLoad(t, s, env)
	if h, _ := v.String("PG_MIGRATION_HOST"); h != "be-postgres" {
		t.Errorf("PG_MIGRATION_HOST = %q", h)
	}
	if p, _ := v.Int("PG_MIGRATION_PORT"); p != 5432 {
		t.Errorf("PG_MIGRATION_PORT = %d (from PG_PORT's default)", p)
	}
	if u, _ := v.String("S3_PUBLIC_URL"); u != "https://files.example.com" {
		t.Errorf("S3_PUBLIC_URL = %q", u)
	}
	env["PG_PORT"] = "bad"
	_, errs := Load(s, mapSource(env))
	if got := errKeys(t, errs); !slices.Equal(got, []string{"PG_PORT=CONFIG_INVALID"}) {
		t.Errorf("an invalid source key is reported once: %v", got)
	}
}

func TestLoadSecretFiles(t *testing.T) {
	s := widgetSchema(t)
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := widgetEnv(t)
	env["PG_PASSWORD_FILE"] = filepath.Join(dir, "missing")
	env["PG_OWNER_PASSWORD_FILE"] = dir
	env["S3_ACCESS_KEY_ID_FILE"] = empty
	env["S3_SECRET_ACCESS_KEY_FILE"] = secretContent // a value, not a path
	_, errs := Load(s, mapSource(env))
	want := []string{"PG_OWNER_PASSWORD_FILE=CONFIG_INVALID", "PG_PASSWORD_FILE=CONFIG_INVALID",
		"S3_ACCESS_KEY_ID_FILE=CONFIG_MISSING", "S3_SECRET_ACCESS_KEY_FILE=CONFIG_INVALID"}
	if got := errKeys(t, errs); !slices.Equal(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestLoadOptionalSecretMayBeEmpty(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "k")
	if err := os.WriteFile(p, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s := NewSchema("a/b", "1.0.0", []Decl{{Name: "NEXT_KEY_FILE", Type: "string", Format: FormatString, Secret: true}})
	v := mustLoad(t, s, map[string]string{"NEXT_KEY_FILE": p})
	if got, _ := v.String("NEXT_KEY_FILE"); got != p {
		t.Errorf("String of a secret key is its path: %q", got)
	}
}

func TestLoadJSONValuesAgainstTheirSchema(t *testing.T) {
	s := widgetSchema(t)
	env := widgetEnv(t)
	env["JOBS_OVERRIDES"] = `{"be.cleanup":{"interval":"2s"}}`
	env["DATA_LIFECYCLE"] = "mode: dry-run\ncold_store: none\n"
	v := mustLoad(t, s, env)
	var dl map[string]string
	if ok, err := v.JSON("DATA_LIFECYCLE", &dl); !ok || err != nil || dl["mode"] != "dry-run" {
		t.Errorf("DATA_LIFECYCLE from YAML = %v %v %v", dl, ok, err)
	}
	env["JOBS_OVERRIDES"] = `{"be.cleanup":{"bogus":1}}`
	env["DATA_LIFECYCLE"] = `{"mode":"sideways"}`
	_, errs := Load(s, mapSource(env))
	want := []string{"DATA_LIFECYCLE=CONFIG_INVALID", "JOBS_OVERRIDES=CONFIG_INVALID"}
	if got := errKeys(t, errs); !slices.Equal(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestLoadChecksFamilyAddressesAtStart(t *testing.T) {
	s := widgetSchema(t)
	env := widgetEnv(t)
	v := mustLoad(t, s, env)
	if a, ok := v.Family("AUTHZ_GRPC_URL"); !ok || a != "infra-authz-3-0-0:9223" {
		t.Errorf("AUTHZ_GRPC_URL target = %q", a)
	}
	if a, ok := v.Family("IAM_URL"); !ok || a != "http://infra-iam-casdoor-3-0-0:8200" {
		t.Errorf("IAM_URL base = %q", a)
	}
	if got := panicReason(t, func() { v.Family("PG_HOST") }); got != ReasonKeyInvalid {
		t.Errorf("Family of a non-family key: %q", got)
	}
	env["AUTHZ_URL"] = "http://infra-authz-3-0-0"            // no port
	env["AUTHZ_GRPC_URL"] = "https://infra-authz-3-0-0:9223" // another scheme
	env["IAM_URL"] = "http://infra-iam-casdoor-3-0-0:8200/x" // a path
	_, errs := Load(s, mapSource(env))
	want := []string{"AUTHZ_GRPC_URL=CONFIG_INVALID", "AUTHZ_URL=CONFIG_INVALID", "IAM_URL=CONFIG_INVALID"}
	if got := errKeys(t, errs); !slices.Equal(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}
