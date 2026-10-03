package config

import (
	"testing"
	"time"
)

// panicReason runs f and returns the reason of the *Error it panics with ("" = no panic).
func panicReason(t *testing.T, f func()) (reason string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		e, ok := AsError(r)
		if !ok {
			t.Fatalf("panic with %T %v, want *Error", r, r)
		}
		reason = e.Reason
	}()
	f()
	return ""
}

func TestValuesTypedGetters(t *testing.T) {
	env := widgetEnv(t)
	env["S3_FORCE_PATH_STYLE"] = "1"
	env["WIDGET_APPROVE_TIMEOUT"] = "45s"
	v := mustLoad(t, widgetSchema(t), env)
	if b, ok := v.Bool("S3_FORCE_PATH_STYLE"); !b || !ok {
		t.Error("S3_FORCE_PATH_STYLE")
	}
	if d, _ := v.Duration("SHUTDOWN_GRACE"); d != 25*time.Second {
		t.Errorf("SHUTDOWN_GRACE = %v", d)
	}
	if loc, _ := v.Location("BUSINESS_TIMEZONE"); loc == nil || loc.String() != "Asia/Shanghai" {
		t.Errorf("BUSINESS_TIMEZONE = %v", loc)
	}
	if d, ok := v.Duration("WIDGET_APPROVE_TIMEOUT"); d != 45*time.Second || !ok {
		t.Errorf("own string key read as a duration = %v", d)
	}
	if n, _ := v.Int("WIDGET_RESERVE_HOLD_SECONDS"); n != 900 {
		t.Errorf("WIDGET_RESERVE_HOLD_SECONDS = %d", n)
	}
	if raw, ok := v.Raw("PG_PORT"); ok || raw != "" {
		t.Errorf("Raw is the source value before defaults: %q %v", raw, ok)
	}
	if !v.Has("PG_MIGRATION_HOST") || v.Has("OTEL_BASE_URL") {
		t.Error("Has follows defaults and default_from")
	}
}

func TestValuesPlatformNamesComeFromTheSource(t *testing.T) {
	v := mustLoad(t, widgetSchema(t), widgetEnv(t))
	if id, ok := v.String("COMPONENT_ID"); !ok || id != "conformance/widget" {
		t.Errorf("COMPONENT_ID = %q", id)
	}
	if p, ok := v.Int("PORT"); !ok || p != 8080 {
		t.Errorf("PORT = %d", p)
	}
	if _, ok := v.String("COMPONENT_VERSION"); ok {
		t.Error("COMPONENT_VERSION is absent in this source")
	}
	if e, ok := v.String("CONFORMANCE_PEER_GRPC_ENDPOINT"); !ok || e == "" {
		t.Error("an *_ENDPOINT is readable without declaration")
	}
}

func TestValuesProgrammingErrorsPanic(t *testing.T) {
	env := widgetEnv(t)
	env["WIDGET_APPROVE_TIMEOUT"] = "30" // a bare number is not a duration
	v := mustLoad(t, widgetSchema(t), env)
	cases := map[string]struct {
		f    func()
		want string
	}{
		"undeclared":           {func() { v.String("SOME_OTHER_KEY") }, ReasonUndeclared},
		"undeclared Has":       {func() { v.Has("PG_PASSWORD") }, ReasonUndeclared},
		"wrong getter":         {func() { v.Int("SHUTDOWN_GRACE") }, ReasonInvalid},
		"secret as int":        {func() { v.Int("PG_PASSWORD_FILE") }, ReasonInvalid},
		"own key bad duration": {func() { v.Duration("WIDGET_APPROVE_TIMEOUT") }, ReasonInvalid},
		"json on an int":       {func() { _, _ = v.JSON("PG_PORT", new(any)) }, ReasonInvalid},
	}
	for name, c := range cases {
		if got := panicReason(t, c.f); got != c.want {
			t.Errorf("%s: want panic %s, got %q", name, c.want, got)
		}
	}
	if got := panicReason(t, func() { v.String("PG_PASSWORD_FILE") }); got != "" {
		t.Errorf("String of a secret key returns its path, got panic %s", got)
	}
}

func TestParseHelpersForOwnStringKeys(t *testing.T) {
	if d, err := ParseDuration("K", "1h30m"); err != nil || d != 90*time.Minute {
		t.Errorf("ParseDuration = %v %v", d, err)
	}
	if _, err := ParseDuration("K", "-1s"); reasonOf(t, err) != ReasonInvalid {
		t.Error("negative duration")
	}
	if _, err := ParseInt("K", "+1"); reasonOf(t, err) != ReasonInvalid {
		t.Error("+1")
	}
	if e, _ := AsError(mustErr(ParseBool("K", "yes"))); e.Key != "K" {
		t.Errorf("error names the key: %v", e)
	}
}

func mustErr[T any](_ T, err error) error { return err }
