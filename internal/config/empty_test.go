package config

import "testing"

// P2.3 (stage-B ruling, vectors rc.2 empty-string-*): an empty value counts as not set for every
// format, plain strings included: the default applies, a required key is missing.
func TestEmptyStringCountsAsNotSet(t *testing.T) {
	def := "x"
	v, err := resolve(Decl{Name: "K", Format: FormatString, Default: &def}, "", true)
	if err != nil || !v.set || v.raw != "x" {
		t.Fatalf("empty string with a default: %+v %v", v, err)
	}
	_, err = resolve(Decl{Name: "K", Format: FormatString, Required: true}, "", true)
	if err == nil || err.Reason != ReasonMissing {
		t.Fatalf("empty required string: %v", err)
	}
	v, err = resolve(Decl{Name: "K", Format: FormatString}, "", true)
	if err != nil || v.set {
		t.Fatalf("empty optional string: %+v %v", v, err)
	}
}

// An empty default is no default, for a plain string too (nothing can make "" a value any more).
func TestEmptyDefaultIsNoValue(t *testing.T) {
	empty := ""
	v, err := resolve(Decl{Name: "K", Format: FormatString, Default: &empty}, "", false)
	if err != nil || v.set {
		t.Fatalf("empty default: %+v %v", v, err)
	}
}
