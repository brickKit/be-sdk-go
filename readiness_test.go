package besdk

import (
	"reflect"
	"testing"
)

func TestReadinessLatches(t *testing.T) {
	r := newReadiness("bundle", "db_identity", "migrations")
	if r.Ready() || !reflect.DeepEqual(r.Waiting(), []string{"bundle", "db_identity", "migrations"}) {
		t.Fatalf("fresh: ready=%v waiting=%v", r.Ready(), r.Waiting())
	}
	r.Set("db_identity", true)
	r.Set("migrations", true)
	if r.Ready() || !reflect.DeepEqual(r.Waiting(), []string{"bundle"}) {
		t.Fatalf("partial: %v", r.Waiting())
	}
	r.Set("migrations", false) // a condition may still regress before the process is ready
	if !reflect.DeepEqual(r.Waiting(), []string{"bundle", "migrations"}) {
		t.Fatalf("regress before ready: %v", r.Waiting())
	}
	r.Set("migrations", true)
	r.Set("bundle", true)
	if !r.Ready() || len(r.Waiting()) != 0 {
		t.Fatalf("all met: ready=%v waiting=%v", r.Ready(), r.Waiting())
	}
	r.Set("db_identity", false) // once ready, an outage never turns /readyz back (P1.4)
	if !r.Ready() {
		t.Fatal("readiness must latch")
	}
}

func TestReadinessWithNoConditionsIsReady(t *testing.T) {
	if r := newReadiness(); !r.Ready() {
		t.Fatal("a component without database or protected routes is ready at once")
	}
}

func TestReadinessUnknownConditionIgnored(t *testing.T) {
	r := newReadiness("bundle")
	r.Set("db_identity", true)
	if r.Ready() {
		t.Fatal("setting an undeclared condition must not make it ready")
	}
}
