package besdk

import (
	"slices"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
)

func eventsValues(t *testing.T, env map[string]string) *config.Values {
	t.Helper()
	cat, err := config.LoadCatalogue()
	if err != nil {
		t.Fatal(err)
	}
	var decls []config.Decl
	for _, n := range []string{"EVENTS_MAX_DELIVER", "EVENTS_BACKOFF"} {
		k, _ := cat.Key(n)
		decls = append(decls, k.Decl())
	}
	v, errs := config.Load(config.NewSchema("erp/sales", "3.0.0", decls), func(k string) (string, bool) {
		s, ok := env[k]
		return s, ok
	})
	if len(errs) > 0 {
		t.Fatalf("load: %v", errs)
	}
	return v
}

// P12.5 (stage-B ruling): EVENTS_MAX_DELIVER / EVENTS_BACKOFF override a subscription only when set;
// absent (or empty), the subscription's own values apply, else the built-in 8 and 1s…1h.
func TestSubscriptionLimitsPrecedence(t *testing.T) {
	own := Subscription{MaxDeliver: 3, Backoff: []time.Duration{2 * time.Second}}
	md, bo := subscriptionLimits(eventsValues(t, nil), own)
	if md != 3 || !slices.Equal(bo, own.Backoff) {
		t.Fatalf("absent keys: %d %v", md, bo)
	}
	md, bo = subscriptionLimits(eventsValues(t, map[string]string{"EVENTS_MAX_DELIVER": "", "EVENTS_BACKOFF": ""}), own)
	if md != 3 || !slices.Equal(bo, own.Backoff) {
		t.Fatalf("empty keys: %d %v", md, bo)
	}
	md, bo = subscriptionLimits(eventsValues(t, map[string]string{"EVENTS_MAX_DELIVER": "5", "EVENTS_BACKOFF": "1s,2s"}), own)
	if md != 5 || !slices.Equal(bo, []time.Duration{time.Second, 2 * time.Second}) {
		t.Fatalf("set keys: %d %v", md, bo)
	}
	md, bo = subscriptionLimits(eventsValues(t, nil), Subscription{})
	if md != 0 || len(bo) != 0 {
		t.Fatalf("nothing set: %d %v (0 / empty = the built-in defaults, resolved by internal/events)", md, bo)
	}
}
