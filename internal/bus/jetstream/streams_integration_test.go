package jetstream

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// P12.4: create if missing, never change if present.
func TestEnsureStreamNeverUpdates(t *testing.T) {
	b := testBus(t)
	seg, stream := testStream(t, b)
	ctx := context.Background()
	s, err := b.js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	c := s.CachedInfo().Config
	if c.MaxAge != 7*24*time.Hour || c.MaxBytes != 1<<30 || c.Duplicates != 10*time.Minute || c.Storage != jetstream.FileStorage {
		t.Fatalf("created with wrong config: %+v", c)
	}
	if err := b.EnsureStream(ctx, stream, []string{seg + ".>"}, StreamOptions{MaxAge: time.Hour}); err != nil {
		t.Fatal(err)
	}
	s, _ = b.js.Stream(ctx, stream)
	if got := s.CachedInfo().Config.MaxAge; got != 7*24*time.Hour {
		t.Fatalf("existing stream changed: MaxAge %s", got)
	}
}

// P12.5: create-only durable; an identical second Ensure is a no-op, an operator's change is
// reported and left unchanged.
func TestEnsureDurableCreateOnly(t *testing.T) {
	b := testBus(t)
	seg, stream := testStream(t, b)
	ctx := context.Background()
	subj := seg + ".order.created.v1"
	name := "erp_finance__" + strings.ReplaceAll(subj, ".", "__")
	w, err := b.EnsureDurable(ctx, stream, name, subj)
	if err != nil || len(w) != 0 {
		t.Fatalf("first Ensure: warnings %v err %v", w, err)
	}
	w, err = b.EnsureDurable(ctx, stream, name, subj)
	if err != nil || len(w) != 0 {
		t.Fatalf("second Ensure: warnings %v err %v", w, err)
	}
	changed := durableConfig(name, subj, 0)
	changed.MaxAckPending = 100
	if _, err := b.js.UpdateConsumer(ctx, stream, changed); err != nil {
		t.Fatal(err)
	}
	w, err = b.EnsureDurable(ctx, stream, name, subj)
	if err != nil || len(w) != 1 || !strings.Contains(w[0], "max_ack_pending") {
		t.Fatalf("after operator change: warnings %v err %v", w, err)
	}
	c, _ := b.js.Consumer(ctx, stream, name)
	if got := c.CachedInfo().Config.MaxAckPending; got != 100 {
		t.Fatalf("operator change overwritten: MaxAckPending %d", got)
	}
}

func TestEnsureDLQStream(t *testing.T) {
	b := testBus(t)
	ctx := context.Background()
	if err := b.EnsureDLQStream(ctx); err != nil {
		t.Fatal(err)
	}
	s, err := b.js.Stream(ctx, DLQStream)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.CachedInfo().Config.Subjects; len(got) != 1 || got[0] != "dlq.>" {
		t.Fatalf("DLQ subjects %v", got)
	}
	// BE_DLQ is shared by every lane on this server: it is left in place.
}
