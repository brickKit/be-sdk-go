package jetstream

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// closedPortURL returns a nats:// URL on a local port nothing listens on.
func closedPortURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return "nats://" + addr
}

func TestConnectRequiresURL(t *testing.T) {
	if _, err := Connect(Options{Name: "erp/sales"}); err == nil {
		t.Fatal("Connect without URL: want an error")
	}
}

// P12.13: a bus that is down at start is retried in the background; Connect still returns.
// P12.1: publishing while the bus is unavailable fails fast with a recognisable error.
func TestConnectAndPublishWhileServerDown(t *testing.T) {
	start := time.Now()
	b, err := Connect(Options{URL: closedPortURL(t), Name: "erp/sales"})
	if err != nil {
		t.Fatalf("Connect with server down: %v", err)
	}
	defer b.Close()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Connect took %s, want it to return at once", d)
	}
	if b.Connected() {
		t.Fatal("Connected() = true with no server")
	}
	start = time.Now()
	_, err = b.Publish(context.Background(), Message{Subject: "x.y.z.v1", ID: "id-1", Data: []byte(`{}`)})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Publish error = %v, want ErrUnavailable", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Publish took %s, want a quick failure", d)
	}
	errs := b.PublishBatch(context.Background(), []Message{{Subject: "x.y.z.v1", ID: "a"}, {Subject: "x.y.z.v1", ID: "b"}})
	if len(errs) != 2 || !errors.Is(errs[0], ErrUnavailable) || !errors.Is(errs[1], ErrUnavailable) {
		t.Fatalf("PublishBatch errors = %v, want two ErrUnavailable", errs)
	}
	if err := b.Signal("infra.authz.changed.v1", nil); err == nil {
		t.Fatal("Signal while down: want an error")
	}
}

// P12.4 defaults.
func TestStreamConfigDefaults(t *testing.T) {
	c := streamConfig("BE_ERP", []string{"erp.>"}, StreamOptions{})
	if c.Name != "BE_ERP" || len(c.Subjects) != 1 || c.Subjects[0] != "erp.>" {
		t.Fatalf("name/subjects = %q %v", c.Name, c.Subjects)
	}
	if c.MaxAge != 7*24*time.Hour || c.MaxBytes != 1<<30 || c.Discard != jetstream.DiscardOld ||
		c.Duplicates != 10*time.Minute || c.Storage != jetstream.FileStorage || c.Replicas != 1 {
		t.Fatalf("defaults wrong: %+v", c)
	}
	o := streamConfig("BE_X", []string{"x.>"}, StreamOptions{MaxAge: time.Hour, MaxBytes: 5, DuplicateWindow: time.Second, Replicas: 3})
	if o.MaxAge != time.Hour || o.MaxBytes != 5 || o.Duplicates != time.Second || o.Replicas != 3 {
		t.Fatalf("overrides ignored: %+v", o)
	}
	d := dlqStreamConfig()
	if d.Name != DLQStream || d.Subjects[0] != "dlq.>" || d.MaxAge != 30*24*time.Hour || d.Duplicates != 10*time.Minute {
		t.Fatalf("DLQ stream config: %+v", d)
	}
}

// P12.5 server-side configuration.
func TestDurableConfig(t *testing.T) {
	c := durableConfig("erp_finance__sales__order__created__v1", "sales.order.created.v1", 0)
	if c.Durable != "erp_finance__sales__order__created__v1" || c.FilterSubject != "sales.order.created.v1" {
		t.Fatalf("name/filter: %+v", c)
	}
	if c.AckPolicy != jetstream.AckExplicitPolicy || c.AckWait != 30*time.Second || c.MaxAckPending != 256 ||
		c.MaxDeliver != -1 || len(c.BackOff) != 0 || c.DeliverPolicy != jetstream.DeliverAllPolicy ||
		c.InactiveThreshold != 30*24*time.Hour || c.DeliverSubject != "" {
		t.Fatalf("constants wrong: %+v", c)
	}
	if s := durableConfig("d", "a.b.c.v1", time.Second); s.AckWait != time.Second {
		t.Fatalf("test AckWait override ignored: %s", s.AckWait)
	}
}

func TestDurableDrift(t *testing.T) {
	want := durableConfig("d", "a.b.c.v1", 0)
	if w := durableDrift(want, want); len(w) != 0 {
		t.Fatalf("identical config: warnings %v", w)
	}
	got := want
	got.MaxAckPending = 100
	got.BackOff = []time.Duration{time.Second}
	got.MaxDeliver = 8
	got.AckWait = time.Second
	got.FilterSubject = "a.b.d.v1"
	got.InactiveThreshold = time.Hour
	got.DeliverPolicy = jetstream.DeliverNewPolicy
	got.AckPolicy = jetstream.AckAllPolicy
	w := durableDrift(want, got)
	joined := strings.Join(w, "\n")
	for _, f := range []string{"max_ack_pending", "backoff", "max_deliver", "ack_wait", "filter_subject", "inactive_threshold", "deliver_policy", "ack_policy"} {
		if !strings.Contains(joined, f) {
			t.Errorf("no warning for %s in %q", f, joined)
		}
	}
	if len(w) != 8 {
		t.Errorf("want 8 warnings, got %d: %v", len(w), w)
	}
}
