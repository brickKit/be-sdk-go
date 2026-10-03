package jetstream

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// P12.1: a message is published only after the PubAck; Nats-Msg-Id = ID drops a duplicate.
func TestPublishWaitsForAckAndDedups(t *testing.T) {
	b := testBus(t)
	seg, stream := testStream(t, b)
	ctx := context.Background()
	m := Message{Subject: seg + ".order.created.v1", ID: "0190-a", Header: map[string]string{"ce-id": "0190-a", "Ce-Mixed": "v"}, Data: []byte(`{"n":1}`)}
	a, err := b.Publish(ctx, m)
	if err != nil || a.Stream != stream || a.Sequence != 1 || a.Duplicate {
		t.Fatalf("first publish: %+v %v", a, err)
	}
	a, err = b.Publish(ctx, m)
	if err != nil || !a.Duplicate {
		t.Fatalf("second publish: %+v %v, want a duplicate", a, err)
	}
	s, _ := b.js.Stream(ctx, stream)
	info, _ := s.Info(ctx)
	if info.State.Msgs != 1 {
		t.Fatalf("stream holds %d messages, want 1", info.State.Msgs)
	}
	raw, err := s.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if raw.Header.Get("Nats-Msg-Id") != "0190-a" || raw.Header.Get("ce-id") != "0190-a" || raw.Header.Get("Ce-Mixed") != "v" {
		t.Fatalf("headers not verbatim: %v", raw.Header)
	}
}

// P12.1: a batch is published with per-message results; duplicates in the batch are not errors.
func TestPublishBatch(t *testing.T) {
	b := testBus(t)
	seg, stream := testStream(t, b)
	ctx := context.Background()
	var ms []Message
	for i := 0; i < 600; i++ {
		ms = append(ms, Message{Subject: seg + ".order.created.v1", ID: fmt.Sprintf("id-%d", i%500), Data: []byte(`{}`)})
	}
	ms = append(ms, Message{Subject: "nostream.order.created.v1", ID: "orphan"})
	errs := b.PublishBatch(ctx, ms)
	if len(errs) != len(ms) {
		t.Fatalf("got %d results for %d messages", len(errs), len(ms))
	}
	for i, err := range errs[:600] {
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
	}
	if errs[600] == nil {
		t.Fatal("message without a stream: want an error")
	}
	s, _ := b.js.Stream(ctx, stream)
	info, _ := s.Info(ctx)
	if info.State.Msgs != 500 {
		t.Fatalf("stream holds %d messages, want 500", info.State.Msgs)
	}
}

// P12.10: best-effort core signals.
func TestSignal(t *testing.T) {
	b := testBus(t)
	subj := fmt.Sprintf("t%p.authz.changed.v1", b)
	got := make(chan []byte, 1)
	unsub, err := b.OnSignal(subj, func(d []byte) { got <- d })
	if err != nil {
		t.Fatal(err)
	}
	defer unsub()
	if err := b.Signal(subj, []byte("poke")); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-got:
		if string(d) != "poke" {
			t.Fatalf("signal data %q", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("signal not received")
	}
}
