package jetstream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"
)

// testBus connects to TEST_NATS_URL, skipping the test when it is unset.
func testBus(t *testing.T) *Bus {
	t.Helper()
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL not set")
	}
	b, err := Connect(Options{URL: url, Name: "test/jetstream"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	deadline := time.Now().Add(5 * time.Second)
	for !b.Connected() {
		if time.Now().After(deadline) {
			t.Fatal("bus not connected after 5s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return b
}

// testStream creates a stream on a random first subject segment and deletes it after the test.
// It returns the segment and the stream name.
func testStream(t *testing.T, b *Bus) (seg, stream string) {
	t.Helper()
	var r [6]byte
	_, _ = rand.Read(r[:])
	seg = "t" + hex.EncodeToString(r[:])
	stream = "BE_" + strings.ToUpper(seg)
	ctx := context.Background()
	if err := b.EnsureStream(ctx, stream, []string{seg + ".>"}, StreamOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.js.DeleteStream(context.Background(), stream) })
	return seg, stream
}

func publishN(t *testing.T, b *Bus, subject string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		var r [8]byte
		_, _ = rand.Read(r[:])
		if _, err := b.Publish(context.Background(), Message{Subject: subject, ID: hex.EncodeToString(r[:]), Data: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
}

// consumeFor runs Consume for d, then cancels it and waits for it to return.
func consumeFor(t *testing.T, b *Bus, stream, durable string, conc int, d time.Duration, h func(context.Context, *Delivery)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := b.Consume(ctx, stream, durable, conc, h); err != nil && ctx.Err() == nil {
		t.Fatalf("Consume: %v", err)
	}
}
