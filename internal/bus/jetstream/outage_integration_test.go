package jetstream

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tcpProxy forwards a fixed local port to the test NATS server and can be cut and restored to
// simulate a bus outage without touching the shared container.
type tcpProxy struct {
	addr, target string
	mu           sync.Mutex
	ln           net.Listener
	conns        []net.Conn
}

func (p *tcpProxy) start(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", p.target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, u)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(u, c); _ = u.Close() }()
			go func() { _, _ = io.Copy(c, u); _ = c.Close() }()
		}
	}()
}

func (p *tcpProxy) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// P12.13 + P12.5: a bus down at start is connected in the background; Consume survives an outage
// and resumes after the reconnect; Publish during the outage fails with ErrUnavailable.
func TestSurvivesOutage(t *testing.T) {
	direct := testBus(t)
	target := strings.TrimPrefix(os.Getenv("TEST_NATS_URL"), "nats://")
	p := &tcpProxy{addr: strings.TrimPrefix(closedPortURL(t), "nats://"), target: target}
	defer p.stop()

	b, err := Connect(Options{URL: "nats://" + p.addr, Name: "test/outage"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	p.start(t)
	waitFor(t, "first connect", 6*time.Second, b.Connected)

	seg, stream := testStream(t, direct)
	subj := seg + ".order.created.v1"
	d := durableFor(subj)
	if _, err := b.EnsureDurable(context.Background(), stream, d, subj); err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Consume(ctx, stream, d, 4, func(ctx context.Context, m *Delivery) { n.Add(1); _ = m.Ack(ctx) })
	}()
	publishN(t, direct, subj, 1)
	waitFor(t, "first message", 3*time.Second, func() bool { return n.Load() == 1 })

	p.stop()
	waitFor(t, "disconnect", 3*time.Second, func() bool { return !b.Connected() })
	if _, err := b.Publish(context.Background(), Message{Subject: subj, ID: "during-outage"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Publish during outage: %v, want ErrUnavailable", err)
	}
	publishN(t, direct, subj, 1)
	time.Sleep(500 * time.Millisecond)
	p.start(t)
	waitFor(t, "reconnect", 6*time.Second, b.Connected)
	waitFor(t, "message published during the outage", 6*time.Second, func() bool { return n.Load() == 2 })
	cancel()
	<-done
}

// Consume keeps retrying while its durable does not exist yet, and consumes once it does.
func TestConsumeWaitsForDurable(t *testing.T) {
	b := testBus(t)
	seg, stream := testStream(t, b)
	subj := seg + ".order.created.v1"
	d := durableFor(subj)
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Consume(ctx, stream, d, 4, func(ctx context.Context, m *Delivery) { n.Add(1); _ = m.Ack(ctx) })
	}()
	time.Sleep(300 * time.Millisecond)
	publishN(t, b, subj, 1)
	if _, err := b.EnsureDurable(context.Background(), stream, d, subj); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "message after durable creation", 4*time.Second, func() bool { return n.Load() == 1 })
	cancel()
	<-done
}
