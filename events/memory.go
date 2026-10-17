package events

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Memory is an in-process transport: the reference the conformance suite is checked against, and a transport for
// module tests. It keeps every published message for the process's life.
type Memory struct {
	mu        sync.Mutex
	max       int
	streams   map[string]bool
	log       []Message
	seen      map[string]bool
	durables  map[string]*memDurable
	listeners map[int]*memListener
	nextID    int
	closed    bool
}

type memListener struct {
	subject string
	fn      func([]byte)
}

type memDurable struct {
	spec     SubscriptionSpec
	cursor   int // next log index to consider
	ready    []*memItem
	inflight map[*memItem]bool
	wake     chan struct{}
}

type memItem struct {
	msg       Message
	attempt   int
	deadline  time.Time // ack deadline while in flight
	notBefore time.Time
	done      bool
}

// NewMemory returns a transport accepting messages up to maxMessage bytes (0: 1 MiB).
func NewMemory(maxMessage int) *Memory {
	if maxMessage <= 0 {
		maxMessage = 1 << 20
	}
	return &Memory{max: maxMessage, streams: map[string]bool{}, seen: map[string]bool{},
		durables: map[string]*memDurable{}, listeners: map[int]*memListener{}}
}

// MaxMessage implements Transport.
func (m *Memory) MaxMessage() int { return m.max }

// EnsureStream implements Transport.
func (m *Memory) EnsureStream(_ context.Context, s Stream) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streams[s.Name] = true
	return nil
}

func size(msg Message) int {
	n := len(msg.Data) + len(msg.Subject)
	for k, v := range msg.Headers {
		n += len(k) + len(v)
	}
	return n
}

// Publish implements Transport: deduplicated by message ID.
func (m *Memory) Publish(_ context.Context, msg Message) error {
	if size(msg) > m.max {
		return ErrTooLarge
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("events: transport closed")
	}
	if id := msg.ID(); id != "" {
		if m.seen[id] {
			return nil
		}
		m.seen[id] = true
	}
	stored := Message{Subject: msg.Subject, Headers: map[string]string{}, Data: append([]byte(nil), msg.Data...)}
	for k, v := range msg.Headers {
		stored.Headers[k] = v
	}
	m.log = append(m.log, stored)
	for _, d := range m.durables {
		m.fill(d)
	}
	return nil
}

func (m *Memory) matches(spec SubscriptionSpec, subject string) bool {
	in := false
	for _, s := range spec.Streams {
		if SubjectMatches(streamPattern(s), subject) {
			in = true
		}
	}
	if !in {
		return false
	}
	if len(spec.Filters) == 0 {
		return true
	}
	for _, f := range spec.Filters {
		if SubjectMatches(f, subject) {
			return true
		}
	}
	return false
}

// fill moves new log entries into a durable's ready queue (lock held).
func (m *Memory) fill(d *memDurable) {
	for ; d.cursor < len(m.log); d.cursor++ {
		if msg := m.log[d.cursor]; m.matches(d.spec, msg.Subject) {
			d.ready = append(d.ready, &memItem{msg: msg})
		}
	}
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

type memSub struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *memSub) Stop() error { s.cancel(); <-s.done; return nil }

// Subscribe implements Transport: an existing durable keeps its settings; instances of one durable compete.
func (m *Memory) Subscribe(ctx context.Context, spec SubscriptionSpec, h Handler) (Subscription, error) {
	m.mu.Lock()
	d, ok := m.durables[spec.Durable]
	if !ok {
		d = &memDurable{spec: spec.withDefaults(), inflight: map[*memItem]bool{}, wake: make(chan struct{}, 1)}
		m.durables[spec.Durable] = d
		m.fill(d)
	}
	m.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	sub := &memSub{cancel: cancel, done: make(chan struct{})}
	var wg sync.WaitGroup
	for i := 0; i < d.spec.Concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); m.work(ctx, d, h) }()
	}
	go func() { wg.Wait(); close(sub.done) }()
	return sub, nil
}

// next takes the next deliverable item (lock held): ready and due, or in flight past its ack deadline.
func (m *Memory) next(d *memDurable, now time.Time) *memItem {
	for it := range d.inflight {
		if now.After(it.deadline) {
			delete(d.inflight, it)
			if it.attempt < d.spec.MaxDeliver {
				it.notBefore = now
				d.ready = append(d.ready, it)
			}
		}
	}
	for i, it := range d.ready {
		if !it.notBefore.After(now) {
			d.ready = append(d.ready[:i], d.ready[i+1:]...)
			it.attempt++
			it.deadline = now.Add(d.spec.AckWait)
			d.inflight[it] = true
			return it
		}
	}
	return nil
}

func (m *Memory) work(ctx context.Context, d *memDurable, h Handler) {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		m.mu.Lock()
		it := m.next(d, time.Now())
		var attempt int
		if it != nil {
			attempt = it.attempt
		}
		m.mu.Unlock()
		if it == nil {
			select {
			case <-ctx.Done():
				return
			case <-d.wake:
			case <-tick.C:
			}
			continue
		}
		del := &Delivery{Message: it.msg, Attempt: attempt, InProgress: func() error {
			m.mu.Lock()
			defer m.mu.Unlock()
			if d.inflight[it] && it.attempt == attempt {
				it.deadline = time.Now().Add(d.spec.AckWait)
			}
			return nil
		}}
		out := h(ctx, del)
		m.mu.Lock()
		if d.inflight[it] && it.attempt == attempt { // a late outcome of an expired delivery changes nothing
			delete(d.inflight, it)
			if out == Nak && it.attempt < d.spec.MaxDeliver {
				it.notBefore = time.Now().Add(d.spec.BackoffFor(it.attempt))
				d.ready = append(d.ready, it)
			}
		}
		m.mu.Unlock()
		if ctx.Err() != nil {
			return
		}
	}
}

// Notify implements Transport.
func (m *Memory) Notify(_ context.Context, subject string, data []byte) error {
	m.mu.Lock()
	var fns []func([]byte)
	for _, l := range m.listeners {
		if SubjectMatches(l.subject, subject) {
			fns = append(fns, l.fn)
		}
	}
	m.mu.Unlock()
	for _, fn := range fns {
		fn(append([]byte(nil), data...))
	}
	return nil
}

type memListen struct {
	m  *Memory
	id int
}

func (l memListen) Stop() error {
	l.m.mu.Lock()
	delete(l.m.listeners, l.id)
	l.m.mu.Unlock()
	return nil
}

// Listen implements Transport.
func (m *Memory) Listen(_ context.Context, subject string, fn func([]byte)) (Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	m.listeners[m.nextID] = &memListener{subject: subject, fn: fn}
	return memListen{m, m.nextID}, nil
}

// Close implements Transport.
func (m *Memory) Close() error { m.mu.Lock(); m.closed = true; m.mu.Unlock(); return nil }
