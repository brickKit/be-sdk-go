package pgqueue

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/jackc/pgx/v5"
)

// Waking the readers (P6.30): a publish NOTIFYs wakeChan with the message's subject in the statement that inserts it;
// one dedicated connection per transport LISTENs and wakes the idle readers of the subscriptions the subject matches.
// The poll stays: while the connection is down, behind a pooler in transaction mode (no LISTEN there), and for rows
// held back by the xmin barrier (P6.23). Without the wake every message waited half a poll at low load (measured:
// arrival p50 about 55 ms at a 100 ms poll, against about 3 ms on JetStream).

const (
	wakeChan     = "be_bus_wake"
	relistenMax  = 30 * time.Second
	relistenFrom = time.Second
)

// waker is one subscription's wake signal: closed and replaced on every matching notification, so a reader that took
// it before its claim sees a wake that came during the claim.
type waker struct {
	spec events.SubscriptionSpec
	mu   sync.Mutex
	ch   chan struct{}
}

func (w *waker) current() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ch
}

func (w *waker) fire() {
	w.mu.Lock()
	close(w.ch)
	w.ch = make(chan struct{})
	w.mu.Unlock()
}

// register adds a subscription's waker and starts the listener with the first one.
func (t *Transport) register(spec events.SubscriptionSpec) *waker {
	w := &waker{spec: spec, ch: make(chan struct{})}
	t.wakeMu.Lock()
	t.wakers = append(t.wakers, w)
	t.wakeMu.Unlock()
	t.listenOnce.Do(func() {
		t.listening.Store(true)
		go t.listen()
	})
	return w
}

func (t *Transport) unregister(w *waker) {
	t.wakeMu.Lock()
	t.wakers = slices.DeleteFunc(t.wakers, func(x *waker) bool { return x == w })
	t.wakeMu.Unlock()
}

// wake fires the wakers of the subscriptions subject belongs to.
func (t *Transport) wake(subject string) {
	t.wakeMu.Lock()
	defer t.wakeMu.Unlock()
	for _, w := range t.wakers {
		if matches(w.spec, subject) {
			w.fire()
		}
	}
}

// listen keeps a LISTEN connection until the transport closes, reconnecting with a growing pause; every connect wakes
// all readers once, for what was published while it was down.
func (t *Transport) listen() {
	defer close(t.listened)
	pause := relistenFrom
	for t.closing.Err() == nil {
		listened, err := t.listenConn()
		if t.closing.Err() != nil {
			return
		}
		if listened {
			pause = relistenFrom
		}
		t.report(fmt.Errorf("bus wake listener (the poll carries on): %w", err))
		select {
		case <-t.closing.Done():
			return
		case <-time.After(pause):
		}
		pause = min(pause*2, relistenMax)
	}
}

// listenConn listens on one connection until it fails; listened reports whether LISTEN took effect.
func (t *Transport) listenConn() (listened bool, err error) {
	conn, err := pgx.ConnectConfig(t.closing, t.cfg.Copy())
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(t.closing, "LISTEN "+wakeChan); err != nil {
		return false, err
	}
	t.wakeAll()
	for {
		n, err := conn.WaitForNotification(t.closing)
		if err != nil {
			return true, err
		}
		t.wake(n.Payload)
	}
}

func (t *Transport) wakeAll() {
	t.wakeMu.Lock()
	defer t.wakeMu.Unlock()
	for _, w := range t.wakers {
		w.fire()
	}
}

// idle waits a poll, or less when the subscription is woken or the reader stops.
func idle(ctx context.Context, d time.Duration, woken <-chan struct{}) {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
	case <-tm.C:
	case <-woken:
	}
}
