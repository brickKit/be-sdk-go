package pgqueue

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/jackc/pgx/v5"
)

// Waking the readers (P6.30): once a publish's insert has committed, the transport's notifier NOTIFYs wakeChan with the
// message's subject in a statement of its own (see Publish and notifyLoop); one dedicated connection per transport
// LISTENs and wakes the idle readers of the subscriptions the subject matches. The poll stays: while the connection is
// down, behind a pooler in transaction mode (no LISTEN there), and for rows held back by the xmin barrier (P6.23).
// Without the wake every message waited half a poll at low load (measured: arrival p50 about 55 ms at a 100 ms poll,
// against about 3 ms on JetStream).

const (
	wakeChan     = "be_bus_wake"
	relistenMax  = 30 * time.Second
	relistenFrom = time.Second
	notifyQueue  = 4096 // NOTIFYs waiting to be sent; past it a publish's NOTIFY is dropped and the poll reads the message
)

// notifyLoop sends the publishes' NOTIFYs, one a new message in the order they were queued, off the publish path.
// Not in the inserting transaction: a transaction that NOTIFYs takes an instance-wide lock at commit and holds it until
// its commit record is flushed, so concurrent publishes would commit one at a time (measured with 8 publishers: about
// 3 500 messages a second against about 10 000); a NOTIFY alone writes nothing and holds it briefly. Not on the publish
// path either: a publisher that publishes in order (the forwarder) would wait a second round trip a message (measured:
// a Python publisher at 200 a second went from about 13 to 19 ms). One notification a message, never merged: each
// wakes one idle reader (waker). A NOTIFY that fails is reported; the message is stored and the poll reads it.
func (t *Transport) notifyLoop() {
	defer close(t.notified)
	for {
		select {
		case <-t.closing.Done():
			return
		case subject := <-t.notes:
			_, err := t.pool.Exec(t.closing, "SELECT pg_notify($1, $2)", wakeChan, subject)
			if err != nil && t.closing.Err() == nil {
				t.report(fmt.Errorf("bus wake after a publish (the poll reads the message): %w", err))
			}
		}
	}
}

// notify queues a new message's NOTIFY; a full queue drops it.
func (t *Transport) notify(subject string) {
	select {
	case t.notes <- subject:
	default:
	}
}

// waker is one subscription's wake signal: a token per matching notification, at most one per reader, so one message
// wakes one idle reader rather than all of them (waking all made every reader claim and advance for one message). A
// token left while every reader is busy is taken at the next idle round, so a notification during a claim is not lost.
type waker struct {
	spec   events.SubscriptionSpec
	tokens chan struct{}
}

func (w *waker) fire() {
	select {
	case w.tokens <- struct{}{}:
	default: // every reader already has a wake waiting
	}
}

// register adds a subscription's waker and starts the listener with the first one.
func (t *Transport) register(spec events.SubscriptionSpec) *waker {
	w := &waker{spec: spec, tokens: make(chan struct{}, max(spec.Concurrency, 1))}
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

// wake gives one token to each subscription subject belongs to.
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

// wakeAll wakes every reader of every subscription once.
func (t *Transport) wakeAll() {
	t.wakeMu.Lock()
	defer t.wakeMu.Unlock()
	for _, w := range t.wakers {
		for range cap(w.tokens) {
			w.fire()
		}
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
