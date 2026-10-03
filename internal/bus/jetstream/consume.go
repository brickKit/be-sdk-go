package jetstream

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/semaphore"
)

// Consume loop constants.
const (
	DefaultConcurrency = 4 // P12.9: up to 4 messages handled at once per subscription
	// fetchWait bounds one pull request; it is also the longest Consume waits after cancellation
	// before it stops fetching.
	fetchWait = time.Second
	// retryDelay is the pause after a failed lookup or fetch while the bus is unavailable.
	retryDelay = time.Second
)

// Handler handles one delivery and settles it (Ack, NakWithDelay or Term).
type Handler func(ctx context.Context, d *Delivery)

// Consume pulls from the durable and runs h on each message, at most concurrency handlers at once
// (zero or negative means DefaultConcurrency; P12.9). It survives bus outages, retrying and logging,
// and returns nil once ctx is cancelled and every handler it started has returned. A handler that
// panics is logged and its delivery left unsettled, so it is redelivered after ack_wait like a crash.
//
// Decision tree per iteration: no consumer handle → look it up (failure: log, pause, retry); then
// reserve one free slot (blocking) plus any other free slots, fetch that many; fetch failure →
// release the slots, log, pause (a deleted durable is looked up again); each message fetched runs in
// its own slot, unless ctx was cancelled meanwhile, in which case it is nak'ed for prompt redelivery.
func (b *Bus) Consume(ctx context.Context, stream, durable string, concurrency int, h Handler) error {
	if h == nil {
		return errors.New("jetstream: Consume: nil handler")
	}
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	slots := semaphore.NewWeighted(int64(concurrency))
	var wg sync.WaitGroup
	defer wg.Wait()
	log := b.log.With("stream", stream, "durable", durable)
	var cons jetstream.Consumer
	failing := false
	fail := func(what string, err error) {
		if !failing {
			log.Warn("event consume: "+what+" failed, retrying", "error", err)
		}
		failing = true
		pause(ctx, retryDelay)
	}
	for ctx.Err() == nil {
		if cons == nil {
			c, err := b.js.Consumer(ctx, stream, durable)
			if err != nil {
				fail("durable lookup", err)
				continue
			}
			cons = c
		}
		n, err := acquireSlots(ctx, slots, concurrency)
		if err != nil {
			break // ctx cancelled
		}
		batch, err := cons.Fetch(n, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			slots.Release(int64(n))
			cons = forgetIfGone(cons, err)
			fail("fetch", err)
			continue
		}
		used := b.dispatch(ctx, batch, slots, &wg, h)
		slots.Release(int64(n - used))
		if err := batch.Error(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			cons = forgetIfGone(cons, err)
			fail("fetch", err)
			continue
		}
		if failing {
			log.Info("event consume resumed")
			failing = false
		}
	}
	return nil
}

// dispatch starts one handler per fetched message, each holding one of the slots reserved for the
// fetch, and returns how many slots it used.
func (b *Bus) dispatch(ctx context.Context, batch jetstream.MessageBatch, slots *semaphore.Weighted, wg *sync.WaitGroup, h Handler) int {
	used := 0
	for m := range batch.Messages() {
		used++
		if ctx.Err() != nil {
			_ = m.Nak()
			slots.Release(1)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer slots.Release(1)
			b.handle(ctx, m, h)
		}()
	}
	return used
}

// handle builds the Delivery and runs h, recovering a panic so one handler never takes the
// process (and every member of a shell) down.
func (b *Bus) handle(ctx context.Context, m jetstream.Msg, h Handler) {
	md, err := m.Metadata()
	if err != nil {
		b.log.Error("event consume: message without JetStream metadata", "subject", m.Subject(), "error", err)
		_ = m.Nak()
		return
	}
	defer func() {
		if r := recover(); r != nil {
			b.log.Error("event handler panicked; delivery left for redelivery", "subject", m.Subject(),
				"stream_seq", md.Sequence.Stream, "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
		}
	}()
	h(ctx, newDelivery(m, md))
}

// acquireSlots blocks for one free slot, then takes every other free slot without blocking.
func acquireSlots(ctx context.Context, slots *semaphore.Weighted, max int) (int, error) {
	if err := slots.Acquire(ctx, 1); err != nil {
		return 0, err
	}
	n := 1
	for n < max && slots.TryAcquire(1) {
		n++
	}
	return n, nil
}

// forgetIfGone drops the consumer handle when the durable no longer exists, so it is looked up
// again (and the lookup failure logged) instead of fetching from a deleted durable forever.
func forgetIfGone(c jetstream.Consumer, err error) jetstream.Consumer {
	if errors.Is(err, jetstream.ErrConsumerDeleted) || errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil
	}
	return c
}

// pause waits d or until ctx is done.
func pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
