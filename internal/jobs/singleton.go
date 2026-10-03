package jobs

import (
	"context"
	"fmt"
	"time"
)

// singletonLoop runs en with at most one holder across replicas and processes (P14 "singleton"):
// it waits for the lease (polling every TTL/3), then, while holding it, runs at once and every
// Interval. A lost lease cancels the run and the loop goes back to waiting; a failed run releases the
// lease and ends the loop with an error, so the supervisor restarts it with backoff (P1.7).
func (e *Engine) singletonLoop(en *entry) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		holder := e.cfg.holder()
		for ctx.Err() == nil {
			l, ok, err := e.acquire(ctx, en.name, holder)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("job %s: take lease: %w", en.name, err)
			}
			if !ok {
				if !sleep(ctx, e.cfg.leaseTTL()/3) {
					return nil
				}
				continue
			}
			err = e.holdAndRun(l, en)
			l.release()
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// holdAndRun runs en every Interval while l is held; nil when the lease is lost or the loop stops.
func (e *Engine) holdAndRun(l *lease, en *entry) error {
	for {
		start := time.Now()
		info := e.info(en, false)
		info.Epoch = l.epoch
		res, err := e.exec(l.ctx, info, en.timeout, en.run)
		switch res {
		case ResultOK:
		case ResultLeaseLost, ResultCancelled:
			return nil
		default:
			return fmt.Errorf("job %s: %s: %w", en.name, res, err)
		}
		if !sleep(l.ctx, en.interval-time.Since(start)) {
			return nil
		}
	}
}
