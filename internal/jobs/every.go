package jobs

import (
	"context"
	"fmt"
	"time"
)

// everyLoop runs en on this replica on its own timer (P14 "every"): at once, then Interval after
// each start (no overlap: a run longer than the interval is followed directly by the next). A failed
// run ends the loop with an error, so the supervisor restarts it with backoff (P1.7).
func (e *Engine) everyLoop(en *entry) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		for {
			start := time.Now()
			res, err := e.exec(ctx, e.info(en, false), en.timeout, en.run)
			if ctx.Err() != nil {
				return nil
			}
			if res != ResultOK {
				return fmt.Errorf("job %s: %s: %w", en.name, res, err)
			}
			if !sleep(ctx, en.interval-time.Since(start)) {
				return nil
			}
		}
	}
}
