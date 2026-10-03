package besdk

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// supervisor runs every piece of background work of one member (P1.7): a failure or panic is
// recovered, logged and the work restarts with exponential backoff from 1 s to 5 min; one piece
// stopping never stops another. Work returning nil before the context ends has finished and is not
// restarted. Standalone and in a shell the same supervisor is used.
type supervisor struct {
	ctx        context.Context
	log        *slog.Logger
	wg         sync.WaitGroup
	minBackoff time.Duration
	maxBackoff time.Duration
	// a run that lasted this long resets the backoff, so a rare failure restarts quickly
	resetAfter time.Duration
	onRestart  func(name string)
}

func newSupervisor(ctx context.Context, log *slog.Logger) *supervisor {
	return &supervisor{ctx: ctx, log: log, minBackoff: time.Second, maxBackoff: 5 * time.Minute,
		resetAfter: time.Minute}
}

// Go starts a named piece of work under supervision.
func (s *supervisor) Go(name string, run func(ctx context.Context) error) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		var backoff time.Duration
		for s.ctx.Err() == nil {
			started := time.Now()
			err := s.runOnce(name, run)
			if err == nil || s.ctx.Err() != nil {
				return
			}
			if time.Since(started) >= s.resetAfter {
				backoff = 0
			}
			backoff = s.nextBackoff(backoff)
			s.log.LogAttrs(s.ctx, slog.LevelError, "background work failed; restarting",
				slog.String("work", name), slog.String("error", err.Error()), slog.Duration("backoff", backoff))
			if s.onRestart != nil {
				s.onRestart(name)
			}
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(backoff):
			}
		}
	}()
}

func (s *supervisor) runOnce(name string, run func(ctx context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in %s: %v\n%s", name, r, debug.Stack())
		}
	}()
	return run(s.ctx)
}

func (s *supervisor) nextBackoff(prev time.Duration) time.Duration {
	if prev <= 0 {
		return s.minBackoff
	}
	return min(2*prev, s.maxBackoff)
}

// Wait blocks until every piece of work has returned (after the context ended).
func (s *supervisor) Wait() { s.wg.Wait() }

// syncWriter serialises writes to an io.Writer shared by goroutines.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}
