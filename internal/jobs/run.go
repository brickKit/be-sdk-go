package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// errLeaseLost is the cancellation cause of a singleton run whose lease was lost.
var errLeaseLost = errors.New("jobs: lease lost")

// maxErrorText bounds last_error and slot results; never a payload.
const maxErrorText = 200

// exec runs fn once as the run described by info (P14.2): its context carries the run and is
// cancelled at the timeout; Wrap (span, log field, origin) goes around it; a panic becomes an error;
// the result is counted and timed (P14.3) and a failure logged. The returned error is non-nil exactly
// when the result is not ResultOK.
func (e *Engine) exec(parent context.Context, info RunInfo, timeout time.Duration, fn func(ctx context.Context) error) (string, error) {
	info.StartedAt = e.cfg.now()
	ctx, cancel := context.WithTimeout(withRun(parent, info), timeout)
	defer cancel()
	start := time.Now()
	err := e.call(ctx, info, fn)
	took := time.Since(start)
	result := classify(parent, ctx, err)
	e.m.observe(info.Name, result, took, e.cfg.now())
	if err != nil {
		level := slog.LevelError
		if result == ResultCancelled || result == ResultLeaseLost {
			level = slog.LevelWarn
		}
		e.log.LogAttrs(context.WithoutCancel(ctx), level, "job run did not succeed", slog.String("job", info.Name),
			slog.String("kind", info.Kind), slog.String("result", result), slog.String("error", err.Error()),
			slog.Duration("took", took))
	}
	return result, err
}

// call runs fn through Wrap, turning a panic into an error (P1.7).
func (e *Engine) call(ctx context.Context, info RunInfo, fn func(ctx context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in job %s: %v\n%s", info.Name, r, debug.Stack())
		}
	}()
	if e.cfg.Wrap != nil {
		return e.cfg.Wrap(ctx, info, fn)
	}
	return fn(ctx)
}

// classify names a run's result: ok when fn returned nil; otherwise lease_lost or cancelled when the
// surrounding context ended, timeout when the run's own deadline did, else error.
func classify(parent, ctx context.Context, err error) string {
	switch {
	case err == nil:
		return ResultOK
	case parent.Err() != nil && errors.Is(context.Cause(parent), errLeaseLost):
		return ResultLeaseLost
	case parent.Err() != nil:
		return ResultCancelled
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return ResultTimeout
	}
	return ResultError
}

// tx runs fn in one short transaction of the member's store.
func (e *Engine) tx(ctx context.Context, fn func(ctx context.Context, tx *pg.Tx) error) error {
	return e.cfg.Store.Run(ctx, pg.TxOptions{}, fn)
}

// detached is a short context that survives the loop's cancellation, for recording a result while
// the process stops.
func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
}

// sleep waits d; false when ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// errorText is err's text cut to maxErrorText bytes of valid UTF-8.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > maxErrorText {
		s = strings.ToValidUTF8(s[:maxErrorText], "")
	}
	return s
}
