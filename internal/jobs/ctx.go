package jobs

import (
	"context"
	"time"
)

type runKey struct{}

// withRun marks ctx as belonging to one run.
func withRun(ctx context.Context, info RunInfo) context.Context {
	return context.WithValue(ctx, runKey{}, info)
}

// RunOf returns the run ctx belongs to, if any.
func RunOf(ctx context.Context) (RunInfo, bool) {
	info, ok := ctx.Value(runKey{}).(RunInfo)
	return info, ok
}

// Epoch is the fencing token of the singleton run ctx belongs to (P14 "singleton"): a write that
// must not happen twice checks it against the value it stored. ok is false outside a singleton run.
func Epoch(ctx context.Context) (epoch int64, ok bool) {
	info, ok := RunOf(ctx)
	if !ok || info.Kind != string(kindSingleton) {
		return 0, false
	}
	return info.Epoch, true
}

// Slot is the slot of the cron run ctx belongs to: a daily job derives its business date from it.
func Slot(ctx context.Context) (slot time.Time, ok bool) {
	info, ok := RunOf(ctx)
	if !ok || info.Kind != string(kindCron) {
		return time.Time{}, false
	}
	return info.Slot, true
}
