package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// RunResult is the outcome class of a one-shot run (P14.8).
type RunResult int

// One-shot outcomes and their exit codes (P14.8, P1.1).
const (
	RunOK      RunResult = iota // the run succeeded: exit 0
	RunNoop                     // nothing to do (slot claimed, lease held elsewhere, queue empty): exit 0
	RunFailed                   // the run failed: exit 1
	RunUnknown                  // no job of that name: exit 64
)

// RunOutcome is what RunOnce did.
type RunOutcome struct {
	Result RunResult
	Reason string // why a no-op or a failure, for the log line
	Err    error  // the run's error when RunFailed
}

// ExitCode maps the outcome to the process exit code of `job run` (P14.8). A configuration error
// (exit 78) is reported earlier, by New.
func (o RunOutcome) ExitCode() int {
	switch o.Result {
	case RunOK, RunNoop:
		return 0
	case RunUnknown:
		return 64
	}
	return 1
}

// RunOnce runs one run of the declared job name through the same tables as the in-process scheduler
// (P14.8), with the holder "<component ID>/job-run:<instance id>", whatever JOBS_OVERRIDES says about
// enabled:
//   - cron: claims the most recent slot at or before now; a slot already claimed is a no-op;
//   - singleton: takes the lease for the run; a lease held elsewhere is a no-op;
//   - every: one run; reconciler: one pass, claiming items as usual;
//   - queue: drains the ready rows of that kind once, within the worker's timeout.
func (e *Engine) RunOnce(ctx context.Context, name string) RunOutcome {
	en, ok := e.plan.byName[name]
	if !ok {
		return RunOutcome{Result: RunUnknown, Reason: fmt.Sprintf("no job named %q", name)}
	}
	var out RunOutcome
	switch en.kind {
	case kindEvery:
		out = e.outcomeOf(e.exec(ctx, e.info(en, true), en.timeout, en.run))
	case kindCron:
		out = e.runOnceCron(ctx, en)
	case kindSingleton:
		out = e.runOnceSingleton(ctx, en)
	case kindQueue:
		out = e.drain(ctx, en)
	case kindReconciler:
		out = e.runOncePass(ctx, en)
	}
	if out.Result == RunNoop {
		e.log.Info("job run: nothing to do", slog.String("job", name), slog.String("reason", out.Reason))
	}
	return out
}

func (e *Engine) outcomeOf(res string, err error) RunOutcome {
	if res == ResultOK {
		return RunOutcome{Result: RunOK}
	}
	return RunOutcome{Result: RunFailed, Reason: res, Err: err}
}

func (e *Engine) runOnceCron(ctx context.Context, en *entry) RunOutcome {
	slot := en.sched.Prev(e.cfg.now())
	claimed, err := e.runSlot(ctx, en, slot, e.cfg.oneShotHolder(), true)
	switch {
	case err != nil:
		return RunOutcome{Result: RunFailed, Reason: "slot " + slot.UTC().Format(time.RFC3339), Err: err}
	case !claimed:
		return RunOutcome{Result: RunNoop, Reason: "slot " + slot.UTC().Format(time.RFC3339) + " already claimed"}
	}
	return RunOutcome{Result: RunOK}
}

func (e *Engine) runOnceSingleton(ctx context.Context, en *entry) RunOutcome {
	l, ok, err := e.acquire(ctx, en.name, e.cfg.oneShotHolder())
	switch {
	case err != nil:
		return RunOutcome{Result: RunFailed, Reason: "take lease", Err: err}
	case !ok:
		return RunOutcome{Result: RunNoop, Reason: "lease held by another holder"}
	}
	defer l.release()
	info := e.info(en, true)
	info.Epoch = l.epoch
	return e.outcomeOf(e.exec(l.ctx, info, en.timeout, en.run))
}

func (e *Engine) runOncePass(ctx context.Context, en *entry) RunOutcome {
	pr, err := e.reconcilePass(ctx, en, true)
	switch {
	case err != nil:
		return RunOutcome{Result: RunFailed, Reason: "pass", Err: err}
	case pr.failed > 0:
		return RunOutcome{Result: RunFailed, Reason: fmt.Sprintf("%d of %d items failed", pr.failed, pr.handled)}
	case pr.handled == 0 && pr.gaveUp == 0:
		return RunOutcome{Result: RunNoop, Reason: "no candidate claimed"}
	}
	return RunOutcome{Result: RunOK}
}

// drain claims and runs the due ready rows of en's kind, one batch of Concurrency at a time, until
// none is left or the worker's timeout passes; rows that fail go back to ready with their backoff
// and are not taken again in this drain.
func (e *Engine) drain(ctx context.Context, en *entry) RunOutcome {
	w := en.worker
	dctx, cancel := context.WithTimeout(ctx, w.Timeout)
	defer cancel()
	ran, failed := 0, 0
	var firstErr error
	for dctx.Err() == nil {
		jobs, err := e.claimQueue(dctx, w, w.Concurrency, false)
		if err != nil {
			if dctx.Err() != nil {
				break
			}
			return RunOutcome{Result: RunFailed, Reason: "claim", Err: err}
		}
		if len(jobs) == 0 {
			break
		}
		for _, j := range jobs {
			ran++
			if res, err := e.process(dctx, en, j, true); res != ResultOK {
				failed++
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}
	switch {
	case failed > 0:
		return RunOutcome{Result: RunFailed, Reason: fmt.Sprintf("%d of %d jobs failed", failed, ran), Err: firstErr}
	case ran == 0:
		return RunOutcome{Result: RunNoop, Reason: "no ready job of this kind"}
	}
	return RunOutcome{Result: RunOK}
}
