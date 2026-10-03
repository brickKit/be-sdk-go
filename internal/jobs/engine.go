package jobs

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
)

// Engine runs one member's background work (P14).
type Engine struct {
	cfg  Config
	plan *plan
	m    *metrics
	log  *slog.Logger
}

// Loop is one named piece of background work for the root's supervisor (P1.7): it runs until ctx
// ends and returns nil, or returns an error, after which the supervisor restarts it with backoff.
type Loop struct {
	Name string
	Run  func(ctx context.Context) error
}

// JobInfo describes one declared job (P14.4 operations endpoint, /_be/info).
type JobInfo struct {
	Name     string
	Kind     string        // every, singleton, cron, queue, reconciler
	Schedule string        // cron: the expression in force; every, singleton, reconciler: the interval
	Timeout  time.Duration // one run
	Enabled  bool          // false: no in-process scheduling (JOBS_OVERRIDES), job run still works
}

// New validates the declarations, applies JOBS_OVERRIDES and registers the P14.3 metrics. A
// declaration or override problem is a *config.Error (exit 78, P14.6); jobs without a store are one
// too.
func New(cfg Config, d Declarations) (*Engine, error) {
	p, err := compile(cfg, d)
	if err != nil {
		return nil, err
	}
	if len(p.entries) > 0 && cfg.Store == nil {
		return nil, &config.Error{Reason: config.ReasonMissing, Key: "PG_SCHEMA",
			Detail: "background jobs keep their state in the component's schema: the db profile is required (P14)"}
	}
	if cfg.ComponentID == "" || cfg.InstanceID == "" {
		return nil, errors.New("jobs: ComponentID and InstanceID are required (holder, P14)")
	}
	m, err := newMetrics(cfg.Registerer)
	if err != nil {
		return nil, err
	}
	return &Engine{cfg: cfg, plan: p, m: m, log: cfg.logger()}, nil
}

// Loops returns the in-process loops of every enabled job, worker and reconciler, in declaration
// order (P14.2, P14.5). The engine starts none of them itself.
func (e *Engine) Loops() []Loop {
	var out []Loop
	for _, en := range e.plan.entries {
		if !en.enabled {
			continue
		}
		var run func(ctx context.Context) error
		switch en.kind {
		case kindEvery:
			run = e.everyLoop(en)
		case kindSingleton:
			run = e.singletonLoop(en)
		case kindCron:
			run = e.cronLoop(en)
		case kindQueue:
			run = e.workerLoop(en)
		case kindReconciler:
			run = e.reconcilerLoop(en)
		}
		out = append(out, Loop{Name: en.name, Run: run})
	}
	return out
}

// Jobs describes every declared job, worker and reconciler, in declaration order.
func (e *Engine) Jobs() []JobInfo {
	out := make([]JobInfo, 0, len(e.plan.entries))
	for _, en := range e.plan.entries {
		ji := JobInfo{Name: en.name, Kind: string(en.kind), Timeout: en.timeout, Enabled: en.enabled}
		switch {
		case en.sched != nil:
			ji.Schedule = en.sched.String()
		case en.interval > 0:
			ji.Schedule = en.interval.String()
		}
		out = append(out, ji)
	}
	return out
}

// info is the RunInfo of a run of en.
func (e *Engine) info(en *entry, oneShot bool) RunInfo {
	holder := e.cfg.holder()
	if oneShot {
		holder = e.cfg.oneShotHolder()
	}
	return RunInfo{Name: en.name, Kind: string(en.kind), OneShot: oneShot, Holder: holder}
}
