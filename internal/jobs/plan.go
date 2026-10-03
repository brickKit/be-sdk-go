package jobs

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/jobs/schedule"
)

// entryKind is one of the five kinds (P14), as written in RunInfo.Kind and the logs.
type entryKind string

const (
	kindEvery      entryKind = "every"
	kindSingleton  entryKind = "singleton"
	kindCron       entryKind = "cron"
	kindQueue      entryKind = "queue"
	kindReconciler entryKind = "reconciler"
)

// namePattern is the job-name pattern of jobs-overrides.schema.json (propertyNames).
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]*$`)

// entry is one declared piece of background work after validation and overrides.
type entry struct {
	name     string
	kind     entryKind
	interval time.Duration // every, singleton, reconciler
	loc      *time.Location
	sched    schedule.Schedule // cron
	timeout  time.Duration
	enabled  bool                            // false: no in-process loop (job run still runs it, P14.5)
	run      func(ctx context.Context) error // every, singleton, cron
	worker   *Worker                         // queue, defaults applied
	rec      *Reconciler                     // reconciler, defaults applied
}

// plan is a member's validated declarations, in declaration order.
type plan struct {
	entries []*entry
	byName  map[string]*entry
}

// compile validates the declarations and applies JOBS_OVERRIDES (P14.5, P14.6). Every problem is a
// *config.Error, which the root turns into exit 78.
func compile(cfg Config, d Declarations) (*plan, error) {
	zone := cfg.Zone
	if zone == nil {
		z, err := time.LoadLocation(DefaultZone)
		if err != nil {
			return nil, &config.Error{Reason: config.ReasonInvalid, Key: "BUSINESS_TIMEZONE", Detail: err.Error()}
		}
		zone = z
	}
	p := &plan{byName: map[string]*entry{}}
	for _, j := range d.Jobs {
		if err := p.add(jobEntry(j, zone, false)); err != nil {
			return nil, err
		}
	}
	for _, j := range d.Runtime {
		if err := p.add(jobEntry(j, zone, true)); err != nil {
			return nil, err
		}
	}
	for _, w := range d.Workers {
		if err := p.add(workerEntry(w)); err != nil {
			return nil, err
		}
	}
	for _, r := range d.Reconcilers {
		if r == nil {
			continue
		}
		if err := p.add(reconcilerEntry(r)); err != nil {
			return nil, err
		}
	}
	if err := p.applyOverrides(cfg.Overrides, cfg.logger()); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *plan) add(e *entry, err error) error {
	if err != nil {
		return err
	}
	if _, dup := p.byName[e.name]; dup {
		return declErr(e.name, "the name is declared twice (jobs, workers and reconcilers share one namespace)")
	}
	p.entries = append(p.entries, e)
	p.byName[e.name] = e
	return nil
}

// declErr is a declaration error naming the job (exit 78). Detail never repeats a configured value.
func declErr(name, format string, args ...any) *config.Error {
	return &config.Error{Reason: config.ReasonInvalid, Key: "job " + name, Detail: fmt.Sprintf(format, args...)}
}

// checkName: the override-schema pattern; "be." only and always for runtime-owned jobs (P14.1).
func checkName(name string, runtime bool) error {
	switch {
	case !namePattern.MatchString(name):
		return declErr(name, "a job name matches ^[a-z][a-z0-9_.-]*$")
	case runtime && !strings.HasPrefix(name, "be."):
		return declErr(name, "a runtime-owned job is named be.<name>")
	case !runtime && strings.HasPrefix(name, "be."):
		return declErr(name, "the prefix be. is reserved for runtime-owned jobs")
	}
	return nil
}

func jobEntry(j Job, zone *time.Location, runtime bool) (*entry, error) {
	if err := checkName(j.Name, runtime); err != nil {
		return nil, err
	}
	if j.Timeout <= 0 {
		return nil, declErr(j.Name, "a timeout is required (P14.2)")
	}
	if j.Run == nil {
		return nil, declErr(j.Name, "Run is required")
	}
	e := &entry{name: j.Name, timeout: j.Timeout, enabled: true, run: j.Run, loc: zone}
	switch j.Kind {
	case Every, Singleton:
		e.kind = kindEvery
		if j.Kind == Singleton {
			e.kind = kindSingleton
		}
		if j.Interval <= 0 {
			return nil, declErr(j.Name, "an %s job needs an interval", e.kind)
		}
		e.interval = j.Interval
	case Cron:
		e.kind = kindCron
		if j.TZ != "" {
			loc, err := time.LoadLocation(j.TZ)
			if err != nil || j.TZ == "Local" {
				return nil, declErr(j.Name, "TZ is not an IANA time-zone name")
			}
			e.loc = loc
		}
		s, err := schedule.Parse(j.Cron, e.loc)
		if err != nil {
			return nil, declErr(j.Name, "invalid schedule (P14.6): %v", err)
		}
		e.sched = s
	default:
		return nil, declErr(j.Name, "unknown kind %d", j.Kind)
	}
	return e, nil
}

func workerEntry(w Worker) (*entry, error) {
	if err := checkName(w.Kind, false); err != nil {
		return nil, err
	}
	if w.Timeout <= 0 {
		return nil, declErr(w.Kind, "a worker timeout is required (P14.2)")
	}
	if w.Run == nil {
		return nil, declErr(w.Kind, "Run is required")
	}
	if w.Concurrency <= 0 {
		w.Concurrency = 1
	}
	if w.MaxAttempts <= 0 {
		w.MaxAttempts = DefaultMaxAttempts
	}
	return &entry{name: w.Kind, kind: kindQueue, timeout: w.Timeout, enabled: true, worker: &w}, nil
}

func reconcilerEntry(r *Reconciler) (*entry, error) {
	if err := checkName(r.name, false); err != nil {
		return nil, err
	}
	if r.timeout <= 0 {
		return nil, declErr(r.name, "a reconciler timeout is required (P14.2)")
	}
	if r.every <= 0 {
		return nil, declErr(r.name, "a reconciler needs Every")
	}
	if err := r.check(); err != nil {
		return nil, declErr(r.name, "%v", err)
	}
	return &entry{name: r.name, kind: kindReconciler, interval: r.every, timeout: r.timeout, enabled: true, rec: r}, nil
}
