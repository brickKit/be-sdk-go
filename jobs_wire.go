package besdk

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/events"
	"github.com/brickKit/be-sdk-go/internal/idem"
	"github.com/brickKit/be-sdk-go/internal/jobs"
	"github.com/brickKit/be-sdk-go/internal/logx"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"go.opentelemetry.io/otel/trace"
)

// cleanupBatch is how many rows one be.cleanup transaction deletes.
const cleanupBatch = 1000

// newJobEngine builds the member's job engine (P14) from the module's declarations and the runtime's
// own be.* jobs. nil without a database: then the module may declare no background work.
func newJobEngine(b *boot, rt *Runtime, mod *Module, runtimeJobs []jobs.Job) (*jobs.Engine, error) {
	d := jobs.Declarations{Runtime: runtimeJobs}
	for _, j := range mod.Jobs {
		d.Jobs = append(d.Jobs, jobs.Job{Name: j.Name, Kind: jobs.Kind(j.Kind), Interval: j.Interval, Cron: j.Cron,
			TZ: j.TZ, Timeout: j.Timeout, Run: j.Run})
	}
	for _, w := range mod.Workers {
		d.Workers = append(d.Workers, workerOf(rt, w))
	}
	for _, r := range mod.Reconcilers {
		if r.bind != nil {
			d.Reconcilers = append(d.Reconcilers, r.bind(rt))
		}
	}
	if rt.deps.store == nil && len(d.Jobs)+len(d.Workers)+len(d.Reconcilers) == 0 {
		return nil, nil
	}
	zone, err := time.LoadLocation(optString(b.vals, "BUSINESS_TIMEZONE", jobs.DefaultZone))
	if err != nil {
		return nil, err
	}
	return jobs.New(jobs.Config{Store: rt.deps.store, ComponentID: b.id, InstanceID: b.instance, Zone: zone,
		Overrides: optString(b.vals, "JOBS_OVERRIDES", ""), Logger: b.log, Registerer: b.member.Registerer(),
		Now: rt.clock, Wrap: jobWrap(b)}, d)
}

func workerOf(rt *Runtime, w Worker) jobs.Worker {
	jw := jobs.Worker{Kind: w.Kind, Concurrency: w.Concurrency, MaxAttempts: w.MaxAttempts, Backoff: w.Backoff,
		Timeout: w.Timeout, Run: w.Run}
	if w.OnDead != nil {
		onDead := w.OnDead
		jw.OnDead = func(ctx context.Context, tx *pg.Tx, j jobs.QueuedJob) error {
			return onDead(ctx, &Tx{Tx: tx, rt: rt}, j)
		}
	}
	return jw
}

// jobWrap runs around every job run: its own trace root (a queued job linked to the enqueuing span),
// the job's log fields, and the publishing origin of events it writes (P12.8, P18.1).
func jobWrap(b *boot) func(ctx context.Context, info jobs.RunInfo, run func(ctx context.Context) error) error {
	return func(ctx context.Context, info jobs.RunInfo, run func(ctx context.Context) error) error {
		ctx, span := b.member.Tracer().Start(ctx, "job "+info.Name,
			trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindInternal))
		defer span.End()
		ctx = logx.WithFields(ctx, slog.String("job", info.Name), slog.String("job_kind", info.Kind))
		return run(context.WithValue(ctx, handlingKey{}, jobOrigin(info)))
	}
}

func jobOrigin(info jobs.RunInfo) envelope.Origin {
	switch info.Kind {
	case "queue":
		if q := info.Queued; q != nil {
			return envelope.Origin{Kind: envelope.OriginQueuedJob, CausationID: q.CausationID, HopCount: q.HopCount}
		}
	case "cron":
		return envelope.Origin{Kind: envelope.OriginCron}
	case "singleton":
		return envelope.Origin{Kind: envelope.OriginSingleton}
	case "reconciler":
		return envelope.Origin{Kind: envelope.OriginReconciler}
	}
	return envelope.Origin{Kind: envelope.OriginEvery}
}

// cleanupJob is the runtime's be.cleanup (P14.7, P13.7, P12.15): done queue rows after 7 days, cron
// slots after 30 days, expired idempotency keys, cursor rows unseen for 30 days. One transaction per
// batch, so it never holds many rows at once.
func cleanupJob(rt *Runtime) jobs.Job {
	return jobs.Job{Name: "be.cleanup", Kind: jobs.Cron, Cron: "@every 1h", Timeout: 10 * time.Minute,
		Run: func(ctx context.Context) error {
			store := rt.deps.store
			_, err := jobs.Cleanup(ctx, store, jobs.Retention{})
			errs := []error{err,
				deleteInBatches(ctx, store, func(ctx context.Context, tx *pg.Tx) (int64, error) {
					return idem.DeleteExpired(ctx, tx, rt.Now(), cleanupBatch)
				}),
				deleteInBatches(ctx, store, func(ctx context.Context, tx *pg.Tx) (int64, error) {
					return events.DeleteStaleCursors(ctx, tx, rt.Now().Add(-events.CursorRetention), cleanupBatch)
				})}
			return errors.Join(errs...)
		}}
}

func deleteInBatches(ctx context.Context, store *pg.Store, del func(ctx context.Context, tx *pg.Tx) (int64, error)) error {
	for ctx.Err() == nil {
		var n int64
		err := store.Run(ctx, pg.TxOptions{}, func(ctx context.Context, tx *pg.Tx) error {
			var err error
			n, err = del(ctx, tx)
			return err
		})
		if err != nil || n < cleanupBatch {
			return err
		}
	}
	return nil
}

// runtimeJobs are the member's own be.* jobs (P14.1).
func (p *process) runtimeJobs() []jobs.Job {
	if p.rt.deps.store == nil {
		return nil
	}
	out := []jobs.Job{cleanupJob(p.rt)}
	if p.rt.deps.lifecycle != nil {
		out = append(out, lifecycleJob(p.rt))
	}
	return out
}

// wireJobs builds the job engine after New declared the module's jobs.
func (p *process) wireJobs() error {
	e, err := newJobEngine(p.b, p.rt, p.mod, p.runtimeJobs())
	if err != nil {
		return err
	}
	p.rt.deps.jobs = e
	return nil
}

// superviseJobs runs every enabled job loop under the supervisor (P1.7, P14.2).
func (p *process) superviseJobs() {
	if p.rt.deps.jobs == nil {
		return
	}
	for _, l := range p.rt.deps.jobs.Loops() {
		p.sup.Go("job:"+l.Name, l.Run)
	}
}
