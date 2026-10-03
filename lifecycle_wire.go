package besdk

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
	"github.com/brickKit/be-sdk-go/internal/events"
	"github.com/brickKit/be-sdk-go/internal/jobs"
	"github.com/brickKit/be-sdk-go/internal/lifecycle"
	"github.com/brickKit/be-sdk-go/internal/pg"
)

// LifecycleGuard is a component's named seal check (`guard: {blocked_by}` in lifecycle.yaml): an
// error blocks the unit with its text (P16).
type LifecycleGuard func(ctx context.Context, tx *Tx, table, unit string) error

// LifecycleHooks are the component's parts of the lifecycle engine (P16). Calendars and
// checkpointers belong to the cold tier, which this SDK version does not have.
type LifecycleHooks struct {
	Guards map[string]LifecycleGuard
}

// loadLifecycle reads and checks migrations/lifecycle.yaml and DATA_LIFECYCLE for a component with a
// database (P16.1, P16.9, P11.11); any problem is a configuration error (exit 78).
func loadLifecycle(b *boot) error {
	if !declared(b.vals, "PG_SCHEMA") {
		return nil
	}
	if b.spec.Migrations == nil {
		return &config.Error{Reason: config.ReasonMissing, Key: "Spec.Migrations",
			Detail: "a component with a database ships its migrations with lifecycle.yaml (P16.1)"}
	}
	d, err := lifecycle.Load(b.spec.Migrations)
	if err != nil {
		return &config.Error{Reason: config.ReasonInvalid, Key: "migrations/" + lifecycle.DeclarationFile, Detail: err.Error()}
	}
	c, err := lifecycle.ParseConfig(optString(b.vals, "DATA_LIFECYCLE", ""))
	if err != nil {
		return &config.Error{Reason: config.ReasonInvalid, Key: "DATA_LIFECYCLE", Detail: err.Error()}
	}
	b.lifecycle, b.lifecycleCfg = d, c
	return nil
}

// wireLifecycle builds the engine once New declared the module's guards.
func (p *process) wireLifecycle() error {
	if p.b.lifecycle == nil {
		return nil
	}
	guards := map[string]lifecycle.Guard{}
	for name, g := range p.mod.Lifecycle.Guards {
		g := g
		guards[name] = func(ctx context.Context, tx lifecycle.Tx, table, unit string) error {
			t, ok := tx.(*pg.Tx)
			if !ok {
				return fmt.Errorf("lifecycle guard %s: unexpected transaction %T", name, tx)
			}
			return g(ctx, &Tx{Tx: t, rt: p.rt}, table, unit)
		}
	}
	e, err := lifecycle.New(p.b.lifecycle, lifecycle.Options{ComponentID: p.b.id, Config: p.b.lifecycleCfg,
		Publish: p.publishLifecycle, Guards: guards, Logger: p.b.log})
	if err != nil {
		return &config.Error{Reason: config.ReasonInvalid, Key: "migrations/" + lifecycle.DeclarationFile, Detail: err.Error()}
	}
	p.rt.deps.lifecycle = e
	return nil
}

// publishLifecycle writes a lifecycle event through the outbox in the step's transaction (P16.7) when
// the component publishes events and its contract declares the subject; otherwise the action is only
// in besdk_lifecycle_log (G11), and the gap is logged once per subject.
func (p *process) publishLifecycle(ctx context.Context, tx lifecycle.Tx, ev lifecycle.Event) error {
	prod := p.rt.deps.producer
	t, ok := tx.(*pg.Tx)
	if prod == nil || !ok {
		p.b.log.DebugContext(ctx, "lifecycle event not published: the component publishes no events",
			slog.String("subject", ev.Subject))
		return nil
	}
	if _, declared := prod.Contract.Lookup(ev.Subject); !declared {
		p.b.log.DebugContext(ctx, "lifecycle event not published: not in the events contract", slog.String("subject", ev.Subject))
		return nil
	}
	_, err := events.Write(ctx, t, prod, events.Outgoing{Subject: ev.Subject, AggregateID: ev.Table + "/" + ev.Unit,
		Version: ev.Version, Payload: ev.Payload, Origin: originOf(ctx)})
	return err
}

// lifecycleJob is the runtime's be.lifecycle singleton (P16.2): one Step a minute.
func lifecycleJob(rt *Runtime) jobs.Job {
	return jobs.Job{Name: "be.lifecycle", Kind: jobs.Singleton, Interval: time.Minute, Timeout: 5 * time.Minute,
		Run: func(ctx context.Context) error {
			r, err := rt.deps.lifecycle.Step(ctx, lifecycleRun(rt.deps.store), rt.Now())
			if len(r.Created)+len(r.Sealed)+len(r.Dropped)+len(r.Blocked) > 0 {
				rt.log.InfoContext(ctx, "lifecycle step", slog.Any("created", r.Created), slog.Any("sealed", r.Sealed),
					slog.Any("dropped", r.Dropped), slog.Any("blocked", r.Blocked))
			}
			return err
		}}
}

func lifecycleRun(store *pg.Store) lifecycle.RunFunc {
	return func(ctx context.Context, lt time.Duration, fn func(ctx context.Context, tx lifecycle.Tx) error) error {
		return store.Run(ctx, pg.TxOptions{LockTimeout: lt}, func(ctx context.Context, tx *pg.Tx) error { return fn(ctx, tx) })
	}
}

// Seal seals one unit of a table whose seal is on_signal (P16 "Seal"): from now on its rows refuse
// UPDATE, DELETE and TRUNCATE (UNIT_SEALED).
func (tx *Tx) Seal(ctx context.Context, table, unit string) error {
	e := tx.rt.deps.lifecycle
	if e == nil {
		return fmt.Errorf("besdk: Seal: the component has no lifecycle declaration")
	}
	return e.Seal(ctx, tx.Tx, table, unit, tx.rt.Now())
}

// OpenListPartition creates, if missing, the LIST partition of table for value (a table declared
// `partition: {kind: list, opened_by: command}`, e.g. an accounting period) and returns its name.
func (tx *Tx) OpenListPartition(ctx context.Context, table, value string) (string, error) {
	e := tx.rt.deps.lifecycle
	if e == nil {
		return "", fmt.Errorf("besdk: OpenListPartition: the component has no lifecycle declaration")
	}
	return e.EnsureListPartition(ctx, tx.Tx, table, value, tx.rt.Now())
}
