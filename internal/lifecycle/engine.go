package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Tx is a runtime transaction as the engine needs it; *pg.Tx satisfies it.
type Tx interface {
	Querier
	Lock(ctx context.Context, name string, parts ...string) error
	TryLock(ctx context.Context, name string, parts ...string) (bool, error)
}

// RunFunc runs fn in one runtime transaction with the given lock_timeout; the root adapts its
// pg.Store.Run to it (P10.2, P16.2).
type RunFunc func(ctx context.Context, lockTimeout time.Duration, fn func(ctx context.Context, tx Tx) error) error

// Event is a lifecycle event the root writes through the outbox in the same transaction (P16.7):
// Subject <domain>.<name>.lifecycle.<Action>.v1, aggregate <Table>/<Unit> at Version.
type Event struct {
	Subject       string
	Action        string // sealed | destroyed (frozen, thawed, erasure_completed: later stages)
	AggregateType string // <domain>.<name>.lifecycle_unit
	Table, Unit   string
	Version       int64
	Payload       map[string]any
}

// Publisher writes an event through the outbox inside tx (the root's tx.Publish).
type Publisher func(ctx context.Context, tx Tx, ev Event) error

// Guard is a component's named check (`guard: {blocked_by}`), called before a unit is sealed; an
// error blocks the unit with the error's text (P16 "Seal").
type Guard func(ctx context.Context, tx Tx, table, unit string) error

// DefaultStepLockTimeout keeps the engine's DDL from queueing on the hot path (G2).
const DefaultStepLockTimeout = time.Second

// Options configure an Engine.
type Options struct {
	ComponentID     string    // "erp/sales" (a shell member's own ID): event subjects and log actor
	Config          Config    // the parsed DATA_LIFECYCLE
	Publish         Publisher // required
	Guards          map[string]Guard
	Logger          *slog.Logger
	StepLockTimeout time.Duration // 0 = DefaultStepLockTimeout
}

// Engine is the lifecycle engine of P16 for hot and warm tiers (cold store and cold query `none`): it
// keeps partition windows ahead, seals units, and expires platform and queue partitions. Data of class
// document, ledger and audit never leaves the database here: with no cold store nothing is frozen,
// exported or destroyed (G4). Snapshot tables are left alone.
type Engine struct {
	decl   *Declaration // the effective declaration: lifecycle.yaml with DATA_LIFECYCLE's overrides
	o      Options
	prefix string // <domain>.<name>
	log    *slog.Logger
}

// Actor is how the engine signs besdk_lifecycle_log rows.
const Actor = "be.lifecycle"

// New builds the engine from the loaded declaration and the deployment's configuration, applying the
// per-table overrides (Config.Apply). A guard the declaration names without an implementation fails.
func New(d *Declaration, o Options) (*Engine, error) {
	if o.Publish == nil {
		return nil, errors.New("lifecycle: Options.Publish is required")
	}
	domain, name, ok := strings.Cut(o.ComponentID, "/")
	if !ok || domain == "" || name == "" {
		return nil, fmt.Errorf("lifecycle: component ID %q is not <domain>/<name>", o.ComponentID)
	}
	if o.Config.Mode == "" {
		o.Config.Mode = ModeOn
	}
	eff, err := o.Config.Apply(d)
	if err != nil {
		return nil, err
	}
	for _, n := range eff.Names() {
		if g := eff.Tables[n].Guard; g != "" && o.Guards[g] == nil {
			return nil, &Error{Table: n, Msg: fmt.Sprintf("guard %s has no implementation (Module.Lifecycle.Guards)", g)}
		}
	}
	if o.StepLockTimeout <= 0 {
		o.StepLockTimeout = DefaultStepLockTimeout
	}
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Engine{decl: eff, o: o, prefix: domain + "." + name, log: log}, nil
}

// Declaration is the effective declaration the engine runs.
func (e *Engine) Declaration() *Declaration { return e.decl }

// Report says what one Step did.
type Report struct {
	Created, Adopted, Skipped []string // partitions (window pass)
	Sealed, Blocked, Dropped  []string // units
	Planned                   []string // dry-run: "seal <unit>", "drop <partition>"
	Busy                      []string // steps another executor held (G2)
}

// Step is one run of the singleton job be.lifecycle (P16.2): keep every window ahead (every mode, G1),
// then, in mode on, seal due units and drop expired platform and queue partitions; in dry-run the
// same decisions are reported in Planned and nothing changes; off stops after the window. Each
// action is its own transaction under a transaction-level step lock and a short lock_timeout. A
// failed action does not stop the others; their errors are joined.
func (e *Engine) Step(ctx context.Context, run RunFunc, now time.Time) (Report, error) {
	var r Report
	errs := []error{e.ensureStep(ctx, run, now, &r)}
	if e.o.Config.Mode != ModeOff {
		errs = append(errs, e.sealStep(ctx, run, now, &r), e.retentionStep(ctx, run, now, &r))
	}
	return r, errors.Join(errs...)
}

// locked runs fn in one transaction holding the step lock ("be.lifecycle", step, parts…); a lock
// held elsewhere marks the step busy and skips it.
func (e *Engine) locked(ctx context.Context, run RunFunc, r *Report, step string, fn func(ctx context.Context, tx Tx) error) error {
	return run(ctx, e.o.StepLockTimeout, func(ctx context.Context, tx Tx) error {
		ok, err := tx.TryLock(ctx, Actor, step)
		if err != nil {
			return err
		}
		if !ok {
			r.Busy = append(r.Busy, step)
			return nil
		}
		return fn(ctx, tx)
	})
}

func (e *Engine) ensureStep(ctx context.Context, run RunFunc, now time.Time, r *Report) error {
	return e.locked(ctx, run, r, "ensure", func(ctx context.Context, tx Tx) error {
		res, err := EnsureWindows(ctx, tx, e.decl, now, Actor)
		if err != nil {
			return err
		}
		for _, s := range res.Skipped {
			e.log.Warn("partition skipped: an existing partition overlaps its range", "partition", s)
		}
		r.Created, r.Adopted, r.Skipped = res.Created, res.Adopted, res.Skipped
		return nil
	})
}

// EnsureListPartition opens a list partition in the caller's transaction (see the package function);
// the root exposes it to commands such as erp/finance's open-fiscal-year.
func (e *Engine) EnsureListPartition(ctx context.Context, tx Tx, table, value string, now time.Time) (string, error) {
	return EnsureListPartition(ctx, tx, e.decl, table, value, now, e.o.ComponentID)
}

// event builds a lifecycle event of this component.
func (e *Engine) event(action, table, unit string, version int64, payload map[string]any) Event {
	return Event{Subject: e.prefix + ".lifecycle." + action + ".v1", Action: action,
		AggregateType: e.prefix + ".lifecycle_unit", Table: table, Unit: unit, Version: version, Payload: payload}
}
