package besdk

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// jobRunSchemaWait bounds how long `job run` waits for the database before it gives up.
const jobRunSchemaWait = 30 * time.Second

// runJob is `<entrypoint> job run <name>` (P14.8): the same configuration and secret files as the
// service, the schema version checked as the serving entry point does (P1.8), the component's New
// called for its declarations, no server and no other background work; one run of the job through
// the same lease and slot tables, then exit 0 (ran or no-op), 1 (failed), 64 (unknown name), 78
// (configuration).
func runJob(ctx context.Context, b *boot, name string) int {
	p := &process{b: b, fatal: make(chan error, 1)}
	defer p.closeDeps()
	if code := p.openRuntime(); code != exitOK {
		return code
	}
	if err := p.wireDeps(ctx); err != nil {
		return p.failCode("job run", err)
	}
	if err := p.checkSchema(ctx); err != nil {
		b.log.Error("job run: the schema is not ready", slog.String("job", name), slog.String("error", err.Error()))
		return exitFailure
	}
	var err error
	if p.mod, err = callNew(ctx, b.spec, p.rt); err != nil {
		return p.failCode("component New failed", err)
	}
	if err := p.wireProducer(); err != nil {
		return p.failCode("events", err)
	}
	if err := p.wireLifecycle(); err != nil {
		return p.failCode("lifecycle", err)
	}
	if err := p.wireJobs(); err != nil {
		return p.failCode("jobs", err)
	}
	if p.rt.deps.jobs == nil {
		b.log.Error("job run: the component declares no background work", slog.String("job", name))
		return exitUsage
	}
	out := p.rt.deps.jobs.RunOnce(ctx, name)
	attrs := []any{slog.String("job", name), slog.String("reason", out.Reason)}
	if out.Err != nil {
		attrs = append(attrs, slog.String("error", out.Err.Error()))
	}
	switch code := out.ExitCode(); code {
	case exitOK:
		b.log.Info("job run done", attrs...)
		return code
	default:
		b.log.Error("job run failed", attrs...)
		return code
	}
}

// checkSchema is the start-up probe once, retried until jobRunSchemaWait: the identity must check out
// and the schema must be at this image's migrations (P1.8, P10.7).
func (p *process) checkSchema(ctx context.Context) error {
	if p.rt.deps.store == nil {
		return nil
	}
	latest, err := pg.LatestVersion(p.b.spec.Migrations)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(jobRunSchemaWait)
	for {
		r := pg.Probe(ctx, p.rt.deps.store, optString(p.b.vals, "PG_OWNER_USER", ""), false)
		switch {
		case r.Fatal != nil:
			return r.Fatal
		case r.Err == nil && len(r.IdentityProblems) > 0:
			return fmt.Errorf("database identity: %v", r.IdentityProblems)
		case r.Err == nil && (r.ComponentVersion != latest || r.ComponentDirty || r.PlatformVersion != pg.PlatformVersion):
			return fmt.Errorf("schema at migration %d (platform %d), image %d (platform %d): run migrate up first",
				r.ComponentVersion, r.PlatformVersion, latest, pg.PlatformVersion)
		case r.Err == nil:
			return nil
		case time.Now().After(deadline):
			return r.Err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// closeDeps runs the closers in reverse order.
func (p *process) closeDeps() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := len(p.closers) - 1; i >= 0; i-- {
		p.closers[i](ctx)
	}
}
