package besdk

import (
	"context"
	"fmt"
)

// wireDeps connects the runtime's dependencies (database, bus, outbound clients) — filled in by the
// store and events waves of this lane.
func (p *process) wireDeps(ctx context.Context) error {
	out, err := newOutbound(p.rt, func(context.Context) bool { return false })
	if err != nil {
		return err
	}
	p.rt.deps.out = out
	p.closers = append(p.closers, func(context.Context) { _ = out.conns.Close() })
	return nil
}

// superviseDeps starts the dependency loops (probe, pump, consumers).
func (p *process) superviseDeps() {}

func (p *process) migrationsInfo() MigrationsInfo { return MigrationsInfo{} }

func (p *process) eventProfiles() []string { return nil }

func runMigrate(ctx context.Context, b *boot, cmd command) int {
	b.log.Error(fmt.Sprintf("migrate: not wired yet (%d)", cmd.kind))
	return exitFailure
}
