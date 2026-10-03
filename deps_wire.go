package besdk

import (
	"context"

	"github.com/brickKit/be-sdk-go/internal/pg"
)

// wireDeps builds the runtime's dependencies before New: the store (database), the outbound clients
// and (with the events wave) the bus. Nothing connects here; connections happen in the background.
func (p *process) wireDeps(ctx context.Context) error {
	if err := p.wireStore(); err != nil {
		return err
	}
	out, err := newOutbound(p.rt, pg.InTx)
	if err != nil {
		return err
	}
	p.rt.deps.out = out
	p.closers = append(p.closers, func(context.Context) { _ = out.conns.Close() })
	return nil
}

// superviseDeps starts the dependency loops (probe, outbox window; pump and consumers with events).
func (p *process) superviseDeps() {
	p.superviseStore()
}

func (p *process) eventProfiles() []string { return nil }
