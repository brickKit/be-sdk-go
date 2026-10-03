package besdk

import (
	"context"

	"github.com/brickKit/be-sdk-go/internal/authz/acl"
)

// pokeSubject is the authorization provider's best-effort poke (P6.1, P6.12, P12.10).
const pokeSubject = "infra.authz.changed.v1"

// wireProjection prepares the ACL projection of a component that declares resource types (P6.12) and
// the provider's gRPC target for sharing (P6.10).
func (p *process) wireProjection() error {
	if declared(p.b.vals, "AUTHZ_GRPC_URL") {
		p.rt.deps.authzGRPC, _ = p.b.vals.Family("AUTHZ_GRPC_URL")
	}
	cat := p.rt.authzCatalog
	if cat == nil || len(cat.Types()) == 0 || p.auth == nil || p.rt.deps.store == nil {
		return nil
	}
	url, _ := p.b.vals.Family("AUTHZ_URL")
	poller := p.auth.poller
	proj, err := acl.New(acl.Config{URL: url, Caller: p.b.id, Types: cat.PulledTypes(), Store: p.rt.deps.store,
		Logger: p.b.log, Active: func() bool {
			b := poller.Current()
			return b != nil && (b.Capabilities.Sharing || b.Capabilities.RelationSync)
		}})
	if err != nil {
		return err
	}
	p.rt.deps.projection = proj
	return nil
}

// superviseAuthz runs the projection's pulls (be.authz.changes) and, with a bus, the poke
// subscription that triggers an immediate bundle fetch and projection pull (P6.1, P6.12).
func (p *process) superviseAuthz() {
	if proj := p.rt.deps.projection; proj != nil {
		p.sup.Go("be.authz.changes", proj.Run)
	}
	if p.auth == nil || p.bus == nil {
		return
	}
	p.sup.Go("be.authz.poke", func(ctx context.Context) error {
		unsubscribe, err := p.bus.OnSignal(pokeSubject, func([]byte) {
			p.auth.poller.Poke()
			if proj := p.rt.deps.projection; proj != nil {
				proj.Poke()
			}
		})
		if err != nil {
			return err
		}
		<-ctx.Done()
		unsubscribe()
		return nil
	})
}
