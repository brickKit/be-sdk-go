package besdk

import (
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/gin-gonic/gin"
)

// ResourceType is a resource type name of the component's authorization catalogue, e.g.
// "erp.sales.order" (be-ops authzgen emits typed constants of it).
type ResourceType string

// ResourceGuard is a permission key decided on a resource type (authz-architecture §4.2): List
// filters a list with the key's scope (Access.ListScope), On decides one record named by a path
// parameter before the handler, through the type's Module.Sharing loader (P6.6).
type ResourceGuard struct {
	Key   PermKey
	Type  ResourceType
	Param string // the path parameter holding the record ID; "" for List
}

func (ResourceGuard) guard() {}

// List guards a list route of t with key k: `besdk.GET(r, "/orders", authzgen.SalesView.List(authzgen.SalesOrder), h)`.
func (k PermKey) List(t ResourceType) ResourceGuard { return ResourceGuard{Key: k, Type: t} }

// On guards a single-record route of t with key k, the record ID in path parameter param:
// `besdk.GET(r, "/orders/:id", authzgen.SalesView.On(authzgen.SalesOrder, "id"), h)`.
func (k PermKey) On(t ResourceType, param string) ResourceGuard {
	return ResourceGuard{Key: k, Type: t, Param: param}
}

// guardKey is the permission key a guard is decided with.
func guardKey(g Guard) PermKey {
	switch v := g.(type) {
	case PermKey:
		return v
	case ResourceGuard:
		return v.Key
	}
	return Authenticated
}

// decideRecord is PermKey.On's single-record decision (P6.6, A3): the record named by the path
// parameter is loaded through the type's SharingLoader with its ACL rows and decided with the route's
// key; an invisible or missing record is 404, a visible one outside the key's scope 403. Without a
// loader for the type the handler decides (Access.Can).
func (g *authGuardian) decideRecord(c *gin.Context, a *Access, rg ResourceGuard) *problem.Error {
	if rg.Param == "" || g.rt == nil {
		return nil
	}
	l, ok := g.rt.deps.loaders[rg.Type]
	if !ok {
		return nil
	}
	rc := &resourceContract{rt: g.rt, loaders: map[ResourceType]SharingLoader{rg.Type: l}}
	id := c.Param(rg.Param)
	recs, acls, err := rc.records(c.Request.Context(), rg.Type, []string{id})
	if err != nil {
		return problem.From(err)
	}
	r, found := recs[id]
	rt, known := a.resourceType(rg.Type)
	if !found || !known {
		return problem.Be("NOT_FOUND", nil)
	}
	d := a.evaluator().Decide(rt, string(rg.Key), rowOf(r), acls[id], nil)
	if d.Allowed {
		return nil
	}
	return problem.From(Decision{Visible: d.Visible, Allowed: d.Allowed, Reason: d.Reason}.Err())
}

// ListScope is Access.Scope for the route's own resource type (PermKey.List).
func (a *Access) ListScope() Scope { return a.Scope(a.typ) }
