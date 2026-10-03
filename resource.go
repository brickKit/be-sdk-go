package besdk

// ResourceType is a resource type name of the component's authorization catalogue, e.g.
// "erp.sales.order" (be-ops authzgen emits typed constants of it).
type ResourceType string

// ResourceGuard is a permission key decided on a resource type (authz-architecture §4.2): List
// filters a list with the key's scope, On decides one record named by a path parameter. In this
// version the guard decides the key only (P6.2); the scope and the single-record decision of P6.3–P6.7
// arrive with the data-scope wave without changing the route registrations.
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
