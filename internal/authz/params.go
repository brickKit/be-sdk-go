package authz

// ScopeParams are the parameters of the canonical list predicate for one key and resource type (P6.5,
// E6–E9). Each field is one @s_* parameter; Dims holds @s_<dim>_all / @s_<dim>_ids keyed by the
// dimension name. Every slice is sorted, deduplicated and never nil. The JSON form is the vectors'
// expected.scope_params.
type ScopeParams struct {
	All        bool                `json:"s_all"`         // @s_all
	Owners     []string            `json:"s_owners"`      // @s_owners
	DeptExact  []string            `json:"s_dept_exact"`  // @s_dept_exact
	DeptPrefix []string            `json:"s_dept_prefix"` // @s_dept_prefix: LIKE patterns, escaped, ending in %
	Dims       map[string]DimParam `json:"s_dims"`        // @s_<dim>_all, @s_<dim>_ids
	ACL        bool                `json:"s_acl"`         // @s_acl
	Relations  []string            `json:"s_relations"`   // @s_relations
	Subjects   []string            `json:"s_subjects"`    // @s_subjects
	GraphIDs   []string            `json:"s_graph_ids"`   // @s_graph_ids
}

// DimParam is one resource dimension's parameters (E7): All is @s_<dim>_all, IDs is @s_<dim>_ids.
type DimParam struct {
	All bool     `json:"all"`
	IDs []string `json:"ids"`
}

// Branches splits the parameters into the canonical predicate's three OR branches (P6.5): the rule
// (identity and dimensions), the ACL and the graph branch. Each keeps only its own branch switched on;
// a row may match several, so a caller that runs them separately either uses UNION or the disjoint
// fragments of Predicate.Branches.
func (p ScopeParams) Branches() []ScopeParams {
	rule, acl, graph := p.clone(), p.clone(), p.clone()
	rule.ACL, rule.GraphIDs = false, []string{}
	acl.All, acl.Owners, acl.DeptExact, acl.DeptPrefix, acl.GraphIDs = false, []string{}, []string{}, []string{}, []string{}
	graph.All, graph.Owners, graph.DeptExact, graph.DeptPrefix, graph.ACL = false, []string{}, []string{}, []string{}, false
	return []ScopeParams{rule, acl, graph}
}

// clone copies the slices and the map so a branch never aliases the original.
func (p ScopeParams) clone() ScopeParams {
	c := p
	c.Owners = append([]string{}, p.Owners...)
	c.DeptExact = append([]string{}, p.DeptExact...)
	c.DeptPrefix = append([]string{}, p.DeptPrefix...)
	c.Relations = append([]string{}, p.Relations...)
	c.Subjects = append([]string{}, p.Subjects...)
	c.GraphIDs = append([]string{}, p.GraphIDs...)
	c.Dims = make(map[string]DimParam, len(p.Dims))
	for k, v := range p.Dims {
		c.Dims[k] = DimParam{All: v.All, IDs: append([]string{}, v.IDs...)}
	}
	return c
}
