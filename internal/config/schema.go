package config

// Schema is a component's declared configuration: its configSchema read with the catalogue (P2.2,
// P2.8), plus the identity the runtime cross-checks against COMPONENT_ID / COMPONENT_VERSION (P1.2).
type Schema struct {
	ID      string // metadata.id
	Version string // metadata.version
	Decls   []Decl // in declaration order
	index   map[string]int
}

// NewSchema builds a Schema from declarations already checked (tests, shells assembling members).
func NewSchema(id, version string, decls []Decl) Schema {
	idx := make(map[string]int, len(decls))
	for i, d := range decls {
		idx[d.Name] = i
	}
	return Schema{ID: id, Version: version, Decls: decls, index: idx}
}

// Lookup returns the declaration of key.
func (s Schema) Lookup(key string) (Decl, bool) {
	i, ok := s.index[key]
	if !ok {
		return Decl{}, false
	}
	return s.Decls[i], true
}
