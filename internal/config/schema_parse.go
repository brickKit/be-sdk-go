package config

import (
	"fmt"
	"slices"

	"gopkg.in/yaml.v3"
)

// manifest is the part of a brickKit component.yaml the configuration reads.
type manifest struct {
	Metadata struct {
		ID      string `yaml:"id"`
		Version string `yaml:"version"`
	} `yaml:"metadata"`
	ConfigSchema struct {
		Properties yaml.Node `yaml:"properties"`
		Required   []string  `yaml:"required"`
	} `yaml:"configSchema"`
}

// propItem is one configSchema property (brickKit's item; unknown fields such as description ignored).
type propItem struct {
	Type    string    `yaml:"type"`
	Default yaml.Node `yaml:"default"`
	Secret  bool      `yaml:"secret"`
	Mount   string    `yaml:"mount"`
	Enum    []string  `yaml:"enum"`
	Minimum *int64    `yaml:"minimum"`
}

// ParseSchema reads metadata.id, metadata.version and configSchema (properties, required) of a
// brickKit component.yaml (P2.2, P2.8). A protocol key takes its format, enum, schemes, default_from
// and one_of from the catalogue, and the component's own default wins over the catalogue's; a
// component's own key gets its format from its brickKit type, or from a catalogue pattern
// (*_NO_FORMAT). Declarations that break P2.4 or P2.12, retired names and types that differ from the
// catalogue are rejected. The error, when not nil, is Errors listing every problem.
func ParseSchema(componentYAML []byte, cat *Catalogue) (Schema, error) {
	var m manifest
	if err := yaml.Unmarshal(componentYAML, &m); err != nil {
		return Schema{}, Errors{newErr(ReasonManifestInvalid, "", "component.yaml: "+err.Error())}
	}
	var errs Errors
	if m.Metadata.ID == "" || m.Metadata.Version == "" {
		errs = append(errs, newErr(ReasonManifestInvalid, "", "component.yaml: metadata.id and metadata.version are required"))
	}
	decls, perrs := parseProperties(&m.ConfigSchema.Properties, m.ConfigSchema.Required, cat)
	errs = append(errs, perrs...)
	for _, r := range m.ConfigSchema.Required {
		if !slices.ContainsFunc(decls, func(d Decl) bool { return d.Name == r }) && !hasKeyErr(perrs, r) {
			errs = append(errs, newErr(ReasonManifestInvalid, r, "listed in configSchema.required but not declared"))
		}
	}
	if len(errs) > 0 {
		return Schema{}, errs
	}
	return NewSchema(m.Metadata.ID, m.Metadata.Version, decls), nil
}

func hasKeyErr(errs Errors, key string) bool {
	return slices.ContainsFunc(errs, func(e *Error) bool { return e.Key == key })
}

// parseProperties walks configSchema.properties in declaration order.
func parseProperties(props *yaml.Node, required []string, cat *Catalogue) ([]Decl, Errors) {
	if props.Kind == 0 {
		return nil, nil
	}
	if props.Kind != yaml.MappingNode {
		return nil, Errors{newErr(ReasonManifestInvalid, "", "configSchema.properties is not a mapping")}
	}
	var decls []Decl
	var errs Errors
	for i := 0; i+1 < len(props.Content); i += 2 {
		name := props.Content[i].Value
		var item propItem
		if err := props.Content[i+1].Decode(&item); err != nil {
			errs = append(errs, newErr(ReasonManifestInvalid, name, "configSchema item: "+err.Error()))
			continue
		}
		d, err := buildDecl(name, item, slices.Contains(required, name), cat)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		decls = append(decls, d)
	}
	return decls, errs
}

// buildDecl turns one configSchema item into a Decl. Decision order: P2.4/P2.12 declaration rules,
// retired names, default shape, then catalogue key / catalogue pattern / own key by brickKit type.
func buildDecl(name string, item propItem, required bool, cat *Catalogue) (Decl, *Error) {
	if item.Type == "" {
		item.Type = "string"
	}
	if err := CheckKeyDeclaration(KeyDeclaration{Key: name, Secret: item.Secret, Mount: item.Mount, Type: item.Type}); err != nil {
		e, _ := AsError(err)
		return Decl{}, e
	}
	if r, ok := cat.Retired(name); ok {
		return Decl{}, newErr(ReasonKeyInvalid, name, "retired name; use "+r.ReplacedBy)
	}
	def, err := defaultOf(name, &item.Default)
	if err != nil {
		return Decl{}, err
	}
	if ck, ok := cat.Key(name); ok {
		if ck.Type != item.Type || ck.Secret != item.Secret {
			return Decl{}, newErr(ReasonKeyInvalid, name, fmt.Sprintf("declared %s (secret %v), the catalogue says %s (secret %v)", item.Type, item.Secret, ck.Type, ck.Secret))
		}
		d := ck.Decl()
		if def != nil {
			d.Default = def
		}
		d.Required = d.Required || required
		return d, nil
	}
	d := Decl{Name: name, Type: item.Type, Required: required, Default: def, Secret: item.Secret, Minimum: item.Minimum}
	if e := ownFormat(&d, item, cat); e != nil {
		return Decl{}, e
	}
	return d, nil
}

// ownFormat derives the protocol format of a component's own key from its brickKit type (P2.3).
func ownFormat(d *Decl, item propItem, cat *Catalogue) *Error {
	if p, ok := cat.Pattern(d.Name); ok {
		if p.Type != item.Type {
			return newErr(ReasonKeyInvalid, d.Name, "a "+p.Pattern+" key is a "+p.Type)
		}
		d.Format = p.Format
		return nil
	}
	switch item.Type {
	case "string":
		d.Format = FormatString
		if len(item.Enum) > 0 {
			d.Format, d.Enum = FormatEnum, item.Enum
		}
	case "integer":
		d.Format = FormatInt
	case "boolean":
		d.Format = FormatBool
	case "object":
		d.Format, d.JSONKind = FormatJSON, JSONObject
	case "array":
		d.Format, d.JSONKind = FormatJSON, JSONArray
	default:
		return newErr(ReasonKeyInvalid, d.Name, "type "+item.Type+" has no protocol format (P2.3); declare a string")
	}
	return nil
}

// defaultOf returns a scalar default as written (5432, false and "5432" all become text); null = none.
func defaultOf(name string, n *yaml.Node) (*string, *Error) {
	if n.Kind == 0 || n.Tag == "!!null" {
		return nil, nil
	}
	if n.Kind != yaml.ScalarNode {
		return nil, newErr(ReasonManifestInvalid, name, "default is not a scalar")
	}
	v := n.Value
	return &v, nil
}
