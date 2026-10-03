package config

import (
	"bytes"
	"fmt"
	"io/fs"
	"regexp"

	beprotocol "github.com/brickKit/be-protocol"
	"gopkg.in/yaml.v3"
)

// cataloguePath is the protocol key catalogue inside the pinned be-protocol module (P2).
const cataloguePath = "schemas/config-keys.yaml"

// CatalogueKey is one protocol key of schemas/config-keys.yaml (format: config-keys.schema.json).
type CatalogueKey struct {
	Name         string   `yaml:"name"`
	Type         string   `yaml:"type"`
	Format       string   `yaml:"format"`
	Enum         []string `yaml:"enum"`
	JSONSchema   string   `yaml:"json_schema"`
	Schemes      []string `yaml:"schemes"`
	Required     bool     `yaml:"required"`
	OneOf        []string `yaml:"one_of"`
	Default      *string  `yaml:"default"`
	DefaultFrom  string   `yaml:"default_from"`
	ShellDefault *string  `yaml:"shell_default"`
	Secret       bool     `yaml:"secret"`
	Mount        string   `yaml:"mount"`
	Shared       bool     `yaml:"shared"`
	Shell        string   `yaml:"shell"` // whose value a shell uses: process | member (P19.3)
	Profiles     []string `yaml:"profiles"`
	AppliesWhen  string   `yaml:"applies_when"`
	Ref          string   `yaml:"ref"`
	Description  string   `yaml:"description"`
}

// Decl returns the key's declaration as the catalogue gives it (P2.8). A json value is an object (P2.3).
func (k CatalogueKey) Decl() Decl {
	d := Decl{
		Name: k.Name, Type: k.Type, Format: k.Format, Required: k.Required, Default: k.Default,
		DefaultFrom: k.DefaultFrom, ShellDefault: k.ShellDefault, Secret: k.Secret, Enum: k.Enum,
		Schemes: k.Schemes, JSONSchema: k.JSONSchema, OneOf: k.OneOf, Protocol: true,
	}
	if k.Format == FormatJSON {
		d.JSONKind = JSONObject
	}
	return d
}

// CataloguePattern is a component-defined key name that follows a protocol pattern (*_NO_FORMAT).
type CataloguePattern struct {
	Pattern     string `yaml:"pattern"`
	Type        string `yaml:"type"`
	Format      string `yaml:"format"`
	Ref         string `yaml:"ref"`
	Description string `yaml:"description"`
	re          *regexp.Regexp
}

// RetiredKey is a name that is never declared any more, with its replacement.
type RetiredKey struct {
	Name       string `yaml:"name"`
	ReplacedBy string `yaml:"replaced_by"`
	Note       string `yaml:"note"`
}

// Catalogue is be-protocol's configuration key catalogue (schemas/config-keys.yaml, P2). It is
// immutable after LoadCatalogue and safe for concurrent use.
type Catalogue struct {
	Protocol       string
	Keys           []CatalogueKey
	Patterns       []CataloguePattern
	ReservedExact  []string
	ReservedSuffix []string
	SecretRoot     string // where brickKit mounts secrets (P2.7)
	RereadSeconds  int    // the longest interval between two checks of a secret file (P2.9)
	RetiredKeys    []RetiredKey
	byName         map[string]int
}

// catalogueFile mirrors the YAML document; unknown fields fail so catalogue drift is noticed.
type catalogueFile struct {
	Protocol string             `yaml:"protocol"`
	Keys     []CatalogueKey     `yaml:"keys"`
	Patterns []CataloguePattern `yaml:"patterns"`
	Reserved struct {
		Exact  []string `yaml:"exact"`
		Suffix []string `yaml:"suffix"`
	} `yaml:"reserved"`
	Secrets struct {
		Mount         string `yaml:"mount"`
		Suffix        string `yaml:"suffix"`
		Root          string `yaml:"root"`
		RereadSeconds int    `yaml:"reread_seconds"`
	} `yaml:"secrets"`
	Retired []RetiredKey `yaml:"retired"`
}

// LoadCatalogue parses schemas/config-keys.yaml from the pinned be-protocol module (P2, P2.8).
func LoadCatalogue() (*Catalogue, error) {
	b, err := fs.ReadFile(beprotocol.FS, cataloguePath)
	if err != nil {
		return nil, fmt.Errorf("config catalogue: %w", err)
	}
	c, err := parseCatalogue(b)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func parseCatalogue(b []byte) (*Catalogue, error) {
	var f catalogueFile
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("config catalogue: %w", err)
	}
	c := &Catalogue{
		Protocol: f.Protocol, Keys: f.Keys, Patterns: f.Patterns, ReservedExact: f.Reserved.Exact,
		ReservedSuffix: f.Reserved.Suffix, SecretRoot: f.Secrets.Root, RereadSeconds: f.Secrets.RereadSeconds,
		RetiredKeys: f.Retired, byName: make(map[string]int, len(f.Keys)),
	}
	for i, k := range c.Keys {
		if _, dup := c.byName[k.Name]; dup {
			return nil, fmt.Errorf("config catalogue: key %s listed twice", k.Name)
		}
		c.byName[k.Name] = i
	}
	for i := range c.Patterns {
		re, err := regexp.Compile(c.Patterns[i].Pattern)
		if err != nil {
			return nil, fmt.Errorf("config catalogue: pattern %q: %w", c.Patterns[i].Pattern, err)
		}
		c.Patterns[i].re = re
	}
	return c, nil
}

// Key returns the protocol key named name.
func (c *Catalogue) Key(name string) (CatalogueKey, bool) {
	i, ok := c.byName[name]
	if !ok {
		return CatalogueKey{}, false
	}
	return c.Keys[i], true
}

// Pattern returns the protocol pattern a component-defined key name follows (*_NO_FORMAT).
func (c *Catalogue) Pattern(name string) (CataloguePattern, bool) {
	for _, p := range c.Patterns {
		if p.re.MatchString(name) {
			return p, true
		}
	}
	return CataloguePattern{}, false
}

// Retired returns the retired entry for name: a name that is never declared any more.
func (c *Catalogue) Retired(name string) (RetiredKey, bool) {
	for _, r := range c.RetiredKeys {
		if r.Name == name {
			return r, true
		}
	}
	return RetiredKey{}, false
}
