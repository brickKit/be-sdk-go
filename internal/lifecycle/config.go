package lifecycle

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Mode is DATA_LIFECYCLE's mode (P16.9).
type Mode string

// The three modes. dry-run plans and reports seals and drops without doing them; off does neither.
// Keeping the partition window ahead runs in every mode: it is what lets the component write (G1).
const (
	ModeOn     Mode = "on"
	ModeDryRun Mode = "dry-run"
	ModeOff    Mode = "off"
)

// ConfigKey is the configuration key this file reads.
const ConfigKey = "DATA_LIFECYCLE"

// Config is a parsed DATA_LIFECYCLE value (schemas/data-lifecycle-config.schema.json).
type Config struct {
	Mode      Mode
	ColdStore string
	ColdQuery string
	Publisher string
	PII       string
	overrides map[string]rawOverride
}

type rawOverride struct {
	Retention struct {
		Min string `json:"min"`
	} `json:"retention"`
	Tiers struct {
		Hot, Seal, Cold string
	} `json:"tiers"`
}

type rawConfig struct {
	Mode      Mode                       `json:"mode"`
	ColdStore string                     `json:"cold_store"`
	ColdQuery string                     `json:"cold_query"`
	Publisher string                     `json:"publisher"`
	PII       string                     `json:"pii"`
	Tables    map[string]rawOverride     `json:"tables"`
	Tenants   map[string]json.RawMessage `json:"tenants"`
}

// ConfigError is a fatal problem in DATA_LIFECYCLE; the start fails naming it (P1.8, P16.9).
type ConfigError struct {
	Table string
	Msg   string
}

func (e *ConfigError) Error() string {
	if e.Table == "" {
		return ConfigKey + ": " + e.Msg
	}
	return ConfigKey + ": table " + e.Table + ": " + e.Msg
}

// supported are the adapters this SDK version has: the 1.0 defaults only (hot and warm tiers).
var supported = map[string]string{"cold_store": "none", "cold_query": "none", "publisher": "none", "pii": "plain"}

// ParseConfig reads DATA_LIFECYCLE, a JSON object or a YAML 1.2 mapping; "" is the default
// {"mode":"on"} (P2.3: empty is unset). It checks the schema and refuses, naming it, any adapter
// this SDK version does not have (P16.9).
func ParseConfig(value string) (Config, error) {
	if strings.TrimSpace(value) == "" {
		value = `{"mode":"on"}`
	}
	raw, doc, err := yamlToJSON([]byte(value))
	if err != nil {
		return Config{}, &ConfigError{Msg: err.Error()}
	}
	loc, msg, ok, err := validate(configSchemaURL, doc)
	if err != nil {
		return Config{}, err
	}
	if !ok {
		if len(loc) >= 2 && loc[0] == "tables" {
			return Config{}, &ConfigError{Table: loc[1], Msg: msg}
		}
		return Config{}, &ConfigError{Msg: msg}
	}
	var r rawConfig
	if err := json.Unmarshal(raw, &r); err != nil {
		return Config{}, &ConfigError{Msg: err.Error()}
	}
	c := Config{Mode: or(r.Mode, ModeOn), ColdStore: or(r.ColdStore, "none"), ColdQuery: or(r.ColdQuery, "none"),
		Publisher: or(r.Publisher, "none"), PII: or(r.PII, "plain"), overrides: r.Tables}
	for _, a := range []struct{ key, value string }{{"cold_store", c.ColdStore}, {"cold_query", c.ColdQuery},
		{"publisher", c.Publisher}, {"pii", c.PII}} {
		if a.value != supported[a.key] {
			return Config{}, &ConfigError{Msg: fmt.Sprintf("adapter %s %s is not supported by this SDK version (it has %s only)", a.key, a.value, supported[a.key])}
		}
	}
	if len(r.Tenants) > 0 {
		return Config{}, &ConfigError{Msg: "tenants (pooled multi-tenancy) is not supported by this SDK version"}
	}
	return c, nil
}

func or[T ~string](v, def T) T {
	if v == "" {
		return def
	}
	return v
}

// Apply returns a copy of d with the deployment's per-table overrides applied. An override may only
// lengthen retention.min and postpone sealing and freezing; anything else fails the start naming the
// table (P16.9). d itself is not changed.
func (c Config) Apply(d *Declaration) (*Declaration, error) {
	out := &Declaration{Tables: make(map[string]*Table, len(d.Tables))}
	for n, t := range d.Tables {
		cp := *t
		if t.RetentionMin != nil {
			m := *t.RetentionMin
			cp.RetentionMin = &m
		}
		out.Tables[n] = &cp
	}
	for _, name := range sortedKeys(c.overrides) {
		t, ok := out.Tables[name]
		if !ok {
			return nil, &ConfigError{Table: name, Msg: "not declared in lifecycle.yaml"}
		}
		if err := applyOverride(t, c.overrides[name]); err != nil {
			return nil, &ConfigError{Table: name, Msg: err.Error()}
		}
	}
	for _, t := range out.Tables {
		if t.inheritsSeal {
			t.Seal = out.Tables[t.Follows].Seal
		}
	}
	return out, nil
}

func applyOverride(t *Table, o rawOverride) error {
	if m := o.Retention.Min; m != "" {
		if err := lengthenRetention(t, m); err != nil {
			return err
		}
	}
	if s := o.Tiers.Seal; s != "" {
		next, _ := parseSeal(s) // the schema admitted it
		if !postpones(t.Seal, next) {
			return fmt.Errorf("tiers.seal %s would seal earlier than the declared %s", s, sealString(t.Seal))
		}
		t.Seal = next
	}
	if cold := o.Tiers.Cold; cold != "" {
		if !postponesAfter(t.Cold, cold) {
			return fmt.Errorf("tiers.cold %s would freeze earlier than the declared %s", cold, t.Cold)
		}
		t.Cold = cold
	}
	if o.Tiers.Hot != "" {
		t.Hot = o.Tiers.Hot
	}
	return nil
}

func lengthenRetention(t *Table, m string) error {
	if m == "forever" {
		t.Forever, t.RetentionMin = true, nil
		return nil
	}
	a, _ := parseAfter(m)
	switch {
	case t.Forever:
		return fmt.Errorf("retention.min %s is below the declared forever", m)
	case t.RetentionMin != nil && (a.Anchor != t.RetentionMin.Anchor || !a.AtLeast(t.RetentionMin.Span)):
		return fmt.Errorf("retention.min %s is below the declared minimum %s", m, t.RetentionMin)
	}
	t.RetentionMin = &a
	return nil
}

// postpones: only an after-anchor seal may move, to the same anchor and a span at least as long, or
// to never; immediate and on_signal are the component's integrity rules and never change.
func postpones(declared, next Seal) bool {
	switch {
	case declared == next || declared.Kind == SealNone:
		return true
	case declared.Kind != SealAfter:
		return false
	case next.Kind == SealNone:
		return true
	}
	return next.Kind == SealAfter && next.After.Anchor == declared.After.Anchor && next.After.AtLeast(declared.After.Span)
}

// postponesAfter: freezing may move to never, or to the same anchor and a span at least as long;
// nothing declared admits anything, and a declared never stays never.
func postponesAfter(declared, next string) bool {
	switch {
	case declared == "" || next == "never":
		return true
	case declared == "never":
		return false
	}
	d, _ := parseAfter(declared)
	n, ok := parseAfter(next)
	return ok && n.Anchor == d.Anchor && n.AtLeast(d.Span)
}

func sealString(s Seal) string {
	switch s.Kind {
	case SealImmediate:
		return "immediate"
	case SealOnSignal:
		return "on_signal"
	case SealAfter:
		return s.After.String()
	}
	return "never"
}
