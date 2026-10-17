// Package config reads a component's declared keys (protocol chapter 02): strict types, empty means unset, no
// fallback on invalid values, every error reported at once, secrets as values or <KEY>_FILE.
//
// Modules never read the process environment: the launcher builds a Source from it (single run) or from the member's
// JSON (shell) and hands the parsed Values to the module through the runtime.
package config

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Type is a declared key's type.
type Type string

// The types of P2.4.
const (
	String   Type = "string"
	Integer  Type = "integer"
	Boolean  Type = "boolean"
	Duration Type = "duration"
	Enum     Type = "enum"
)

// Key declares one configuration key, as the component's configSchema does.
type Key struct {
	Name     string
	Type     Type
	Required bool
	Default  any      // string, int64, bool, time.Duration or an enum value; nil = no default
	Values   []string // Enum only
	Min, Max *int64   // Integer only, both inclusive
	Secret   bool
	// Process marks a process-level key that must agree across a shell's members (P2.11).
	Process bool
}

// Source looks a raw value up by key name. found=false and "" both mean unset (P2.5).
type Source interface {
	Lookup(name string) (value string, found bool)
}

// Map is a Source over a plain map: a shell member's config, or tests.
type Map map[string]string

// Lookup implements Source.
func (m Map) Lookup(name string) (string, bool) { v, ok := m[name]; return v, ok }

// Problem is one configuration error. It never holds a secret's value.
type Problem struct {
	Key      string
	Kind     string // missing | invalid | unknown_enum | out_of_range | conflict | undeclared
	Expected string
	Got      string // a prefix of a non-secret value, empty for secrets
}

func (p Problem) String() string {
	s := fmt.Sprintf("%s: %s", p.Key, p.Kind)
	if p.Expected != "" {
		s += " (expected " + p.Expected + ")"
	}
	if p.Got != "" {
		s += fmt.Sprintf(" got %q", p.Got)
	}
	return s
}

var (
	intRe = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
	durRe = regexp.MustCompile(`^(0|[1-9][0-9]*)(ms|s|m|h)$`)
	keyRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
)

var durUnit = map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour}

// PlatformKeys are injected by BrickKit and never declared by components (P2.3).
var PlatformKeys = []string{"COMPONENT_ID", "COMPONENT_VERSION", "PORT", "BRICKKIT_SERVED_MEMBERS",
	"BRICKKIT_SERVED_MEMBERS_CONFIG"}

// CheckDeclaration rejects key declarations the protocol forbids: bad names, _ENDPOINT suffixes, platform names,
// duplicates, missing enum values.
func CheckDeclaration(keys []Key) []Problem {
	var out []Problem
	seen := map[string]bool{}
	for _, k := range keys {
		switch {
		case !keyRe.MatchString(k.Name):
			out = append(out, Problem{Key: k.Name, Kind: "undeclared", Expected: "UPPER_SNAKE name"})
		case strings.HasSuffix(k.Name, "_ENDPOINT") || slices.Contains(PlatformKeys, k.Name):
			out = append(out, Problem{Key: k.Name, Kind: "undeclared", Expected: "a name the platform does not own"})
		case seen[k.Name]:
			out = append(out, Problem{Key: k.Name, Kind: "undeclared", Expected: "declared once"})
		case k.Type == Enum && len(k.Values) == 0:
			out = append(out, Problem{Key: k.Name, Kind: "undeclared", Expected: "enum values"})
		}
		seen[k.Name] = true
	}
	return out
}

func prefix(k Key, raw string) string {
	if k.Secret {
		return ""
	}
	if len(raw) > 64 {
		return raw[:64]
	}
	return raw
}

func expected(k Key) string {
	switch k.Type {
	case Integer:
		return "a base-10 integer"
	case Boolean:
		return "true or false"
	case Duration:
		return "an integer with ms, s, m or h"
	case Enum:
		return "one of " + strings.Join(k.Values, ", ")
	}
	return string(k.Type)
}

// parseOne converts one raw value; ok=false with the problem kind on failure.
func parseOne(k Key, raw string) (any, string) {
	switch k.Type {
	case String:
		return raw, ""
	case Integer:
		if !intRe.MatchString(raw) || raw == "-0" {
			return nil, "invalid"
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, "invalid"
		}
		if (k.Min != nil && n < *k.Min) || (k.Max != nil && n > *k.Max) {
			return nil, "out_of_range"
		}
		return n, ""
	case Boolean:
		if raw != "true" && raw != "false" {
			return nil, "invalid"
		}
		return raw == "true", ""
	case Duration:
		m := durRe.FindStringSubmatch(raw)
		if m == nil {
			return nil, "invalid"
		}
		n, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			return nil, "invalid"
		}
		return time.Duration(n) * durUnit[m[2]], ""
	case Enum:
		if slices.Contains(k.Values, raw) {
			return raw, ""
		}
		return nil, "unknown_enum"
	}
	return nil, "invalid"
}

// Parse reads every declared key from src. It returns the values, or every problem at once (P2.6).
func Parse(keys []Key, src Source) (*Values, []Problem) {
	if p := CheckDeclaration(keys); len(p) > 0 {
		return nil, p
	}
	return parse(keys, src)
}

// ParsePlatform reads keys the platform owns (COMPONENT_ID, PORT, ...), which components may not declare. Only the
// launcher and the shell call it.
func ParsePlatform(keys []Key, src Source) (*Values, []Problem) { return parse(keys, src) }

func parse(keys []Key, src Source) (*Values, []Problem) {
	v := &Values{keys: map[string]Key{}, vals: map[string]any{}, secrets: map[string]*Secret{}}
	var problems []Problem
	for _, k := range keys {
		v.keys[k.Name] = k
		raw, found := src.Lookup(k.Name)
		var file string
		if k.Secret {
			if fs, ok := src.(FileSource); ok {
				if path, has := fs.Lookup(k.Name + "_FILE"); has && path != "" {
					if found && raw != "" {
						problems = append(problems, Problem{Key: k.Name, Kind: "conflict", Expected: "either " + k.Name + " or " + k.Name + "_FILE"})
						continue
					}
					content, err := fs.ReadFile(path)
					if err != nil {
						problems = append(problems, Problem{Key: k.Name + "_FILE", Kind: "invalid", Expected: "a readable file", Got: path})
						continue
					}
					raw, found, file = trimNewline(content), true, path
				}
			}
		}
		if !found || raw == "" {
			if k.Required {
				problems = append(problems, Problem{Key: k.Name, Kind: "missing", Expected: expected(k)})
				continue
			}
			if k.Default != nil {
				v.vals[k.Name] = normalizeDefault(k)
			}
			continue
		}
		val, kind := parseOne(k, raw)
		if kind != "" {
			problems = append(problems, Problem{Key: k.Name, Kind: kind, Expected: expected(k), Got: prefix(k, raw)})
			continue
		}
		v.vals[k.Name] = val
		if k.Secret {
			s := &Secret{path: file}
			s.value.Store(raw)
			v.secrets[k.Name] = s
		}
	}
	if len(problems) > 0 {
		return nil, problems
	}
	return v, nil
}

func trimNewline(b []byte) string {
	s := string(b)
	s = strings.TrimSuffix(s, "\n")
	return strings.TrimSuffix(s, "\r")
}

func normalizeDefault(k Key) any {
	switch d := k.Default.(type) {
	case int:
		return int64(d)
	case time.Duration, int64, bool, string:
		return d
	}
	return k.Default
}
