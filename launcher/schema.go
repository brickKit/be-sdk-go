package launcher

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/host"
	"go.yaml.in/yaml/v3"
)

// fileKeys are the keys whose value is a file path BrickKit mounts (the service certificates, re-read on rotation).
var fileKeys = map[string]bool{"SERVICE_TLS_CERT_FILE": true, "SERVICE_TLS_KEY_FILE": true, "SERVICE_TLS_CA_FILE": true}

// ConfigSchema is the configSchema block of the component's component.yaml: exactly the keys the SDK reads for serve
// and migrate (the component's own, the SDK's, the database's and the bus's), with their types, secrets and defaults.
// own describes the component's own keys. A component's test compares its manifest with it, so the manifest never
// declares a key nobody reads nor misses one that is read; the Python SDK's probes do the same (config_schema.py).
func ConfigSchema(def component.Definition, own map[string]string) map[string]any {
	seen := map[string]bool{}
	var keys []config.Key
	all := host.Keys(def)
	if def.Migrations != nil { // the migrate entry reads the owner role and the direct address too
		all = append(all, db.MigrateKeys()...)
	}
	for _, k := range all {
		if !seen[k.Name] {
			seen[k.Name] = true
			keys = append(keys, k)
		}
	}
	required := []string{}
	props := map[string]any{}
	for _, k := range keys {
		if k.Required {
			required = append(required, k.Name)
		}
		p := map[string]any{"type": schemaType(k.Type)}
		if k.Type == config.Enum {
			p["enum"] = append([]string(nil), k.Values...)
		}
		if k.Secret || fileKeys[k.Name] {
			p["secret"] = true
		}
		if fileKeys[k.Name] {
			p["mount"] = "file"
		}
		if d := describe(k, own[k.Name]); d != "" {
			p["description"] = d
		}
		props[k.Name] = p
	}
	sort.Strings(required)
	return map[string]any{"type": "object", "required": required, "properties": props}
}

func schemaType(t config.Type) string {
	switch t {
	case config.Integer:
		return "integer"
	case config.Boolean:
		return "boolean"
	}
	return "string" // string, duration, enum
}

func describe(k config.Key, own string) string {
	var parts []string
	if own != "" {
		parts = append(parts, own)
	}
	if k.Type == config.Duration {
		parts = append(parts, "a duration (e.g. 2s, 500ms)")
	}
	if k.Default != nil {
		d := k.Default
		if dur, ok := d.(time.Duration); ok {
			d = fmt.Sprintf("%gs", dur.Seconds())
		}
		parts = append(parts, fmt.Sprintf("SDK default %v", d))
	}
	if k.Process {
		parts = append(parts, "process-level: the same for every member of a shell")
	}
	if fileKeys[k.Name] {
		parts = append(parts, "a file path, re-read on rotation")
	}
	return strings.Join(parts, "; ")
}

// ManifestProblems compares a component.yaml with the code: metadata.id and metadata.version with the definition's
// ID and Version (a release that bumps one and not the other would refuse to start), then the configSchema with
// ConfigSchema: every key missing, extra or declared differently, in key order. A component's test asserts it is
// empty and prints ConfigSchemaYAML otherwise.
func ManifestProblems(manifest []byte, def component.Definition, own map[string]string) []string {
	var doc struct {
		Metadata struct {
			ID      string `yaml:"id"`
			Version string `yaml:"version"`
		} `yaml:"metadata"`
		ConfigSchema map[string]any `yaml:"configSchema"`
	}
	if err := yaml.Unmarshal(manifest, &doc); err != nil {
		return []string{"component.yaml: " + err.Error()}
	}
	var out []string
	if doc.Metadata.ID != def.ID {
		out = append(out, fmt.Sprintf("metadata.id: %q, the code is %q", doc.Metadata.ID, def.ID))
	}
	if doc.Metadata.Version != def.Version {
		out = append(out, fmt.Sprintf("metadata.version: %q, the code is %q", doc.Metadata.Version, def.Version))
	}
	want := ConfigSchema(def, own)
	got := doc.ConfigSchema
	gotReq := map[string]bool{}
	if l, ok := got["required"].([]any); ok {
		for _, v := range l {
			gotReq[fmt.Sprint(v)] = true
		}
	}
	for _, k := range want["required"].([]string) {
		if !gotReq[k] {
			out = append(out, k+": not listed as required")
		}
		delete(gotReq, k)
	}
	for k := range gotReq {
		out = append(out, k+": listed as required, but the SDK does not require it")
	}
	wantProps := want["properties"].(map[string]any)
	gotProps, _ := got["properties"].(map[string]any)
	names := make([]string, 0, len(wantProps)+len(gotProps))
	for k := range wantProps {
		names = append(names, k)
	}
	for k := range gotProps {
		if _, ok := wantProps[k]; !ok {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	for _, k := range names {
		w, inWant := wantProps[k]
		g, inGot := gotProps[k]
		switch {
		case !inGot:
			out = append(out, k+": not declared")
		case !inWant:
			out = append(out, k+": declared, but nothing reads it")
		case fmt.Sprint(normalize(g)) != fmt.Sprint(normalize(w)):
			out = append(out, fmt.Sprintf("%s: declared as %v, the SDK reads %v", k, normalize(g), normalize(w)))
		}
	}
	return out
}

// EventProblems compares the events.publishes list of a component.yaml with the event contracts the component
// publishes: contracts holds one <event type>.schema.json per published type (the component's contract/events/).
// Every contract must be listed and every listed type must have a contract; BrickKit's dependency report and the
// project's event topology come from the list. Subscriptions are known only to the running module, so the
// component keeps events.subscribes in step itself.
func EventProblems(manifest []byte, contracts fs.FS) []string {
	var doc struct {
		Events struct {
			Publishes []string `yaml:"publishes"`
		} `yaml:"events"`
	}
	if err := yaml.Unmarshal(manifest, &doc); err != nil {
		return []string{"component.yaml: " + err.Error()}
	}
	files, err := fs.Glob(contracts, "*.schema.json")
	if err != nil {
		return []string{"event contracts: " + err.Error()}
	}
	have := map[string]bool{}
	for _, f := range files {
		have[strings.TrimSuffix(f, ".schema.json")] = true
	}
	listed := map[string]bool{}
	var out []string
	for _, e := range doc.Events.Publishes {
		listed[e] = true
		if !have[e] {
			out = append(out, e+": in events.publishes, but there is no "+e+".schema.json")
		}
	}
	for _, f := range files {
		if e := strings.TrimSuffix(f, ".schema.json"); !listed[e] {
			out = append(out, e+": has a contract, but is not in events.publishes")
		}
	}
	sort.Strings(out)
	return out
}

// normalize makes a property comparable whichever way it was decoded (YAML gives []any, ConfigSchema []string).
func normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range x {
			out[k] = normalize(e)
		}
		return out
	case []any:
		s := make([]string, len(x))
		for i, e := range x {
			s[i] = fmt.Sprint(e)
		}
		return s
	}
	return v
}

// ConfigSchemaYAML is ConfigSchema as the configSchema block of a component.yaml, to paste.
func ConfigSchemaYAML(def component.Definition, own map[string]string) string {
	var b strings.Builder
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2) // as the rest of a component.yaml
	_ = enc.Encode(map[string]any{"configSchema": ConfigSchema(def, own)})
	_ = enc.Close()
	return b.String()
}
