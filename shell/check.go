package shell

import (
	"encoding/json"
	"fmt"
	"reflect"
	goruntime "runtime"
	"runtime/debug"
	"slices"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/internal/host"
)

// Served is one member as BrickKit asks the shell to host it (BRICKKIT_SERVED_MEMBERS_CONFIG).
type Served struct {
	ComponentID string            `json:"componentId"`
	Version     string            `json:"version"`
	HTTPPort    int               `json:"httpPort"`
	ExtraPorts  []Port            `json:"extraPorts"`
	Config      map[string]string `json:"config"`
}

func (s Served) extraPort(name string) int {
	for _, p := range s.ExtraPorts {
		if p.Name == name {
			return p.Port
		}
	}
	return 0
}

// Port is a named extra port.
type Port struct {
	Name string `json:"name"`
	Port int    `json:"port"`
}

// refusal is one reason the shell will not start, naming the member.
type refusal struct{ member, key, kind, detail string }

func (r refusal) problem() config.Problem {
	return config.Problem{Key: r.member + ":" + r.key, Kind: r.kind, Expected: r.detail}
}

// planned is a member that passed every check.
type planned struct {
	def    component.Definition
	served Served
	values *config.Values
}

// moduleOf finds which Go module provides a definition: the package of its New function, matched against the
// build info's dependencies (longest module path that is a prefix).
func moduleOf(def component.Definition, bi *debug.BuildInfo) (*debug.Module, string) {
	if def.New == nil || bi == nil {
		return nil, ""
	}
	pkg := packageOf(goruntime.FuncForPC(reflect.ValueOf(def.New).Pointer()).Name())
	var best *debug.Module
	for _, d := range bi.Deps {
		if (pkg == d.Path || strings.HasPrefix(pkg, d.Path+"/")) && (best == nil || len(d.Path) > len(best.Path)) {
			best = d
		}
	}
	return best, pkg
}

// packageOf returns the package path of a function's full name: "github.com/x/y/v3/sales.(*m).New.func1" ->
// "github.com/x/y/v3/sales".
func packageOf(funcName string) string {
	i := strings.LastIndex(funcName, "/")
	if j := strings.Index(funcName[i+1:], "."); j >= 0 {
		return funcName[:i+1+j]
	}
	return funcName
}

// check applies protocol 17's refusals: members listed but not compiled in, compiled at another version (by the
// definition and by the Go build info, no replace in a release build), members whose configuration is invalid, and
// process-level keys that disagree. Every refusal is returned at once.
func check(raw string, defs []component.Definition, bi *debug.BuildInfo, modOf func(component.Definition, *debug.BuildInfo) (*debug.Module, string),
	allowReplace bool) ([]planned, []refusal, []string) {
	var served []Served
	if err := json.Unmarshal([]byte(raw), &served); err != nil {
		return nil, []refusal{{"shell", "BRICKKIT_SERVED_MEMBERS_CONFIG", "invalid", "JSON list of members: " + err.Error()}}, nil
	}
	byID := map[string]component.Definition{}
	for _, d := range defs {
		byID[d.ID] = d
	}
	var out []planned
	var refusals []refusal
	var warnings []string
	seenIDs, seenPorts := map[string]bool{}, map[int]string{}
	for _, s := range served {
		id := s.ComponentID
		if seenIDs[id] {
			refusals = append(refusals, refusal{id, "componentId", "invalid", "listed twice"})
			continue
		}
		seenIDs[id] = true
		if other, clash := seenPorts[s.HTTPPort]; clash || s.HTTPPort <= 0 {
			refusals = append(refusals, refusal{id, "httpPort", "invalid", fmt.Sprintf("a free port (%d is taken by %s)", s.HTTPPort, other)})
		}
		seenPorts[s.HTTPPort] = id
		for _, p := range s.ExtraPorts {
			if other, clash := seenPorts[p.Port]; clash || p.Port <= 0 {
				refusals = append(refusals, refusal{id, "extraPorts." + p.Name, "invalid", fmt.Sprintf("a free port (%d is taken by %s)", p.Port, other)})
			}
			seenPorts[p.Port] = id
		}
		def, ok := byID[id]
		if !ok {
			refusals = append(refusals, refusal{id, "componentId", "invalid", "a member compiled into this shell; this shell has no module " + id})
			continue
		}
		if def.Version != s.Version {
			refusals = append(refusals, refusal{id, "version", "invalid",
				fmt.Sprintf("%s, the version compiled in is %s", s.Version, def.Version)})
		}
		if mod, pkg := modOf(def, bi); mod == nil {
			refusals = append(refusals, refusal{id, "module", "invalid", "the Go module providing " + pkg + " is not in the build info"})
		} else {
			if want := goVersion(s.Version); mod.Version != want {
				refusals = append(refusals, refusal{id, "module", "invalid",
					fmt.Sprintf("module %s at %s, but the build info has %s", mod.Path, want, mod.Version)})
			}
			if mod.Replace != nil {
				msg := fmt.Sprintf("module %s is replaced by %s %s", mod.Path, mod.Replace.Path, mod.Replace.Version)
				if allowReplace {
					warnings = append(warnings, id+": "+msg+" (allowed by SHELL_ALLOW_REPLACE; never in a release build)")
				} else {
					refusals = append(refusals, refusal{id, "module", "invalid", "no replace in a release build; " + msg})
				}
			}
		}
		values, problems := config.Parse(host.Keys(def), config.Map(s.Config))
		for _, p := range problems {
			refusals = append(refusals, refusal{id, p.Key, p.Kind, p.Expected})
		}
		if values != nil {
			out = append(out, planned{def: def, served: s, values: values})
		}
	}
	refusals = append(refusals, processKeysAgree(out)...)
	refusals = append(refusals, targetsAgree(out)...)
	return out, refusals, warnings
}

// targetsAgree checks the per-target database keys (P2.11): members on the same target share one pool, so they must
// agree on how statements are executed there.
func targetsAgree(members []planned) []refusal {
	first := map[db.Target]planned{}
	var out []refusal
	for _, m := range members {
		if m.def.Migrations == nil {
			continue
		}
		s := db.SettingsFrom(m.values)
		f, ok := first[s.Target]
		if !ok {
			first[s.Target] = m
			continue
		}
		if fs := db.SettingsFrom(f.values); fs.Prepared != s.Prepared {
			out = append(out, refusal{m.def.ID, "PG_POOLER_PREPARED", "invalid",
				fmt.Sprintf("the same value as %s (members on target %s share one pool)", f.def.ID, s.Target)})
		}
	}
	return out
}

// goVersion is the Go module version of a component version: v + version (the /vN path suffix is in the module path).
func goVersion(v string) string { return "v" + v }

// processKeysAgree checks that every key marked Process has the same value in every member that declares it (P2.11).
func processKeysAgree(members []planned) []refusal {
	type seen struct {
		member string
		value  any
	}
	first := map[string]seen{}
	var out []refusal
	for _, m := range members {
		for _, k := range host.Keys(m.def) {
			if !k.Process {
				continue
			}
			v, _ := m.values.Raw(k.Name)
			if f, ok := first[k.Name]; !ok {
				first[k.Name] = seen{m.def.ID, v}
			} else if !reflect.DeepEqual(f.value, v) {
				out = append(out, refusal{m.def.ID, k.Name, "invalid",
					fmt.Sprintf("the same value as %s (a process-level key must agree across the shell)", f.member)})
			}
		}
	}
	slices.SortFunc(out, func(a, b refusal) int { return strings.Compare(a.member+a.key, b.member+b.key) })
	return out
}
