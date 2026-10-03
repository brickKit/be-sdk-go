// Package vectors runs the language-neutral test vectors of be-protocol (vectors/<area>/<topic>.json)
// from the pinned module's embedded FS. Only test code imports it; the public helper for components is
// besdktest.Vectors (G9), which wraps this package.
package vectors

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"testing"

	beprotocol "github.com/brickKit/be-protocol"
)

// Case is one vector case. Exactly one of Expected and ExpectedError is set.
type Case struct {
	ID            string          `json:"id"`
	Description   string          `json:"description"`
	Op            string          `json:"op"`
	Refs          []string        `json:"refs"`
	Input         json.RawMessage `json:"input"`
	Expected      json.RawMessage `json:"expected"`
	ExpectedError *struct {
		Reason string `json:"reason"`
	} `json:"expected_error"`
}

// File is one vector file.
type File struct {
	Area     string `json:"area"`
	Topic    string `json:"topic"`
	Protocol string `json:"protocol"`
	Cases    []Case `json:"cases"`
}

// Load reads vectors/<area>/<topic>.json from the pinned be-protocol module.
func Load(area, topic string) (File, error) {
	var f File
	b, err := fs.ReadFile(beprotocol.FS, path.Join("vectors", area, topic+".json"))
	if err != nil {
		return f, err
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return f, fmt.Errorf("vectors %s/%s: %w", area, topic, err)
	}
	return f, nil
}

// Topics lists the topics of an area, sorted.
func Topics(area string) ([]string, error) {
	ents, err := fs.ReadDir(beprotocol.FS, path.Join("vectors", area))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if !e.IsDir() && path.Ext(e.Name()) == ".json" {
			out = append(out, e.Name()[:len(e.Name())-len(".json")])
		}
	}
	sort.Strings(out)
	return out, nil
}

// Run runs every case of one file whose op is in ops (all ops when ops is empty) as a subtest named by
// the case id. A case whose op has no runner fails, so a new op in a later vector release is noticed.
func Run(t *testing.T, area, topic string, ops map[string]func(t *testing.T, c Case)) {
	t.Helper()
	f, err := Load(area, topic)
	if err != nil {
		t.Fatalf("load vectors: %v", err)
	}
	if len(f.Cases) == 0 {
		t.Fatalf("vectors %s/%s: no cases", area, topic)
	}
	for _, c := range f.Cases {
		c := c
		t.Run(c.ID, func(t *testing.T) {
			run, ok := ops[c.Op]
			if !ok {
				t.Fatalf("no runner for op %q (%s)", c.Op, c.Description)
			}
			run(t, c)
		})
	}
}

// JSONEqual reports whether two JSON documents are deeply equal (numbers compared as float64).
func JSONEqual(a, b []byte) (bool, error) {
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &y); err != nil {
		return false, err
	}
	xa, _ := json.Marshal(x)
	ya, _ := json.Marshal(y)
	return string(xa) == string(ya), nil
}

// RequireJSON fails the test when got (any Go value, marshalled) differs from the expected JSON.
func RequireJSON(t *testing.T, c Case, got any) {
	t.Helper()
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	eq, err := JSONEqual(b, c.Expected)
	if err != nil {
		t.Fatalf("compare: %v", err)
	}
	if !eq {
		t.Fatalf("%s\n input:    %s\n expected: %s\n got:      %s", c.Description, c.Input, c.Expected, b)
	}
}

// RequireReason fails the test unless the case expects an error with exactly this reason.
func RequireReason(t *testing.T, c Case, got string) {
	t.Helper()
	if c.ExpectedError == nil {
		t.Fatalf("%s\n input: %s\n expected success %s, got error reason %q", c.Description, c.Input, c.Expected, got)
	}
	if c.ExpectedError.Reason != got {
		t.Fatalf("%s\n input: %s\n expected reason %q, got %q", c.Description, c.Input, c.ExpectedError.Reason, got)
	}
}
