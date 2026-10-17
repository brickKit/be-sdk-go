package sdk_test

import (
	"encoding/json"
	"io/fs"
	"os"
	"regexp"
	"strings"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
)

type capability struct {
	ID, Owner string
	Rules     []string
}

type feature struct {
	Status   string
	Evidence []string
	Pending  []struct{ Rule, Reason string }
}

// TestFeatureMatrix keeps features.json honest: exactly the SDK-owned capabilities of the pinned protocol, known
// statuses, evidence that names real tests, and pending rules that belong to their capability.
func TestFeatureMatrix(t *testing.T) {
	var caps struct {
		Protocol     string
		Capabilities []capability
	}
	if err := json.Unmarshal(protocol.Capabilities, &caps); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("features.json")
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Protocol string
		Features map[string]feature
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.Protocol != caps.Protocol {
		t.Errorf("features.json is for protocol %s, go.mod pins %s", m.Protocol, caps.Protocol)
	}
	tests := testNames(t)
	want := map[string]capability{}
	for _, c := range caps.Capabilities {
		if c.Owner == "sdk" {
			want[c.ID] = c
		}
	}
	for id, f := range m.Features {
		c, ok := want[id]
		if !ok {
			t.Errorf("%s: not an SDK capability of the protocol", id)
			continue
		}
		switch f.Status {
		case "verified", "implemented":
			if len(f.Evidence) == 0 {
				t.Errorf("%s: %s without evidence", id, f.Status)
			}
		case "not_implemented":
		default:
			t.Errorf("%s: unknown status %q", id, f.Status)
		}
		for _, e := range f.Evidence {
			if !tests[e] {
				t.Errorf("%s: evidence %s is not a test of this repository", id, e)
			}
		}
		rules := map[string]bool{}
		for _, r := range c.Rules {
			rules[r] = true
		}
		for _, p := range f.Pending {
			if !rules[p.Rule] || p.Reason == "" {
				t.Errorf("%s: pending %s is not one of its rules, or has no reason", id, p.Rule)
			}
		}
	}
	for id := range want {
		if _, ok := m.Features[id]; !ok {
			t.Errorf("%s: an SDK capability of the protocol missing from features.json", id)
		}
	}
}

func testNames(t *testing.T) map[string]bool {
	names := map[string]bool{}
	re := regexp.MustCompile(`(?m)^func (Test[A-Za-z0-9_]+)\(`)
	err := fs.WalkDir(os.DirFS("."), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return err
		}
		b, err := fs.ReadFile(os.DirFS("."), path)
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			names[m[1]] = true
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return names
}
