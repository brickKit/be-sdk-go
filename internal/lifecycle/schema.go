package lifecycle

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

// The protocol schemas, by $id, compiled once from the pinned be-protocol (no network).
const (
	declSchemaURL   = "https://github.com/brickKit/be-protocol/schemas/lifecycle.schema.json"
	configSchemaURL = "https://github.com/brickKit/be-protocol/schemas/data-lifecycle-config.schema.json"
)

var compiled = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.UseLoader(offlineLoader{})
	for url, file := range map[string]string{declSchemaURL: "schemas/lifecycle.schema.json",
		configSchemaURL: "schemas/data-lifecycle-config.schema.json"} {
		raw, err := fs.ReadFile(beprotocol.FS, file)
		if err != nil {
			return nil, fmt.Errorf("lifecycle: protocol schema %s: %w", file, err)
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		if err := c.AddResource(url, doc); err != nil {
			return nil, err
		}
	}
	out := map[string]*jsonschema.Schema{}
	for _, url := range []string{declSchemaURL, configSchemaURL} {
		s, err := c.Compile(url)
		if err != nil {
			return nil, fmt.Errorf("lifecycle: compile %s: %w", url, err)
		}
		out[url] = s
	}
	return out, nil
})

type offlineLoader struct{}

func (offlineLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("schema %s is not available offline", url)
}

// yamlToJSON reads a YAML 1.2 document (yaml.v3: on/off/yes/no are strings unless decoded into a
// bool, timestamps stay strings, duplicate keys are errors) and re-encodes it as JSON, the one form
// both the schema validator and the typed decoder read.
func yamlToJSON(data []byte) ([]byte, any, error) {
	var v any
	if err := yaml.Unmarshal(data, &v); err != nil {
		return nil, nil, err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, nil, err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, nil, err
	}
	return raw, doc, nil
}

// validate checks doc against one protocol schema and, on failure, returns the deepest violation
// with its instance location (e.g. ["tables", "gl", "pii"]).
func validate(url string, doc any) (loc []string, msg string, ok bool, err error) {
	schemas, err := compiled()
	if err != nil {
		return nil, "", false, err
	}
	verr := schemas[url].Validate(doc)
	if verr == nil {
		return nil, "", true, nil
	}
	var ve *jsonschema.ValidationError
	if !errors.As(verr, &ve) {
		return nil, verr.Error(), false, nil
	}
	leaf := deepest(ve)
	return leaf.InstanceLocation, leafMessage(leaf), false, nil
}

func deepest(ve *jsonschema.ValidationError) *jsonschema.ValidationError {
	best := ve
	for _, c := range ve.Causes {
		if d := deepest(c); len(d.InstanceLocation) > len(best.InstanceLocation) {
			best = d
		}
	}
	return best
}

func leafMessage(ve *jsonschema.ValidationError) string {
	s := ve.Error()
	if i := strings.LastIndex(s, "\n"); i >= 0 {
		s = s[i+1:]
	}
	return strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(s), "- "))
}
