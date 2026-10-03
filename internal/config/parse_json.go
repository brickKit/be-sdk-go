package config

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// JSON kinds a json key may require (vectors json_kind).
const (
	JSONObject = "object"
	JSONArray  = "array"
)

// maxJSONDepth bounds the nesting a configuration value may have.
const maxJSONDepth = 64

// ParseJSON checks a json value (P2.3, vectors json): I-JSON (valid UTF-8, duplicate names and
// non-JSON tokens such as NaN rejected) and, when kind is not empty, an object or an array. It returns
// the value compacted.
func ParseJSON(key, raw, kind string) ([]byte, error) {
	if !utf8.ValidString(raw) {
		return nil, newErr(ReasonInvalid, key, "not valid UTF-8")
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.UseNumber()
	first, err := strictWalk(dec, 0)
	if err != nil {
		return nil, newErr(ReasonInvalid, key, "not I-JSON: "+err.Error())
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, newErr(ReasonInvalid, key, "not I-JSON: trailing data")
	}
	if kind == JSONObject && first != '{' || kind == JSONArray && first != '[' {
		return nil, newErr(ReasonInvalid, key, "JSON value is not an "+kind)
	}
	var out bytes.Buffer
	if err := json.Compact(&out, []byte(raw)); err != nil {
		return nil, newErr(ReasonInvalid, key, "not I-JSON")
	}
	return out.Bytes(), nil
}

// strictWalk consumes one JSON value from dec, rejecting duplicate object names. It returns '{' or
// '[' for a container and 0 for a scalar.
func strictWalk(dec *json.Decoder, depth int) (json.Delim, error) {
	if depth > maxJSONDepth {
		return 0, errText("nested too deeply")
	}
	tok, err := dec.Token()
	if err != nil {
		return 0, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return 0, nil
	}
	seen := map[string]bool{}
	for dec.More() {
		if delim == '{' {
			name, err := dec.Token()
			if err != nil {
				return 0, err
			}
			s, _ := name.(string)
			if seen[s] {
				return 0, errText("duplicate name")
			}
			seen[s] = true
		}
		if _, err := strictWalk(dec, depth+1); err != nil {
			return 0, err
		}
	}
	if _, err := dec.Token(); err != nil {
		return 0, err
	}
	return delim, nil
}

type errText string

func (e errText) Error() string { return string(e) }

// yamlMappingToJSON converts a YAML mapping (YAML 1.2 core schema as yaml.v3 reads it: on and off
// are strings) to JSON. Only DATA_LIFECYCLE accepts this form (P16.9).
func yamlMappingToJSON(key, raw string) (string, error) {
	var v map[string]any
	if err := yaml.Unmarshal([]byte(raw), &v); err != nil || v == nil {
		return "", newErr(ReasonInvalid, key, "neither a JSON object nor a YAML mapping")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", newErr(ReasonInvalid, key, "YAML mapping has no JSON form")
	}
	return string(b), nil
}
