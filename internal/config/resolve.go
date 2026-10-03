package config

import (
	"path/filepath"
	"strings"
	"time"
)

// value is one resolved configuration value: the text that applies (after defaults) and its parse.
type value struct {
	set  bool
	raw  string
	i    int64
	b    bool
	d    time.Duration
	ds   []time.Duration
	json []byte
	zone *time.Location
}

// resolve applies presence and default rules and parses one value strictly (P2.3, vectors
// parse_value). present == false means the variable does not exist. A present value that does not
// parse is CONFIG_INVALID and never falls back to the default; a default that does not parse is too.
func resolve(d Decl, raw string, present bool) (value, *Error) {
	if present && raw == "" && !d.emptyIsValue() {
		present = false
	}
	fromDefault := false
	if !present {
		def, ok := d.defaultText()
		if !ok {
			if d.Required {
				return value{}, newErr(ReasonMissing, d.Name, "required key is not set")
			}
			return value{}, nil
		}
		raw, fromDefault = def, true
	}
	v, err := parseFormat(d, raw)
	if err != nil {
		if fromDefault {
			err.Detail = "default: " + err.Detail
		}
		return value{}, err
	}
	if d.Secret {
		if err := checkSecretPath(d.Name, raw); err != nil {
			return value{}, err
		}
	}
	v.set, v.raw = true, raw
	return v, nil
}

// parseFormat parses raw by d.Format.
func parseFormat(d Decl, raw string) (value, *Error) {
	var v value
	var err error
	switch d.Format {
	case FormatString:
	case FormatInt:
		v.i, err = ParseInt(d.Name, raw)
		if err == nil && d.Minimum != nil && v.i < *d.Minimum {
			err = newErr(ReasonInvalid, d.Name, "below the minimum")
		}
	case FormatBool:
		v.b, err = ParseBool(d.Name, raw)
	case FormatDuration:
		v.d, err = ParseDuration(d.Name, raw)
	case FormatDurations:
		v.ds, err = ParseDurations(d.Name, raw)
	case FormatURL:
		_, err = ParseURL(d.Name, raw, d.Schemes)
	case FormatJSON:
		v.json, err = parseJSONValue(d, raw)
	case FormatEnum:
		_, err = ParseEnum(d.Name, raw, d.Enum)
	case FormatZone:
		v.zone, err = ParseZone(d.Name, raw)
	case FormatLocale:
		_, err = ParseLocale(d.Name, raw)
	default:
		err = newErr(ReasonInvalid, d.Name, "unknown format "+d.Format)
	}
	if err != nil {
		e, _ := AsError(err)
		return value{}, e
	}
	return v, nil
}

// acceptsYAML reports whether a json key also accepts a YAML mapping (P16.9). The catalogue has no flag for
// this yet; see the package report.
func acceptsYAML(key string) bool { return key == "DATA_LIFECYCLE" }

// parseJSONValue parses a json key's value, converting a YAML mapping first where the key allows one.
func parseJSONValue(d Decl, raw string) ([]byte, error) {
	if acceptsYAML(d.Name) && !strings.HasPrefix(strings.TrimSpace(raw), "{") {
		j, err := yamlMappingToJSON(d.Name, raw)
		if err != nil {
			return nil, err
		}
		raw = j
	}
	return ParseJSON(d.Name, raw, d.JSONKind)
}

// checkSecretPath checks that a secret key holds the absolute path of a file, never the value itself
// (P2.7, vectors secret-file-*).
func checkSecretPath(key, path string) *Error {
	if !filepath.IsAbs(path) || strings.HasSuffix(path, "/") || strings.ContainsRune(path, 0) {
		return newErr(ReasonInvalid, key, "a secret key holds the absolute path of a file")
	}
	return nil
}
