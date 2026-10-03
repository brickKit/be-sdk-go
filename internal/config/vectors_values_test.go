package config

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/vectors"
)

// parseValueInput is the input of a vectors config/values.json parse_value case.
type parseValueInput struct {
	Type     string   `json:"type"`
	Value    *string  `json:"value"`
	Default  *string  `json:"default"`
	Required bool     `json:"required"`
	Secret   bool     `json:"secret"`
	Minimum  *int64   `json:"minimum"`
	Schemes  []string `json:"schemes"`
	JSONKind string   `json:"json_kind"`
	Enum     []string `json:"enum"`
}

// vectorFormat maps a vector type name to the protocol format.
func vectorFormat(t *testing.T, typ string) string {
	t.Helper()
	switch typ {
	case "string":
		return FormatString
	case "integer":
		return FormatInt
	case "boolean":
		return FormatBool
	case "duration":
		return FormatDuration
	case "duration_list":
		return FormatDurations
	case "url":
		return FormatURL
	case "json":
		return FormatJSON
	case "enum":
		return FormatEnum
	}
	t.Fatalf("unknown vector type %q", typ)
	return ""
}

// renderValue renders a resolved value in the shape vectors config/README.md gives.
func renderValue(d Decl, v value) map[string]any {
	if !v.set {
		return map[string]any{"set": false}
	}
	if d.Secret {
		return map[string]any{"set": true, "source": "file", "path": v.raw}
	}
	out := map[string]any{"set": true}
	switch d.Format {
	case FormatInt:
		out["value"] = v.i
	case FormatBool:
		out["value"] = v.b
	case FormatDuration:
		out["value"] = strconv.FormatInt(int64(v.d), 10)
	case FormatDurations:
		l := make([]string, len(v.ds))
		for i, x := range v.ds {
			l[i] = strconv.FormatInt(int64(x), 10)
		}
		out["value"] = l
	case FormatJSON:
		out["value"] = json.RawMessage(v.json)
	default:
		out["value"] = v.raw
	}
	return out
}

func runParseValue(t *testing.T, c vectors.Case) {
	var in parseValueInput
	if err := json.Unmarshal(c.Input, &in); err != nil {
		t.Fatal(err)
	}
	d := Decl{
		Name: "VECTOR_KEY", Format: vectorFormat(t, in.Type), Required: in.Required, Default: in.Default,
		Secret: in.Secret, Minimum: in.Minimum, Schemes: in.Schemes, JSONKind: in.JSONKind, Enum: in.Enum,
	}
	raw, present := "", in.Value != nil
	if present {
		raw = *in.Value
	}
	v, err := resolve(d, raw, present)
	if err != nil {
		vectors.RequireReason(t, c, err.Reason)
		return
	}
	vectors.RequireJSON(t, c, renderValue(d, v))
}

func runReadUndeclared(t *testing.T, c vectors.Case) {
	var in struct {
		Key      string   `json:"key"`
		Declared []string `json:"declared"`
	}
	if err := json.Unmarshal(c.Input, &in); err != nil {
		t.Fatal(err)
	}
	var decls []Decl
	for _, k := range in.Declared {
		decls = append(decls, Decl{Name: k, Type: "string", Format: FormatString})
	}
	vals, errs := Load(NewSchema("conformance/widget", "1.0.0", decls), func(string) (string, bool) { return "x", true })
	if len(errs) > 0 {
		t.Fatalf("load: %v", errs)
	}
	reason := ""
	func() {
		defer func() {
			if e, ok := AsError(recover()); ok {
				reason = e.Reason
			}
		}()
		vals.String(in.Key)
	}()
	if reason != "" {
		vectors.RequireReason(t, c, reason)
		return
	}
	vectors.RequireJSON(t, c, map[string]any{"allowed": true})
}

func runSecretText(t *testing.T, c vectors.Case) {
	var in struct {
		Content  string `json:"content"`
		Required bool   `json:"required"`
	}
	if err := json.Unmarshal(c.Input, &in); err != nil {
		t.Fatal(err)
	}
	text, set, err := SecretText("VECTOR_KEY_FILE", []byte(in.Content), in.Required)
	if err != nil {
		vectors.RequireReason(t, c, reasonOf(t, err))
		return
	}
	if !set {
		vectors.RequireJSON(t, c, map[string]any{"set": false})
		return
	}
	vectors.RequireJSON(t, c, map[string]any{"set": true, "value": text})
}

// reasonOf extracts the vector error class from an error returned by this package.
func reasonOf(t *testing.T, err error) string {
	t.Helper()
	e, ok := AsError(err)
	if !ok {
		t.Fatalf("error %v is not a *config.Error", err)
	}
	return e.Reason
}

func TestVectorsValues(t *testing.T) {
	vectors.Run(t, "config", "values", map[string]func(*testing.T, vectors.Case){
		"parse_value":     runParseValue,
		"read_undeclared": runReadUndeclared,
		"secret_text":     runSecretText,
	})
}
