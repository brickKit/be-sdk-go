package httpx

import (
	"fmt"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"
)

// OpenAPIProblems compares a component's registered routes with its OpenAPI document (contract/openapi.yaml): every
// route must be a path and method there with the same x-operation (empty for a Public route), and every operation
// there must be registered. A component's test asserts it is empty, so the document never drifts from the code.
func OpenAPIProblems(document []byte, routes []Route) []string {
	var doc struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(document, &doc); err != nil {
		return []string{"openapi: " + err.Error()}
	}
	documented := map[string]string{} // "METHOD /path" -> x-operation
	for path, item := range doc.Paths {
		for method, op := range item {
			m := strings.ToUpper(method)
			switch m {
			case "GET", "PUT", "POST", "PATCH", "DELETE", "HEAD", "OPTIONS":
			default:
				continue // parameters, summary, ...
			}
			x := ""
			if o, ok := op.(map[string]any); ok {
				x = fmt.Sprint(o["x-operation"])
				if o["x-operation"] == nil {
					x = ""
				}
			}
			documented[m+" "+path] = x
		}
	}
	var out []string
	registered := map[string]bool{}
	for _, r := range routes {
		key := r.Method + " " + Template(r.Path)
		registered[key] = true
		x, ok := documented[key]
		switch {
		case !ok:
			out = append(out, key+": registered but not in the OpenAPI document")
		case x != r.Operation:
			out = append(out, fmt.Sprintf("%s: x-operation %q in the document, %q registered", key, x, r.Operation))
		}
	}
	for key := range documented {
		if !registered[key] {
			out = append(out, key+": in the OpenAPI document but not registered")
		}
	}
	sort.Strings(out)
	return out
}
