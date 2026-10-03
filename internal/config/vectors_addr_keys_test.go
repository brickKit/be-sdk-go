package config

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/vectors"
)

func decodeInput(t *testing.T, c vectors.Case, into any) {
	t.Helper()
	if err := json.Unmarshal(c.Input, into); err != nil {
		t.Fatal(err)
	}
}

func TestVectorsEndpoints(t *testing.T) {
	vectors.Run(t, "config", "endpoints", map[string]func(*testing.T, vectors.Case){
		"endpoint_name": func(t *testing.T, c vectors.Case) {
			var in struct{ Dependency, Port string }
			decodeInput(t, c, &in)
			name, err := EndpointName(in.Dependency, in.Port)
			if err != nil {
				vectors.RequireReason(t, c, reasonOf(t, err))
				return
			}
			vectors.RequireJSON(t, c, map[string]any{"name": name})
		},
		"endpoint_value": func(t *testing.T, c vectors.Case) {
			var in struct{ Value *string }
			decodeInput(t, c, &in)
			addr, ok, err := Endpoint(deref(in.Value), in.Value != nil)
			if err != nil {
				vectors.RequireReason(t, c, reasonOf(t, err))
				return
			}
			if !ok {
				vectors.RequireJSON(t, c, map[string]any{"present": false})
				return
			}
			vectors.RequireJSON(t, c, map[string]any{"present": true, "address": addr})
		},
		"family_address": func(t *testing.T, c vectors.Case) {
			var in struct {
				Key   string
				Value *string
			}
			decodeInput(t, c, &in)
			addr, ok, err := FamilyAddress(in.Key, deref(in.Value), in.Value != nil)
			if err != nil {
				vectors.RequireReason(t, c, reasonOf(t, err))
				return
			}
			if !ok {
				vectors.RequireJSON(t, c, map[string]any{"present": false})
				return
			}
			field := "base"
			if strings.HasSuffix(in.Key, "_GRPC_URL") {
				field = "target"
			}
			vectors.RequireJSON(t, c, map[string]any{"present": true, field: addr})
		},
	})
}

func TestVectorsKeys(t *testing.T) {
	vectors.Run(t, "config", "keys", map[string]func(*testing.T, vectors.Case){
		"key_name": func(t *testing.T, c vectors.Case) {
			var in struct{ Key string }
			decodeInput(t, c, &in)
			if err := CheckKeyName(in.Key); err != nil {
				vectors.RequireReason(t, c, reasonOf(t, err))
				return
			}
			vectors.RequireJSON(t, c, map[string]any{"valid": true})
		},
		"key_declaration": func(t *testing.T, c vectors.Case) {
			var in KeyDeclaration
			decodeInput(t, c, &in)
			if err := CheckKeyDeclaration(in); err != nil {
				vectors.RequireReason(t, c, reasonOf(t, err))
				return
			}
			vectors.RequireJSON(t, c, map[string]any{"valid": true})
		},
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
