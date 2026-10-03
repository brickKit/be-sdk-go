package logx

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/vectors"
)

// decodeNumbers decodes JSON keeping numbers as json.Number, as the handler does.
func decodeNumbers(t *testing.T, raw []byte, into any) {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(into); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestRedactionVectors(t *testing.T) {
	vectors.Run(t, "redaction", "redact", map[string]func(*testing.T, vectors.Case){
		"redact": func(t *testing.T, c vectors.Case) {
			var in struct {
				Record map[string]any `json:"record"`
			}
			decodeNumbers(t, c.Input, &in)
			vectors.RequireJSON(t, c, map[string]any{"record": Redact(in.Record)})
		},
		"protected_key": func(t *testing.T, c vectors.Case) {
			var in struct {
				Key string `json:"key"`
			}
			decodeNumbers(t, c.Input, &in)
			vectors.RequireJSON(t, c, map[string]any{"protected": ProtectedKey(in.Key)})
		},
	})
}

func TestRedactDoesNotMutateInput(t *testing.T) {
	in := map[string]any{"user": map[string]any{"phone": "1"}}
	_ = Redact(in)
	if got := in["user"].(map[string]any)["phone"]; got != "1" {
		t.Fatalf("input mutated: %v", got)
	}
}

func TestProtectedKeyWordSplitting(t *testing.T) {
	cases := map[string]bool{
		"ABCPhoneNumber": true,  // ABC_Phone_Number
		"setCookies":     true,  // plural on the last word of a two-word name
		"set_cookie_x":   true,  //
		"key_api":        false, // words out of order
		"apiKeys":        true,  //
		"bank_cards":     true,  //
		"bankscard":      false, // one word
		"":               false, //
		"__":             false, //
		"id__card":       true,  // empty words dropped
		"phone2":         false, // digits stay in the word
		"v2Phone":        true,  // v2_Phone
	}
	for k, want := range cases {
		if got := ProtectedKey(k); got != want {
			t.Errorf("ProtectedKey(%q) = %v, want %v", k, got, want)
		}
	}
}
