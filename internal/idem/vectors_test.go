package idem

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"testing"
	"time"

	beprotocol "github.com/brickKit/be-protocol"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/vectors"
	"github.com/stretchr/testify/require"
)

// reasonOf is the vector error class of err: a *problem.Invalid's reason or a *problem.Error's reason.
func reasonOf(t *testing.T, err error) string {
	t.Helper()
	var inv *problem.Invalid
	if errors.As(err, &inv) {
		return inv.Reason
	}
	var pe *problem.Error
	if errors.As(err, &pe) {
		return pe.Reason
	}
	t.Fatalf("error %v is neither a vector class nor a protocol error", err)
	return ""
}

func TestFingerprintVectors(t *testing.T) {
	vectors.Run(t, "idempotency", "fingerprint", map[string]func(*testing.T, vectors.Case){
		"fingerprint": func(t *testing.T, c vectors.Case) {
			var in struct {
				JSONText string `json:"json_text"`
			}
			require.NoError(t, json.Unmarshal(c.Input, &in))
			canon, err := Canonicalize([]byte(in.JSONText))
			if c.ExpectedError != nil {
				require.Error(t, err)
				vectors.RequireReason(t, c, reasonOf(t, err))
				return
			}
			require.NoError(t, err)
			var want struct {
				Canonical string `json:"canonical"`
				SHA256    string `json:"sha256"`
			}
			require.NoError(t, json.Unmarshal(c.Expected, &want))
			require.Equal(t, want.Canonical, string(canon), c.Description)
			sum, err := Fingerprint([]byte(in.JSONText))
			require.NoError(t, err)
			require.Equal(t, want.SHA256, hex.EncodeToString(sum))
		},
	})
}

// vecCaller is the vectors' caller object.
type vecCaller struct {
	Kind     string `json:"kind"`
	Sub      string `json:"sub"`
	BeCaller string `json:"be_caller"`
}

func (v vecCaller) caller() Caller {
	return Caller{Kind: CallerKind(v.Kind), Sub: v.Sub, BeCaller: v.BeCaller}
}

func TestKeysVectors(t *testing.T) {
	vectors.Run(t, "idempotency", "keys", map[string]func(*testing.T, vectors.Case){
		"resolve_key": func(t *testing.T, c vectors.Case) {
			var in struct{ Header, Body *string }
			require.NoError(t, json.Unmarshal(c.Input, &in))
			key, err := ResolveKey(deref(in.Header), deref(in.Body))
			if c.ExpectedError != nil {
				require.Error(t, err)
				vectors.RequireReason(t, c, reasonOf(t, err))
				requireCodeHTTP(t, c, err)
				return
			}
			require.NoError(t, err)
			var got any
			if key != "" {
				got = key
			}
			vectors.RequireJSON(t, c, map[string]any{"key": got})
		},
		"caller_namespace": func(t *testing.T, c vectors.Case) {
			var in struct{ Caller vecCaller }
			require.NoError(t, json.Unmarshal(c.Input, &in))
			ns, err := in.Caller.caller().Namespace()
			require.NoError(t, err)
			vectors.RequireJSON(t, c, map[string]any{"caller": ns})
		},
		"expires_at": func(t *testing.T, c vectors.Case) {
			var in struct {
				CreatedAt time.Time `json:"created_at"`
			}
			require.NoError(t, json.Unmarshal(c.Input, &in))
			var want struct {
				ExpiresAt time.Time `json:"expires_at"`
			}
			require.NoError(t, json.Unmarshal(c.Expected, &want))
			require.True(t, want.ExpiresAt.Equal(ExpiresAt(in.CreatedAt)), "got %s", ExpiresAt(in.CreatedAt))
		},
	})
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// requireCodeHTTP checks the case's expected code and http (read from the raw case) against err.
func requireCodeHTTP(t *testing.T, c vectors.Case, err error) {
	t.Helper()
	var want struct {
		Code string `json:"code"`
		HTTP int    `json:"http"`
	}
	require.NoError(t, json.Unmarshal(rawExpectedError(t, "keys", c.ID), &want))
	var pe *problem.Error
	require.True(t, errors.As(err, &pe), "want a *problem.Error, got %v", err)
	require.Equal(t, want.Code, problem.CodeName(pe.Code))
	require.Equal(t, want.HTTP, pe.HTTPStatus())
}

// rawExpectedError reads a case's whole expected_error object (vectors.Case keeps only the reason).
func rawExpectedError(t *testing.T, topic, id string) json.RawMessage {
	t.Helper()
	b, err := fs.ReadFile(beprotocol.FS, "vectors/idempotency/"+topic+".json")
	require.NoError(t, err)
	var f struct {
		Cases []struct {
			ID            string          `json:"id"`
			ExpectedError json.RawMessage `json:"expected_error"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(b, &f))
	for _, c := range f.Cases {
		if c.ID == id {
			return c.ExpectedError
		}
	}
	t.Fatalf("case %s not found", id)
	return nil
}
