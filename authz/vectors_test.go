package authz

import (
	"encoding/json"
	"io/fs"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vectorCases(t testing.TB, topic string) []json.RawMessage {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/"+topic+".json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	return v.Cases
}

type rawFact struct {
	State string
	Value json.RawMessage
}

type vecRecord map[string]json.RawMessage

type evalFixture struct {
	Resource struct {
		Type          string
		ViewOperation string `json:"view_operation"`
		Dimensions    map[string]struct {
			Kind           string
			Column         string
			CurrencyColumn string `json:"currency_column"`
			PrincipalRole  string `json:"principal_role"`
		}
		Fields map[string]struct{ Class string }
	}
	Records []vecRecord
	At      string
}

func (f evalFixture) resource() *Resource {
	r := &Resource{Type: f.Resource.Type, ViewOperation: f.Resource.ViewOperation, Dimensions: map[string]Dimension{},
		Fields: map[string]Field{}}
	for n, d := range f.Resource.Dimensions {
		r.Dimensions[n] = Dimension{Kind: d.Kind, Column: d.Column, CurrencyColumn: d.CurrencyColumn, PrincipalRole: d.PrincipalRole}
	}
	for n, fd := range f.Resource.Fields {
		r.Fields[n] = Field{Class: fd.Class, ReadKey: "k." + n}
	}
	return r
}

// facts turns a vector record into facts by dimension.
func (f evalFixture) facts(r *Resource, rec vecRecord) Facts {
	out := Facts{}
	for name, d := range r.Dimensions {
		var rf rawFact
		key := d.Column
		if d.Kind == Amount {
			key = "amount"
		}
		_ = json.Unmarshal(rec[key], &rf)
		switch rf.State {
		case "value":
			if d.Kind == Amount {
				var m struct{ Amount, Currency string }
				_ = json.Unmarshal(rf.Value, &m)
				out[name] = Money(m.Amount, m.Currency)
			} else {
				var s string
				_ = json.Unmarshal(rf.Value, &s)
				out[name] = Value(s)
			}
		case "null":
			out[name] = Null
		default:
			out[name] = Unobtained
		}
	}
	return out
}

func idOf(rec vecRecord) string { var s string; _ = json.Unmarshal(rec["id"], &s); return s }

func triName(t Tri) string {
	return map[Tri]string{True: "true", False: "false", Unknown: "unknown"}[t]
}

type evalCase struct {
	ID, Kind, Principal, Action string
	Segments                    []Segment
	Filter                      map[string]string
	Expect                      json.RawMessage
}

func loadEval(t testing.TB) (evalFixture, []evalCase) {
	cs := vectorCases(t, "segment-eval")
	var f evalFixture
	require.NoError(t, json.Unmarshal(cs[0], &f))
	var out []evalCase
	for _, raw := range cs[1:] {
		var c evalCase
		require.NoError(t, json.Unmarshal(raw, &c))
		out = append(out, c)
	}
	return f, out
}

func TestSegmentEvalVectors(t *testing.T) {
	f, cases := loadEval(t)
	r := f.resource()
	at, err := time.Parse(time.RFC3339Nano, f.At)
	require.NoError(t, err)
	for _, c := range cases {
		if c.Kind != "evaluate" {
			continue
		}
		var want struct {
			Records map[string]struct {
				Visible, Action string
				Fields          []string
				ReadError       *string `json:"read_error"`
				ActionError     *string `json:"action_error"`
			}
		}
		require.NoError(t, json.Unmarshal(c.Expect, &want))
		for _, s := range c.Segments {
			require.NoError(t, s.Check(r), c.ID)
		}
		for _, rec := range f.Records {
			id := idOf(rec)
			d := Evaluate(r, c.Segments, c.Action, f.facts(r, rec), c.Principal, at)
			w := want.Records[id]
			assert.Equal(t, w.Visible, triName(d.Visible), "%s %s visible", c.ID, id)
			assert.Equal(t, w.Action, triName(d.Action), "%s %s action", c.ID, id)
			if w.Fields == nil {
				w.Fields = []string{}
			}
			if d.Fields == nil {
				d.Fields = []string{}
			}
			assert.Equal(t, w.Fields, d.Fields, "%s %s fields", c.ID, id)
			read := Evaluate(r, c.Segments, r.ViewOperation, f.facts(r, rec), c.Principal, at)
			assert.Equal(t, w.ReadError, reasonOf(read.Err()), "%s %s read error", c.ID, id)
			assert.Equal(t, w.ActionError, reasonOf(d.Err()), "%s %s action error", c.ID, id)
		}
	}
}

func TestSegmentCanonicalVectors(t *testing.T) {
	for _, raw := range vectorCases(t, "canonical") {
		var c struct {
			ID, Kind string
			In       json.RawMessage
			Expect   struct{ Canonical, Digest string }
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		if c.Kind != "segment" {
			continue
		}
		var seg Segment
		require.NoError(t, json.Unmarshal(c.In, &seg))
		got, err := seg.Digest()
		require.NoError(t, err, c.ID)
		assert.Equal(t, c.Expect.Digest, got, c.ID)
	}
}

func reasonOf(err error) *string {
	if err == nil {
		return nil
	}
	r := err.(*errs.Error).Reason
	return &r
}
