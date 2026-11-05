package extcmd

import (
	"encoding/json"
	"io/fs"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type vecCommand struct {
	State          string  `json:"state"`
	ResendDeadline *string `json:"resend_deadline"`
	ExpiresAt      *string `json:"expires_at"`
	FirstReached   *string `json:"first_reached_at"`
	LastReached    *string `json:"last_reached_at"`
	UnknownSince   *string `json:"unknown_since"`
	AcceptedAt     *string `json:"accepted_at"`
	Unrecorded     *string `json:"unrecorded_until"`
}

type vecCaps struct {
	DedupeWindow  float64 `json:"dedupe_window"`
	WindowFrom    string  `json:"window_from"`
	Query         bool    `json:"query"`
	MaxProcessing float64 `json:"max_processing"`
}

func at(t *testing.T, s *string) *time.Time {
	if s == nil {
		return nil
	}
	v, err := time.Parse(time.RFC3339Nano, *s)
	require.NoError(t, err)
	return &v
}

func (c vecCaps) caps() Capabilities {
	return Capabilities{DedupeWindow: time.Duration(c.DedupeWindow) * time.Second, LastUse: c.WindowFrom == "last_use",
		Query: c.Query, MaxProcessing: time.Duration(c.MaxProcessing) * time.Second}
}

func (c vecCommand) facts(t *testing.T) Facts {
	return Facts{State: c.State, ResendDeadline: at(t, c.ResendDeadline), ExpiresAt: at(t, c.ExpiresAt),
		FirstReached: at(t, c.FirstReached), LastReached: at(t, c.LastReached), UnknownSince: at(t, c.UnknownSince),
		AcceptedSince: at(t, c.AcceptedAt), UnrecordedUntil: at(t, c.Unrecorded)}
}

// The state table, the resend deadline and the request's digest against be-protocol's vectors (P13.2-P13.8).
func TestExternalCommandVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/external-command.json")
	require.NoError(t, err)
	var v struct {
		Cases []struct {
			ID    string
			Input struct {
				Command      vecCommand
				Event        map[string]string
				Capabilities vecCaps
				Now          string
				Incoming     map[string]any
			}
			Expected struct {
				State          string
				Reason         string
				ResendDeadline *string `json:"resend_deadline"`
				Digest         string
			}
		}
	}
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	kinds := map[string]string{"claim": Claim, "before_send": BeforeSend, "lease_lost": LeaseLost, "send_result": SendResult,
		"finality_passed": FinalityPassed, "registered": Registered}
	for _, c := range v.Cases {
		t.Run(c.ID, func(t *testing.T) {
			switch {
			case c.Expected.State != "":
				e := Event{Kind: kinds[c.Input.Event["kind"]], Result: c.Input.Event["result"]}
				if c.Input.Event["kind"] == "query" {
					e.Kind = map[string]string{"found": QueryFound, "not_found": QueryNotFound, "unavailable": QueryDown}[c.Input.Event["result"]]
					e.External, e.Result = c.Input.Event["external_state"], ""
				}
				now, err := time.Parse(time.RFC3339Nano, c.Input.Now)
				require.NoError(t, err)
				got, why := Step(c.Input.Command.facts(t), e, c.Input.Capabilities.caps(), now)
				assert.Equal(t, c.Expected.State, got)
				assert.Equal(t, c.Expected.Reason, why)
			case c.Expected.Digest != "":
				d, err := canon.Digest(ParamsRecord, 1, c.Input.Incoming)
				require.NoError(t, err)
				assert.Equal(t, c.Expected.Digest, d)
			default:
				got := Deadline(c.Input.Command.facts(t), c.Input.Capabilities.caps())
				if c.Expected.ResendDeadline == nil {
					assert.Nil(t, got)
				} else if assert.NotNil(t, got) {
					assert.Equal(t, *c.Expected.ResendDeadline, got.UTC().Format("2006-01-02T15:04:05.000000Z"))
				}
			}
		})
	}
}

// The step 7 matching against be-protocol's vectors (P13.12).
func TestExternalReconcileVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/external-reconcile.json")
	require.NoError(t, err)
	type cmd struct {
		vecCommand
		Reference string  `json:"reference"`
		SendingAt *string `json:"sending_at"`
	}
	var v struct {
		Cases []struct {
			ID    string
			Input struct {
				T, Now       string
				FailedAt     *string `json:"failed_at"`
				Commands     []cmd
				External     map[string]string
				Issued       []string
				Capabilities vecCaps
			}
			Expected map[string]struct {
				Conclusion     string
				State          string
				ResendDeadline *string `json:"resend_deadline"`
				Reason         string
			}
		}
	}
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	for _, c := range v.Cases {
		t.Run(c.ID, func(t *testing.T) {
			T, now := *at(t, &c.Input.T), *at(t, &c.Input.Now)
			until := now
			if c.Input.FailedAt != nil {
				until = *at(t, c.Input.FailedAt)
			}
			var open []Open
			for _, o := range c.Input.Commands {
				open = append(open, Open{Reference: o.Reference, Facts: o.facts(t), SendingAt: at(t, o.SendingAt)})
			}
			caps := c.Input.Capabilities.caps()
			got := Match(open, c.Input.External, map[string]bool{}, c.Input.Issued, func(string) Capabilities { return caps },
				T, until, now)
			require.Len(t, got, len(c.Expected))
			for _, g := range got {
				want, ok := c.Expected[g.Reference]
				require.True(t, ok, g.Reference)
				assert.Equal(t, want.Conclusion, g.Conclusion, g.Reference)
				assert.Equal(t, want.State, g.State, g.Reference)
				assert.Equal(t, want.Reason, g.Detail, g.Reference)
				if want.ResendDeadline == nil {
					assert.Nil(t, g.ResendDeadline, g.Reference)
				} else if assert.NotNil(t, g.ResendDeadline, g.Reference) {
					assert.Equal(t, *want.ResendDeadline, g.ResendDeadline.UTC().Format("2006-01-02T15:04:05.000000Z"), g.Reference)
				}
			}
		})
	}
}
