package calls

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

func cases(t *testing.T, topic string) []json.RawMessage {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/"+topic+".json")
	require.NoError(t, err)
	var v struct{ Cases []json.RawMessage }
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	return v.Cases
}

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestOutboundTimeoutVectors(t *testing.T) {
	for _, raw := range cases(t, "outbound-timeout") {
		var c struct {
			ID        string
			Remaining *int `json:"remaining_ms"`
			Expect    struct {
				Send    bool
				Timeout int `json:"timeout_ms"`
				Reason  string
			}
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		var rem time.Duration
		if c.Remaining != nil {
			rem = ms(*c.Remaining)
		}
		timeout, send := AttemptTimeout(rem, c.Remaining != nil)
		assert.Equal(t, c.Expect.Send, send, c.ID)
		if send {
			assert.Equal(t, ms(c.Expect.Timeout), timeout, c.ID)
		} else {
			assert.Equal(t, errs.ReasonDeadlineBudgetExhausted, c.Expect.Reason, c.ID)
		}
	}
}

func TestRetryDecisionVectors(t *testing.T) {
	for _, raw := range cases(t, "retry-decision") {
		var c struct {
			ID, Kind, Outcome, Expect string
			Idempotent                bool
			AttemptsMade              int  `json:"attempts_made"`
			BudgetAllows              bool `json:"budget_allows"`
			After                     *int `json:"remaining_ms_after_backoff"`
			Failed                    int  `json:"failed_attempts"`
			MaxBackoff                int  `json:"max_backoff_ms"`
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		if c.Kind == "backoff" {
			assert.Equal(t, ms(c.MaxBackoff), MaxBackoff(c.Failed), c.ID)
			continue
		}
		var after time.Duration
		if c.After != nil {
			after = ms(*c.After)
		}
		got := "stop"
		if ShouldRetry(c.Idempotent, c.Outcome, c.AttemptsMade, c.BudgetAllows, after, c.After != nil) {
			got = "retry"
		}
		assert.Equal(t, c.Expect, got, c.ID)
	}
}

func TestRetryBudgetVectors(t *testing.T) {
	for _, raw := range cases(t, "retry-budget") {
		var c struct {
			ID      string
			Events  []string
			Tokens  []int  `json:"expect_tokens"`
			Allowed []bool `json:"expect_retry_allowed"`
			Final   bool   `json:"expect_final_retry_allowed"`
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		b := NewBudget()
		for i, e := range c.Events {
			b.Record(e)
			assert.Equal(t, c.Tokens[i], b.Tokens(), "%s step %d", c.ID, i)
			assert.Equal(t, c.Allowed[i], b.AllowsRetry(), "%s step %d", c.ID, i)
		}
		assert.Equal(t, c.Final, b.AllowsRetry(), c.ID)
	}
}

func TestCircuitBreakerVectors(t *testing.T) {
	for _, raw := range cases(t, "circuit-breaker") {
		var c struct {
			ID    string
			Steps []struct {
				At                          int `json:"at_ms"`
				Kind, Call, Outcome, Expect string
				ExpectState                 string `json:"expect_state"`
			}
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		b := NewBreaker()
		base := time.Now()
		permits := map[string]Permit{}
		for i, s := range c.Steps {
			at := base.Add(ms(s.At))
			if s.Kind == "start" {
				p, ok := b.Start(at)
				got := "rejected"
				if ok {
					got, permits[s.Call] = "sent", p
				}
				assert.Equal(t, s.Expect, got, "%s step %d", c.ID, i)
			} else {
				b.End(permits[s.Call], at, s.Outcome)
			}
			assert.Equal(t, s.ExpectState, b.State(), "%s step %d", c.ID, i)
		}
	}
}

func TestCallOutcomesVectors(t *testing.T) {
	type pair struct{ Domain, Reason string }
	for _, raw := range cases(t, "call-outcomes") {
		var c struct {
			ID, Surface, Code, Dependency string
			Status                        *int
			Info, Problem                 *pair
			Idempotent                    bool
			ConnectFailed                 bool `json:"connect_failed"`
			CallerCancelled               bool `json:"caller_cancelled"`
			Expect                        struct {
				Class, Breaker, Budget string
				RetryOutcome           string          `json:"retry_outcome"`
				Upward                 json.RawMessage `json:"upward"`
			}
		}
		require.NoError(t, json.Unmarshal(raw, &c))
		r := Result{ConnectFailed: c.ConnectFailed}
		remote := c.Info
		if c.Surface == "grpc" {
			r.Code = errs.Code(c.Code)
		} else {
			remote = c.Problem
			status := 0
			if c.Status != nil {
				status = *c.Status
			}
			r.Code = errs.CodeOfStatus(status)
			if c.Problem != nil && c.Problem.Domain == errs.PlatformDomain {
				entry, ok := errs.Platform(c.Problem.Reason)
				require.True(t, ok, c.ID)
				r.Code = entry.Code
			}
		}
		if remote != nil {
			r.Remote = &errs.Error{Domain: remote.Domain, Reason: remote.Reason, Code: r.Code}
		}
		if r.Code != "OK" || c.ConnectFailed {
			r.Err = assert.AnError
		}
		class := Classify(r, c.CallerCancelled)
		assert.Equal(t, c.Expect.Class, string(class), c.ID)
		assert.Equal(t, c.Expect.Breaker, class.BreakerOutcome(), c.ID)
		assert.Equal(t, c.Expect.Budget, class.BudgetOutcome(), c.ID)
		assert.Equal(t, c.Expect.RetryOutcome, class.RetryOutcome(), c.ID)

		up := Upward(class, r, c.Idempotent, c.Dependency)
		switch string(c.Expect.Upward) {
		case "null":
			assert.NoError(t, up, c.ID)
		case `"unchanged"`:
			assert.Same(t, r.Remote, up, c.ID)
		default:
			var want struct {
				Domain, Reason string
				Metadata       map[string]string
			}
			require.NoError(t, json.Unmarshal(c.Expect.Upward, &want))
			e := errs.From(up)
			assert.Equal(t, want.Domain, e.Domain, c.ID)
			assert.Equal(t, want.Reason, e.Reason, c.ID)
			for k, v := range want.Metadata {
				assert.Equal(t, v, e.Metadata[k], c.ID)
			}
		}
	}
}
