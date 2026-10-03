package idem

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/vectors"
	"github.com/stretchr/testify/require"
)

type vecRow struct {
	Caller         string          `json:"caller"`
	IdempotencyKey string          `json:"idempotency_key"`
	Command        string          `json:"command"`
	Target         string          `json:"target"`
	RequestHash    string          `json:"request_hash"`
	Status         string          `json:"status"`
	Result         json.RawMessage `json:"result"`
	CreatedAt      time.Time       `json:"created_at"`
	ExpiresAt      time.Time       `json:"expires_at"`
}

func (v vecRow) row(t *testing.T) Row {
	h, err := hex.DecodeString(v.RequestHash)
	require.NoError(t, err)
	r := Row{Caller: v.Caller, Key: v.IdempotencyKey, Command: v.Command, Target: v.Target, Hash: h,
		Status: Status(v.Status), CreatedAt: v.CreatedAt, ExpiresAt: v.ExpiresAt}
	if len(v.Result) > 0 && string(v.Result) != "null" {
		var res Result
		require.NoError(t, json.Unmarshal(v.Result, &res))
		r.Result = &res
	}
	return r
}

func TestDecideVectors(t *testing.T) {
	vectors.Run(t, "idempotency", "decide", map[string]func(*testing.T, vectors.Case){
		"decide": func(t *testing.T, c vectors.Case) {
			var in struct {
				Rows     []vecRow  `json:"rows"`
				Now      time.Time `json:"now"`
				Incoming struct {
					Caller  vecCaller `json:"caller"`
					Key     string    `json:"key"`
					Command string    `json:"command"`
					Target  string    `json:"target"`
					Request string    `json:"request"`
				} `json:"incoming"`
			}
			require.NoError(t, json.Unmarshal(c.Input, &in))
			ns, err := in.Incoming.Caller.caller().Namespace()
			require.NoError(t, err)
			hash, err := Fingerprint([]byte(in.Incoming.Request))
			require.NoError(t, err)
			rows := make([]Row, len(in.Rows))
			for i, r := range in.Rows {
				rows[i] = r.row(t)
			}
			cmd := Command{Caller: ns, Key: in.Incoming.Key, Name: in.Incoming.Command,
				Target: in.Incoming.Target, Hash: hash}
			d, err := Decide(Find(rows, ns, cmd.Key), in.Now, cmd)
			got := map[string]any{"caller": ns}
			var pe *problem.Error
			switch {
			case errors.As(err, &pe):
				got["outcome"] = "REJECT"
				got["code"] = problem.CodeName(pe.Code)
				got["http"] = pe.HTTPStatus()
				got["reason"] = pe.Reason
			case err != nil:
				t.Fatalf("unexpected error %v", err)
			case d.Outcome == Execute:
				got["outcome"] = "EXECUTE"
			default:
				got["outcome"] = "REPLAY"
				got["result"] = d.Result
			}
			vectors.RequireJSON(t, c, got)
		},
	})
}

// The errors are the package's sentinels, so errors.Is works for the root's exported values.
func TestDecideReturnsSentinels(t *testing.T) {
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	row := &Row{Caller: "system", Key: "k", Command: "c", Hash: []byte{1}, Status: StatusClaimed,
		CreatedAt: now, ExpiresAt: ExpiresAt(now)}
	_, err := Decide(row, now, Command{Caller: "system", Key: "k", Name: "c", Hash: []byte{1}})
	require.ErrorIs(t, err, ErrInProgress)
	_, err = Decide(row, now, Command{Caller: "system", Key: "k", Name: "c", Hash: []byte{2}})
	require.ErrorIs(t, err, ErrMismatch)
}

// A DONE row without a stored result is corrupt: never replay an empty answer.
func TestDecideDoneWithoutResult(t *testing.T) {
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	row := &Row{Caller: "system", Key: "k", Command: "c", Hash: []byte{1}, Status: StatusDone,
		CreatedAt: now, ExpiresAt: ExpiresAt(now)}
	_, err := Decide(row, now, Command{Caller: "system", Key: "k", Name: "c", Hash: []byte{1}})
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrMismatch)
}
