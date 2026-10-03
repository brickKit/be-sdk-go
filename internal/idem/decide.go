package idem

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// The two errors of the decision (P13.2, P13.3). They are shared values with empty metadata, so
// errors.Is works against them; never modify them.
var (
	// ErrMismatch: same caller and key, another command, target or request hash (400 / INVALID_ARGUMENT).
	ErrMismatch = problem.Be("IDEMPOTENCY_MISMATCH", nil)
	// ErrInProgress: the first use is still CLAIMED (409 / ABORTED).
	ErrInProgress = problem.Be("IDEMPOTENCY_IN_PROGRESS", nil)
)

// Status is a row's state (besdk_idempotency.status).
type Status string

// The two states of a key.
const (
	StatusClaimed Status = "CLAIMED"
	StatusDone    Status = "DONE"
)

// Command is one keyed command, fully resolved by the root: the caller's namespace, the key, the
// command name (permission key or full rpc name), the target aggregate ID ("" for a create) and the
// request hash (Fingerprint / FingerprintValue). Status is the HTTP status a successful first
// execution answers with, stored for replay; 0 means 200.
type Command struct {
	Caller string
	Key    string
	Name   string
	Target string
	Hash   []byte
	Status int
}

// Result is the stored outcome of a completed key, the JSONB column result (P13.3): the HTTP status
// and the response body. A gRPC command stores HTTP 200 (codes.OK's HTTP mapping, P4.2) and the
// protojson form of its response message.
type Result struct {
	HTTP int             `json:"http"`
	Body json.RawMessage `json:"body"`
}

// Row is one besdk_idempotency row.
type Row struct {
	Caller    string
	Key       string
	Command   string
	Target    string
	Hash      []byte
	Status    Status
	Result    *Result // set once DONE
	CreatedAt time.Time
	ExpiresAt time.Time
}

// Outcome is what to do with an incoming command.
type Outcome int

// The two non-error outcomes.
const (
	Execute Outcome = iota // claim and run
	Replay                 // answer with the stored Result, do not run
)

// Decision is Decide's answer; Result is set for Replay.
type Decision struct {
	Outcome Outcome
	Result  Result
}

// Find returns the row of (caller, key) among rows, or nil. Rows of other namespaces are invisible
// (P13.4).
func Find(rows []Row, caller, key string) *Row {
	for i := range rows {
		if rows[i].Caller == caller && rows[i].Key == key {
			return &rows[i]
		}
	}
	return nil
}

// Decide applies the decision order of P13 (vectors/idempotency README) to the row of the command's
// (caller, key):
//
//	no row, or now >= expires_at (half-open)   → Execute
//	command, target or request hash differs    → ErrMismatch (before the state, P13.2)
//	CLAIMED                                    → ErrInProgress (P13.3)
//	DONE                                       → Replay with the stored result
func Decide(row *Row, now time.Time, c Command) (Decision, error) {
	if row == nil || row.Caller != c.Caller || row.Key != c.Key || !now.Before(row.ExpiresAt) {
		return Decision{Outcome: Execute}, nil
	}
	if row.Command != c.Name || row.Target != c.Target || !bytes.Equal(row.Hash, c.Hash) {
		return Decision{}, ErrMismatch
	}
	if row.Status == StatusClaimed {
		return Decision{}, ErrInProgress
	}
	if row.Status != StatusDone || row.Result == nil {
		return Decision{}, fmt.Errorf("idem: row %s/%s in state %q has no result", row.Caller, row.Key, row.Status)
	}
	return Decision{Outcome: Replay, Result: *row.Result}, nil
}
