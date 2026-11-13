package extcmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/jackc/pgx/v5"
)

// Parameters of the sending (the assembly project's protocol 10).
const (
	BackoffStart = time.Minute
	BackoffCap   = time.Hour
	// SendLease is how long a claimed command stays sending before another run takes it as its claimant gone.
	SendLease = 2 * time.Minute
	// SendTimeout bounds one call to the external system (the adapter's read timeout lies within it).
	SendTimeout = 30 * time.Second
	// ListTimeout bounds the listing of the external records in a full recovery's reconciliation (P13.11).
	ListTimeout = 2 * time.Minute
	// RunEvery is how often the job looks for due commands; Batch how many it takes a run.
	RunEvery = 2 * time.Second
	Batch    = 50
	// IssuedCategory is the evidence category of an irrevocable command's issue record (P13.4).
	IssuedCategory = "external_command_issued"
	// JobName is the SDK's job that sends and queries the commands.
	JobName = "be.extcmd.process"
)

// QueryOnlyKey is the deployment's query-only switch (P13.10).
const QueryOnlyKey = "EXTERNAL_COMMANDS_QUERY_ONLY"

// Keys are the configuration keys of a member that sends external commands.
func Keys() []config.Key {
	return []config.Key{{Name: QueryOnlyKey, Type: config.Boolean, Default: false}} // query, never send (P13.10)
}

// Command is a command as the adapter sends and queries it.
type Command struct {
	ID, Requester, Reference, Operation string
	Params                              map[string]any
	ExpiresAt                           *time.Time
	AbandonAt                           *time.Time // P13.14: the local abandon time, nil when the command has none
	ExternalID                          string
}

// Result is a send's outcome as the adapter classifies it (P13.5): Class is not_reached, maybe_reached, accepted,
// succeeded, failed or rejected.
type Result struct {
	Class      string
	ExternalID string
	Outcome    map[string]any // the external system's report (amounts, status), kept on a final state
	Response   string         // a digest or a short summary of the external response, never a secret
}

// Answer is a query's outcome: the external system has a record of the reference, in State, or has none.
type Answer struct {
	Found      bool
	State      string // accepted, succeeded, failed, rejected, or anything not final
	ExternalID string
	Outcome    map[string]any
	Response   string
}

// Adapter talks to the external system.
type Adapter interface {
	// Send makes the request. An error is a result the adapter could not classify: it counts as maybe_reached.
	Send(ctx context.Context, c Command) (Result, error)
	// Query asks the external system by the business reference. An error is "the query is unavailable".
	Query(ctx context.Context, c Command) (Answer, error)
}

// Provider is a module that sends external commands: its adapter and each operation's capabilities (P13.9).
type Provider interface {
	ExternalCommands() (Adapter, map[string]Capabilities)
}

// Commands is a member's external commands.
type Commands struct {
	DB        *db.DB
	Component string
	Adapter   Adapter
	Ops       map[string]Capabilities
	Evidence  *evidence.Writer // the issue records of irrevocable commands; nil: irrevocable ones are never sent
	QueryOnly bool             // EXTERNAL_COMMANDS_QUERY_ONLY (P13.10): query, never send
	Log       *slog.Logger
	// OnSettled runs in the transaction that takes a command to a final state (succeeded, failed, rejected): the
	// adapter's result fact goes out with it (protocol 10's business facts, once per command). Nil: nothing.
	OnSettled func(ctx context.Context, tx db.Tx, cmd Command, state, why string) error
}

func (c *Commands) log() *slog.Logger {
	if c.Log != nil {
		return c.Log
	}
	return slog.New(slog.DiscardHandler)
}

// Request writes a command in the caller's transaction (P13.2), idempotent by the requesting component and its
// business reference: the same reference with the same parameters returns the command already there; with other
// parameters it answers IDEMPOTENCY_KEY_REUSED and writes nothing. The job sends it after the commit.
func (c *Commands) Request(ctx context.Context, tx db.Tx, requester, reference, operation string, params map[string]any,
	expiresAt *time.Time) (Command, string, error) {
	return c.RequestUntil(ctx, tx, requester, reference, operation, params, expiresAt, nil)
}

// RequestUntil is Request with a local abandon time (P13.14): past it the command is not sent, queried or awaited
// any more and fails as expired. For commands that do not matter late (notifications); never an irrevocable one.
func (c *Commands) RequestUntil(ctx context.Context, tx db.Tx, requester, reference, operation string,
	params map[string]any, expiresAt, abandonAt *time.Time) (Command, string, error) {
	if c == nil || c.Adapter == nil {
		return Command{}, "", errs.New(errs.ReasonCapabilityUnsupported, "this member sends no external commands")
	}
	caps, ok := c.Ops[operation]
	if !ok {
		return Command{}, "", errs.New(errs.ReasonRequestInvalid, "no such external operation: "+operation)
	}
	if abandonAt != nil && caps.Irrevocable {
		return Command{}, "", errs.New(errs.ReasonRequestInvalid, "an irrevocable command has no abandon time (P13.14)")
	}
	raw, err := canon.Encode(params)
	if err != nil {
		return Command{}, "", errs.New(errs.ReasonRequestInvalid, "the parameters have no canonical form: "+err.Error())
	}
	digest, err := canon.Digest(ParamsRecord, 1, params)
	if err != nil {
		return Command{}, "", err
	}
	cmd := Command{ID: ids.New().String(), Requester: requester, Reference: reference, Operation: operation, Params: params,
		ExpiresAt: expiresAt, AbandonAt: abandonAt}
	tag, err := tx.Exec(ctx, `INSERT INTO be_external_command (id, requester, reference, operation, params, params_digest,
		irrevocable, state, reason, expires_at, abandon_at) VALUES ($1, $2, $3, $4, $5, $6, $7, 'prepared', 'requested', $8, $9)
		ON CONFLICT (requester, reference) DO NOTHING`, cmd.ID, requester, reference, operation, raw, digest,
		caps.Irrevocable, expiresAt, abandonAt)
	if err != nil {
		return Command{}, "", err
	}
	if tag.RowsAffected() == 1 {
		return cmd, Prepared, nil
	}
	var id, state, stored, op string
	if err := tx.QueryRow(ctx, `SELECT id::text, state, params_digest, operation FROM be_external_command
		WHERE requester = $1 AND reference = $2`, requester, reference).Scan(&id, &state, &stored, &op); err != nil {
		return Command{}, "", err
	}
	if stored != digest || op != operation {
		return Command{}, "", errs.New(errs.ReasonIdempotencyKeyReused, "this business reference was requested with other parameters")
	}
	cmd.ID = id
	return cmd, state, nil
}

// Job is the SDK's job that processes the due commands.
func (c *Commands) Job() work.Job {
	return work.Job{Name: JobName, Kind: work.Singleton, Effect: work.IdempotentExternal, Interval: RunEvery,
		MaxAttempts: work.Unlimited, Timeout: 10 * time.Minute, Handle: func(ctx context.Context, r *work.Run) error {
			_, err := c.Run(ctx)
			if err == nil {
				err = r.Complete(ctx, nil)
			}
			return err
		}}
}

// Run processes the commands that are due now, a batch at most, and answers how many it took.
func (c *Commands) Run(ctx context.Context) (int, error) {
	var due []string
	if err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text FROM be_external_command WHERE next_at <= now()
			AND state IN ('prepared', 'retry_wait', 'sending', 'unknown', 'accepted') ORDER BY next_at, id LIMIT $1`, Batch)
		if err != nil {
			return err
		}
		due, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	}); err != nil {
		return 0, err
	}
	for _, id := range due {
		if err := c.process(ctx, id); err != nil {
			return 0, fmt.Errorf("external command %s: %w", id, err)
		}
	}
	return len(due), nil
}

// row is a command as the job reads it.
type row struct {
	Command
	Facts
	caps     Capabilities
	attempts int
	now      time.Time
}

var errNotDue = errors.New("not due")

// load locks a due command (another run holding it is skipped) and reads the primary's time.
func (c *Commands) load(ctx context.Context, tx db.Tx, id string) (*row, error) {
	r := &row{}
	var raw []byte
	var external *string
	err := tx.QueryRow(ctx, `SELECT id::text, requester, reference, operation, params, state, external_id, expires_at,
			resend_deadline, first_reached_at, last_reached_at, unknown_since, accepted_since, unrecorded_until, abandon_at,
			(SELECT count(*) FROM be_external_attempt a WHERE a.command = c.id), now()
		FROM be_external_command c WHERE id = $1 AND next_at <= now() FOR UPDATE SKIP LOCKED`, id).Scan(
		&r.ID, &r.Requester, &r.Reference, &r.Operation, &raw, &r.State, &external, &r.Facts.ExpiresAt, &r.ResendDeadline,
		&r.FirstReached, &r.LastReached, &r.UnknownSince, &r.AcceptedSince, &r.UnrecordedUntil, &r.Facts.AbandonAt,
		&r.attempts, &r.now)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errNotDue
	}
	if err != nil {
		return nil, err
	}
	if external != nil {
		r.ExternalID = *external
	}
	r.Command.ExpiresAt, r.Command.AbandonAt = r.Facts.ExpiresAt, r.Facts.AbandonAt
	if err := json.Unmarshal(raw, &r.Params); err != nil {
		return nil, err
	}
	r.caps = c.Ops[r.Operation]
	return r, nil
}

// process takes one command a step forward: a claim and a send, a takeover, a query.
func (c *Commands) process(ctx context.Context, id string) error {
	var r *row
	send, query := false, false
	err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var err error
		send, query = false, false
		if r, err = c.load(ctx, tx, id); err != nil {
			return err
		}
		if r.Facts.AbandonAt != nil && r.now.After(*r.Facts.AbandonAt) { // P13.14: before any send or query
			if next, why := Step(r.Facts, Event{Kind: Claim}, r.caps, r.now); next == Failed {
				return c.set(ctx, tx, r, Failed, why, r.now)
			}
		}
		switch r.State {
		case Prepared, RetryWait:
			if c.QueryOnly { // P13.10: they stay where they are
				return c.set(ctx, tx, r, r.State, "query-only: not sent", r.now.Add(BackoffStart))
			}
			next, why := Step(r.Facts, Event{Kind: Claim}, r.caps, r.now)
			if next == Unknown {
				return c.set(ctx, tx, r, Unknown, why, r.now)
			}
			send = true
			return c.set(ctx, tx, r, Sending, why, r.now.Add(SendLease)) // committed before the send (P13.4)
		case Sending: // due while sending: its claimant is gone (P13.5)
			next, why := Step(r.Facts, Event{Kind: LeaseLost}, r.caps, r.now)
			return c.set(ctx, tx, r, next, why, r.now)
		case Unknown:
			query = true
			return nil
		case Accepted:
			if r.caps.Finality > 0 && r.AcceptedSince != nil && r.now.Sub(*r.AcceptedSince) < r.caps.Finality {
				return c.set(ctx, tx, r, Accepted, "waiting for the final state", r.AcceptedSince.Add(r.caps.Finality))
			}
			next, why := Step(r.Facts, Event{Kind: FinalityPassed}, r.caps, r.now)
			if next == Accepted && r.caps.Query {
				query = true
				return nil
			}
			return c.set(ctx, tx, r, next, why, r.now.Add(BackoffCap))
		}
		return nil
	})
	switch {
	case errors.Is(err, errNotDue):
		return nil
	case err != nil:
		return err
	case send:
		return c.send(ctx, r)
	case query:
		return c.query(ctx, r)
	}
	return nil
}

// set writes a transition and when the command is next due.
func (c *Commands) set(ctx context.Context, tx db.Tx, r *row, state, why string, next time.Time) error {
	if state != r.State {
		c.log().Info("an external command moved", "event", "external_command_state", "command", r.ID,
			"operation", r.Operation, "from", r.State, "to", state, "why", why)
	}
	// a command waiting to be sent or queried is due at its abandon time at the latest (P13.14)
	if a := r.Facts.AbandonAt; a != nil && (state == Prepared || state == RetryWait || state == Unknown) && a.Before(next) {
		next = *a
	}
	_, err := tx.Exec(ctx, `UPDATE be_external_command SET state = $2, reason = $3, next_at = $4, updated_at = now(),
		unknown_since = CASE WHEN $2 = 'unknown' THEN coalesce(unknown_since, now()) ELSE NULL END,
		accepted_since = CASE WHEN $2 = 'accepted' THEN coalesce(accepted_since, now()) ELSE NULL END,
		settled_at = CASE WHEN $2 IN ('succeeded', 'failed', 'rejected') THEN now() END WHERE id = $1`,
		r.ID, state, why, next)
	if err != nil {
		return err
	}
	settles := terminal(state) && !terminal(r.State)
	r.State = state
	if settles && c.OnSettled != nil {
		return c.OnSettled(ctx, tx, r.Command, state, why)
	}
	return nil
}

func backoff(attempts int) time.Duration {
	d := BackoffStart
	for i := 1; i < attempts && d < BackoffCap; i++ {
		d *= 2
	}
	return min(d, BackoffCap)
}

func digestOf(s string) string {
	if s == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// send checks the deadline once more, writes an irrevocable command's issue record, sends, and applies the result.
func (c *Commands) send(ctx context.Context, r *row) error {
	n := r.attempts + 1
	var ok bool
	if err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT now()").Scan(&r.now); err != nil {
			return err
		}
		next, why := Step(r.Facts, Event{Kind: BeforeSend}, r.caps, r.now)
		if next == Unknown { // P13.4: not sent
			return c.set(ctx, tx, r, Unknown, why, r.now)
		}
		ok = true
		_, err := tx.Exec(ctx, `INSERT INTO be_external_attempt (command, n, kind, started_at) VALUES ($1, $2, 'send', now())`,
			r.ID, n)
		return err
	}); err != nil || !ok {
		return err
	}
	if r.caps.Irrevocable && !c.issued(ctx, r) {
		return c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			if _, err := tx.Exec(ctx, `UPDATE be_external_attempt SET ended_at = now(), result = 'not_sent_no_evidence'
				WHERE command = $1 AND n = $2`, r.ID, n); err != nil {
				return err
			}
			return c.set(ctx, tx, r, RetryWait, "the issue record reached neither the evidence store nor the alert channel: not sent",
				r.now.Add(backoff(n)))
		})
	}
	sctx, cancel := context.WithTimeout(ctx, SendTimeout)
	res, err := c.Adapter.Send(sctx, r.Command)
	cancel()
	if err != nil {
		res = Result{Class: MaybeReached, Response: err.Error()}
	}
	return c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT now()").Scan(&r.now); err != nil {
			return err
		}
		reached := res.Class != NotReached
		if reached {
			if r.FirstReached == nil {
				r.FirstReached = &r.now
			}
			r.LastReached = &r.now
		}
		r.ResendDeadline = Deadline(r.Facts, r.caps)
		if _, err := tx.Exec(ctx, `UPDATE be_external_attempt SET ended_at = now(), result = $3, response = $4, reached = $5
			WHERE command = $1 AND n = $2`, r.ID, n, res.Class, digestOf(res.Response), reached); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE be_external_command SET first_reached_at = $2, last_reached_at = $3,
			resend_deadline = $4, external_id = coalesce(nullif($5, ''), external_id), outcome = coalesce($6, outcome)
			WHERE id = $1`, r.ID, r.FirstReached, r.LastReached, r.ResendDeadline, res.ExternalID, outcomeOf(res.Outcome)); err != nil {
			return err
		}
		next, why := Step(r.Facts, Event{Kind: SendResult, Result: res.Class}, r.caps, r.now)
		return c.set(ctx, tx, r, next, why, c.nextAt(r, next, n))
	})
}

func outcomeOf(m map[string]any) any {
	if len(m) == 0 {
		return nil
	}
	b, _ := json.Marshal(m)
	return b
}

// nextAt is when a command in state is next due.
func (c *Commands) nextAt(r *row, state string, attempts int) time.Time {
	switch state {
	case RetryWait:
		return r.now.Add(backoff(attempts))
	case Unknown, Sending:
		return r.now
	case Accepted:
		if r.caps.Finality > 0 {
			return r.now.Add(r.caps.Finality)
		}
		return r.now.Add(BackoffStart)
	}
	return r.now.Add(BackoffCap) // manual or terminal: not processed again (the state takes it out)
}

// issued writes an irrevocable command's issue record before the send (P13.4): the evidence store, else the alert
// channel; false when neither took it.
func (c *Commands) issued(ctx context.Context, r *row) bool {
	if c.Evidence == nil {
		c.log().Error("an irrevocable external command is not sent: this member writes no evidence", "command", r.ID)
		return false
	}
	p, err := c.Evidence.Intent(ctx, IssuedCategory, r.Requester, evidence.Object{Type: "be.external.command", ID: r.ID},
		map[string]any{"reference": r.Reference, "operation": r.Operation, "component": c.Component,
			"params_digest": mustDigest(r.Params)})
	if err != nil || p == nil {
		return false
	}
	return p.Intent == "store" || p.Intent == "alert"
}

func mustDigest(params map[string]any) string {
	d, err := canon.Digest(ParamsRecord, 1, params)
	if err != nil {
		return ""
	}
	return d
}

// query asks the external system by the reference and applies the answer; a "not found" that allows it sends again
// with the same reference.
func (c *Commands) query(ctx context.Context, r *row) error {
	n := r.attempts + 1
	sctx, cancel := context.WithTimeout(ctx, SendTimeout)
	ans, err := c.Adapter.Query(sctx, r.Command)
	cancel()
	e, result := Event{Kind: QueryDown}, "unavailable"
	switch {
	case err != nil || !r.caps.Query:
	case ans.Found:
		e, result = Event{Kind: QueryFound, External: ans.State}, "found:"+ans.State
	default:
		e, result = Event{Kind: QueryNotFound}, "not_found"
	}
	resend := false
	if err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT now()").Scan(&r.now); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO be_external_attempt (command, n, kind, started_at, ended_at, result, response)
			VALUES ($1, $2, 'query', now(), now(), $3, $4)`, r.ID, n, result, digestOf(ans.Response)); err != nil {
			return err
		}
		if ans.Found {
			if _, err := tx.Exec(ctx, `UPDATE be_external_command SET external_id = coalesce(nullif($2, ''), external_id),
				outcome = coalesce($3, outcome) WHERE id = $1`, r.ID, ans.ExternalID, outcomeOf(ans.Outcome)); err != nil {
				return err
			}
		}
		next, why := Step(r.Facts, e, r.caps, r.now)
		if next == Sending && c.QueryOnly { // P13.10: the table would send it again; query-only does not
			next, why = Unknown, "query-only: not sent again ("+why+")"
		}
		if next == Sending {
			resend = true
			return c.set(ctx, tx, r, Sending, why, r.now.Add(SendLease))
		}
		if next == Unknown && r.UnknownSince != nil && r.now.Sub(*r.UnknownSince) > WarnAfter {
			c.log().Warn("an external command's result has been unknown for over an hour", "event", "external_command_unknown",
				"command", r.ID, "operation", r.Operation, "since", r.UnknownSince)
		}
		return c.set(ctx, tx, r, next, why, c.queryAgain(r, next, n))
	}); err != nil || !resend {
		return err
	}
	r.attempts = n // the query was this attempt; the send is the next
	return c.send(ctx, r)
}

// queryAgain is when a queried command is next due: unknown and accepted back off.
func (c *Commands) queryAgain(r *row, state string, attempts int) time.Time {
	if state == Unknown || state == Accepted {
		return r.now.Add(backoff(attempts))
	}
	return c.nextAt(r, state, attempts)
}

// Register is a person's verified outcome of a manual command (P13.8): succeeded, failed, rejected, or "did not
// happen" (sent again with the same reference). The caller audits it with its grounds.
func (c *Commands) Register(ctx context.Context, tx db.Tx, id, result, why string) error {
	if result != Succeeded && result != Failed && result != Rejected && result != Sending {
		return errs.New(errs.ReasonRequestInvalid, "a manual command is registered succeeded, failed, rejected or sending")
	}
	r := &row{Command: Command{ID: id}}
	var raw []byte
	if err := tx.QueryRow(ctx, `SELECT state, requester, reference, operation, params FROM be_external_command
		WHERE id = $1 FOR UPDATE`, id).Scan(&r.Facts.State, &r.Requester, &r.Reference, &r.Operation, &raw); err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &r.Params); err != nil {
		return err
	}
	if state := r.Facts.State; state != Manual {
		return errs.New(errs.ReasonRequestInvalid, "only a manual command is registered (it is "+state+")")
	}
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&r.now); err != nil {
		return err
	}
	if result == Sending { // "did not happen": the claim sends it again, same reference, through the deadline check
		result = RetryWait
	}
	return c.set(ctx, tx, r, result, "registered: "+why, r.now)
}
