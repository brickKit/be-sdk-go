package flow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/audit"
	sdkauthz "github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/idem"
	"github.com/jackc/pgx/v5"
)

// DecisionIn is a decision's request (P17.10).
type DecisionIn struct {
	Version  int64   `json:"version"`
	Decision Choice  `json:"decision"`
	Comment  *string `json:"comment"`
}

// Auditor writes the audit records of decisions and of refused checks (protocol 03); the runtime's audit.
type Auditor interface {
	Write(ctx context.Context, tx db.Tx, r audit.Record) (string, error)
	Denial(ctx context.Context, r audit.Record) error
}

// Decide is a decision: the idempotency key's result when it has one (P17.11); otherwise, in one transaction, the
// document and the task locked (P17.17), the checks of P17.10 in order, the decision recorded and its outcome
// carried out (P17.13). The subject is the caller's; req is the idempotency request of the route.
func (k *Kernel) Decide(ctx context.Context, subj *sdkauthz.Subject, req idem.Request, taskID string, in DecisionIn) (Task, error) {
	if in.Decision != Approve && in.Decision != Reject {
		return Task{}, errs.New(errs.ReasonRequestInvalid, "decision is approve or reject").
			WithViolations(errs.Violation{Field: "decision", Reason: "INVALID", Description: "approve or reject"})
	}
	out, _, err := idem.Do(ctx, k.DB, req, func(ctx context.Context, tx db.Tx) (idem.Outcome, error) {
		t, err := k.decideIn(ctx, tx, subj, taskID, in)
		if err != nil {
			return idem.Outcome{}, err
		}
		b, err := json.Marshal(t)
		return idem.Outcome{Status: 200, Result: b}, err
	})
	var r refused
	if errors.As(err, &r) && k.Audit != nil { // the refused check's security record, after the transaction (P17.10)
		if derr := k.Audit.Denial(ctx, audit.Record{Operation: "be.flow.decide", Actor: subj.Principal,
			Target: audit.TargetOf("flow.task", taskID, nil), Reason: r.reason}); derr != nil {
			k.logger().Warn("a refused decision's audit record failed", "error", derr.Error())
		}
	}
	if err != nil {
		return Task{}, err
	}
	var t Task
	return t, json.Unmarshal(out.Result, &t)
}

// refused is a refused check: the protocol's error, and the reason for the security record.
type refused struct {
	reason string
	err    error
}

func (r refused) Error() string { return r.err.Error() }
func (r refused) Unwrap() error { return r.err }

// errNotFound is a task that does not exist or that the caller may not see.
func errNotFound() error { return errs.New(errs.ReasonNotFound, "task not found") }

func (k *Kernel) decideIn(ctx context.Context, tx db.Tx, subj *sdkauthz.Subject, taskID string, in DecisionIn) (Task, error) {
	if !uuidForm(taskID) {
		return Task{}, errNotFound()
	}
	// the document first (P17.17): read the task unlocked to know the document, lock the document, then the task
	peek, err := k.readTask(ctx, tx, taskID, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, errNotFound()
	}
	if err != nil {
		return Task{}, err
	}
	revision, date, facts, err := k.Document(ctx, tx, peek.instance.Document)
	if err != nil {
		return Task{}, err
	}
	t, err := k.readTask(ctx, tx, taskID, true)
	if err != nil {
		return Task{}, err
	}
	i := t.instance
	d, err := k.definition(i.FlowKey, i.FlowVersion)
	if err != nil {
		return Task{}, err
	}
	me := subj.Principal
	if !k.visible(ctx, subj, t, d, facts) { // the first check of P17.10: a refusal like the others
		return Task{}, refused{reason: errs.ReasonNotFound, err: errNotFound()}
	}
	if in.Decision == Reject && d.RejectComment && (in.Comment == nil || strings.TrimSpace(*in.Comment) == "") { // P17.1
		return Task{}, errs.New(errs.ReasonRequestInvalid, "a rejection of this task says why").
			WithViolations(errs.Violation{Field: "comment", Reason: "REQUIRED", Description: "the rejection's comment"})
	}
	s := Situation{Task: t.State, Instance: i.State, VersionMatches: in.Version == t.Version, Candidate: contains(t.Candidates, me),
		RevisionMatches: i.KeyRevision == revision, Qualification: Qualified}
	person := me
	if k.Directory != nil {
		if person, err = k.Directory.PersonOf(ctx, tx, me); err != nil {
			return Task{}, err
		}
		initiatorNow, err := k.Directory.PersonOf(ctx, tx, i.Initiator)
		if err != nil {
			return Task{}, err
		}
		s.Self = person == i.InitiatorPerson || person == initiatorNow
	}
	s.Self = s.Self || me == i.Initiator
	_, _, err = subj.Check(ctx, d.Operation, facts)
	s.Operation = err == nil
	if d.Steps[i.Step-1].Rule == Position && s.Task == Pending {
		b := i.Assignments[i.Step-1]
		holds, complete, err := k.Directory.Holds(ctx, tx, me, b.LegalEntity, b.PositionID, date)
		switch {
		case err != nil:
			return Task{}, err
		case !complete:
			s.Qualification = Unknown
		case !holds:
			s.Qualification = NotQualified
		}
	}
	others, err := k.others(ctx, tx, t)
	if err != nil {
		return Task{}, err
	}
	o := Decide(s, in.Decision, len(others) > 0, i.Step == len(d.Steps), i.OnReject)
	if o.Refused != "" {
		return Task{}, refused{reason: o.Refused, err: refusal(o.Refused)}
	}
	digest := ""
	if i.Step-1 < len(i.Digests) {
		digest = i.Digests[i.Step-1]
	}
	if _, err := tx.Exec(ctx, `INSERT INTO be_flow_decision (task_id, decision, comment, by_account, by_person, key_revision,
		digest) VALUES ($1, $2, $3, $4, $5, $6, $7)`, t.ID, string(in.Decision), in.Comment, me, person, i.KeyRevision,
		digest); err != nil {
		return Task{}, err
	}
	if err := k.bump(ctx, tx, t.ID, Decided, "", nil); err != nil {
		return Task{}, err
	}
	for _, id := range others {
		if err := k.bump(ctx, tx, id, Superseded, "", nil); err != nil {
			return Task{}, err
		}
	}
	switch {
	case o.NextStep:
		if _, err := tx.Exec(ctx, `UPDATE be_flow_instance SET step = step + 1, version = version + 1, updated_at = now()
			WHERE id = $1`, i.ID); err != nil {
			return Task{}, err
		}
		i.Step++
		if err := k.makeTasks(ctx, tx, i); err != nil {
			return Task{}, err
		}
	case o.Instance != Running:
		var at time.Time
		if err := tx.QueryRow(ctx, `SELECT decided_at FROM be_flow_decision WHERE task_id = $1`, t.ID).Scan(&at); err != nil {
			return Task{}, err
		}
		i.Ending = &DecisionRecord{Decision: in.Decision, By: me, At: canon.Time(at), Comment: in.Comment}
		if err := k.end(ctx, tx, i, o.Instance, true); err != nil {
			return Task{}, err
		}
	}
	if k.Audit != nil {
		if _, err := k.Audit.Write(ctx, tx, audit.Record{Category: audit.BusinessChange, Operation: d.Operation,
			Outcome: audit.Succeeded, Actor: me, Target: audit.TargetOf("flow.task", t.ID, map[string]any{"version": t.Version + 1}),
			Changes: map[string]any{"decision": in.Decision, "document": i.Document, "instance": i.ID}}); err != nil {
			return Task{}, err
		}
	}
	return k.readTask(ctx, tx, t.ID, false)
}

// others are the step's other pending tasks: an "any" step's first approval and any rejection supersede them.
func (k *Kernel) others(ctx context.Context, tx db.Tx, t Task) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT id::text FROM be_flow_task WHERE instance_id = $1 AND step = $2 AND id <> $3
		AND state IN ('pending', 'paused')`, t.instance.ID, t.instance.Step, t.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// visible says whether the caller may see a task (P17.14): a candidate, its decider, the initiator, or one who may read
// the document.
func (k *Kernel) visible(ctx context.Context, subj *sdkauthz.Subject, t Task, d Definition, facts sdkauthz.Facts) bool {
	me := subj.Principal
	if contains(t.Candidates, me) || me == t.instance.Initiator || (t.Decision != nil && t.Decision.By == me) {
		return true
	}
	if d.ReadOperation == "" {
		return false
	}
	_, _, err := subj.Check(ctx, d.ReadOperation, facts)
	return err == nil
}

// refusal is a refused check as the protocol's error.
func refusal(reason string) error {
	return errs.New(reason, map[string]string{
		ReasonTaskNotPending: "the task is not pending, or its instance has ended", ReasonVersionConflict: "the task changed since that version",
		ReasonSelfApproval: "the one who started it does not decide it", ReasonNotAssignee: "you are not a candidate of this task",
		ReasonPermissionDenied: "you may not decide this task", ReasonNotQualified: "you no longer qualify for this step",
		ReasonQualificationUnknown: "your qualification cannot be proven right now", ReasonRevisionChanged: "the document changed since the approval started",
	}[reason])
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func uuidForm(s string) bool {
	if len(s) != 36 {
		return false
	}
	for n, r := range s {
		if n == 8 || n == 13 || n == 18 || n == 23 {
			if r != '-' {
				return false
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

// DecideDocument is a decision through the component's own approve or reject route (P17.14): the task is picked in the
// document's newest instance (the current step's pending task the caller is a candidate of; else a task the caller
// decided, so a repeat replays; else any open task of the current step, so the checks of P17.10 answer why) and
// decided at its version now. Without a key the key is the task, the caller and the decision; the request compared
// for idempotency is the decision and the comment. A document without an instance answers TASK_NOT_PENDING.
func (k *Kernel) DecideDocument(ctx context.Context, subj *sdkauthz.Subject, key string, d Document, c Choice, comment *string) (Task, error) {
	me := subj.Principal
	var task string
	var version int64
	err := k.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `WITH i AS (SELECT id, step FROM be_flow_instance WHERE document_type = $1 AND document_id = $2::uuid
				ORDER BY created_at DESC, id DESC LIMIT 1)
			SELECT t.id::text, t.version FROM be_flow_task t JOIN i ON i.id = t.instance_id
				LEFT JOIN be_flow_decision d ON d.task_id = t.id
			ORDER BY CASE
				WHEN t.step = i.step AND t.state = 'pending' AND EXISTS (SELECT 1 FROM be_flow_candidate c
					WHERE c.task_id = t.id AND c.principal = $3) THEN 0
				WHEN d.by_account = $3 THEN 1
				WHEN t.step = i.step AND t.state IN ('pending', 'paused') THEN 2
				ELSE 3 END, t.created_at DESC, t.id DESC LIMIT 1`, d.Type, d.ID, me).Scan(&task, &version)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, refusal(ReasonTaskNotPending)
	}
	if err != nil {
		return Task{}, err
	}
	if key == "" {
		key = "flow:" + task + ":" + string(c)
	} else if !idem.ValidKey(key) {
		return Task{}, errs.New(errs.ReasonRequestInvalid, "the idempotency key is 1-255 visible ASCII characters")
	}
	body, err := json.Marshal(struct {
		Decision Choice  `json:"decision"`
		Comment  *string `json:"comment"`
	}{c, comment})
	if err != nil {
		return Task{}, err
	}
	return k.Decide(ctx, subj, idem.Request{Principal: me, Key: key, Operation: k.Operation, Body: body,
		Target: "POST " + RoutePrefix + "tasks/" + task + "/decisions"}, task, DecisionIn{Version: version, Decision: c, Comment: comment})
}
