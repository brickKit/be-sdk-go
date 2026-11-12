package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Instance is what the runtime keeps of an instance (P17.4).
type Instance struct {
	ID              string
	FlowKey         string
	FlowVersion     int
	TaskType        string
	Document        Document
	LegalEntity     string
	KeyRevision     int64
	Digests         []string
	Assignments     []Binding
	OnReject        InstanceState
	Summary         Summary
	Initiator       string
	InitiatorPerson string
	State           InstanceState
	Step            int
	Version         int64
	// Ending is the decision that ended the instance (approved, rejected, returned), for the end hook; nil otherwise.
	Ending *DecisionRecord
}

// Start is a new instance's request: the flow, the document, its key revision and each step's digest and binding, the
// initiator, the summary.
type Start struct {
	Flow        string
	Document    Document
	LegalEntity string
	KeyRevision int64
	Digests     []string  // one a step
	Bindings    []Binding // one a step
	Initiator   string
	Summary     Summary
	// Automatic is the component's flow choice of no manual approval (P17.5): the instance is approved at once with
	// its key revision and digests, no task; Bindings may be empty.
	Automatic bool
}

// ErrManual is an automatic start of a flow people must approve (P17.5).
var ErrManual = errors.New("flow: people must approve this document type: no automatic approval")

// ErrLive is a start for a document that has a running or approved instance already (P17.4).
var ErrLive = errors.New("flow: the document has a running or approved instance")

// StartIn starts an instance in the component's transaction that wrote the document (P17.5): the newest version of
// the flow, the first step's tasks resolved and published.
func (k *Kernel) StartIn(ctx context.Context, tx db.Tx, s Start) (Instance, error) {
	d, err := k.latest(s.Flow)
	if err != nil {
		return Instance{}, err
	}
	switch {
	case s.Automatic && d.Manual:
		return Instance{}, ErrManual
	case len(s.Digests) != len(d.Steps) || (!s.Automatic && len(s.Bindings) != len(d.Steps)):
		return Instance{}, fmt.Errorf("flow: %s has %d steps: a digest and a binding each", d.Key, len(d.Steps))
	}
	if !s.Automatic {
		for n, st := range d.Steps {
			b := s.Bindings[n]
			pos, person := b.PositionID != "" && b.LegalEntity != "", b.Principal != ""
			if pos == person || (st.Rule == Position && !pos) || (st.Rule == Person && !person) {
				return Instance{}, fmt.Errorf("flow: %s step %d (%s) is bound to a position or a principal, one of them", d.Key, n+1, st.Rule)
			}
		}
	}
	state := Running
	if s.Automatic {
		state = Approved
	}
	person := s.Initiator
	if k.Directory != nil {
		if person, err = k.Directory.PersonOf(ctx, tx, s.Initiator); err != nil {
			return Instance{}, err
		}
	}
	i := Instance{ID: ids.New().String(), FlowKey: d.Key, FlowVersion: d.Version, TaskType: d.TaskType, Document: s.Document,
		LegalEntity: s.LegalEntity, KeyRevision: s.KeyRevision, Digests: s.Digests, Assignments: s.Bindings,
		OnReject: d.OnReject, Summary: s.Summary, Initiator: s.Initiator, InitiatorPerson: person, State: state, Step: 1,
		Version: 1}
	if i.Assignments == nil {
		i.Assignments = []Binding{}
	}
	digests, _ := json.Marshal(i.Digests)
	bindings, _ := json.Marshal(i.Assignments)
	summary, _ := json.Marshal(i.Summary)
	var le *string
	if i.LegalEntity != "" {
		le = &i.LegalEntity
	}
	_, err = tx.Exec(ctx, `INSERT INTO be_flow_instance (id, flow_key, flow_version, task_type, document_type, document_id,
		document_number, legal_entity_id, key_revision, digests, assignments, reject_outcome, summary, initiator,
		initiator_person, state, step) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 1)`,
		i.ID, i.FlowKey, i.FlowVersion, i.TaskType, i.Document.Type, i.Document.ID, nullable(i.Document.Number), le,
		i.KeyRevision, digests, bindings, string(i.OnReject), summary, i.Initiator, i.InitiatorPerson, string(state))
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" { // the unique index of live instances
			return Instance{}, ErrLive
		}
		return Instance{}, err
	}
	if s.Automatic {
		return i, nil
	}
	return i, k.makeTasks(ctx, tx, i)
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// makeTasks makes the current step's tasks: one task, its candidates resolved now (P17.8); paused when the directory
// cannot prove the holders, or when nobody may decide.
func (k *Kernel) makeTasks(ctx context.Context, tx db.Tx, i Instance) error {
	id := ids.New().String()
	cands, reason, err := k.resolve(ctx, tx, i)
	if err != nil {
		return err
	}
	state := Pending
	if reason != "" {
		state = Paused
	}
	if _, err := tx.Exec(ctx, `INSERT INTO be_flow_task (id, instance_id, step, state, paused_reason) VALUES ($1, $2, $3, $4, $5)`,
		id, i.ID, i.Step, string(state), nullable(reason)); err != nil {
		return err
	}
	if reason == ReasonNoAssignee {
		k.logger().Warn("an approval task has nobody who may decide it", "event", "flow_no_assignee", "task", id,
			"task_type", i.TaskType, "document", i.Document.ID)
	}
	if err := k.setCandidates(ctx, tx, id, cands); err != nil {
		return err
	}
	return k.publish(ctx, tx, id)
}

// The reasons a task is paused (P17.8).
const (
	ReasonProjectionIncomplete = "projection_incomplete"
	ReasonNoAssignee           = "no_assignee"
)

// resolve answers the current step's candidates, the initiator's person left out (D10), or why the task waits.
func (k *Kernel) resolve(ctx context.Context, tx db.Tx, i Instance) ([]string, string, error) {
	b := i.Assignments[i.Step-1]
	d, err := k.definition(i.FlowKey, i.FlowVersion)
	if err != nil {
		return nil, "", err
	}
	var accounts []string
	switch kind(d.Steps[i.Step-1], b) {
	case Person:
		accounts = []string{b.Principal}
	case Position:
		if k.Directory == nil {
			return nil, ReasonNoAssignee, nil
		}
		_, date, _, err := k.Document(ctx, tx, i.Document)
		if err != nil {
			return nil, "", err
		}
		holders, complete, err := k.Directory.Holders(ctx, tx, b.LegalEntity, b.PositionID, date)
		if err != nil {
			return nil, "", err
		}
		if !complete {
			return nil, ReasonProjectionIncomplete, nil
		}
		accounts = holders
	}
	var out []string
	for _, a := range accounts {
		if a == "" || a == i.Initiator {
			continue
		}
		if k.Directory != nil {
			p, err := k.Directory.PersonOf(ctx, tx, a)
			if err != nil {
				return nil, "", err
			}
			if p == i.InitiatorPerson {
				continue
			}
		}
		out = append(out, a)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil, ReasonNoAssignee, nil
	}
	return out, "", nil
}

func (k *Kernel) setCandidates(ctx context.Context, tx db.Tx, task string, cands []string) error {
	if _, err := tx.Exec(ctx, `DELETE FROM be_flow_candidate WHERE task_id = $1`, task); err != nil {
		return err
	}
	if len(cands) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO be_flow_candidate (task_id, principal) SELECT $1, unnest($2::text[])`, task, cands)
	return err
}

// Task is a task's complete state as the shared task contract carries it (schemas/flow/task.v1, P17.15).
type Task struct {
	ID           string          `json:"id"`
	TaskType     string          `json:"task_type"`
	Component    string          `json:"component"`
	Document     TaskDocument    `json:"document"`
	Summary      Summary         `json:"summary"`
	Candidates   []string        `json:"candidates"`
	State        TaskState       `json:"state"`
	PausedReason *string         `json:"paused_reason"`
	Decision     *DecisionRecord `json:"decision"`
	DueAt        *string         `json:"due_at"`
	Instance     TaskInstance    `json:"instance"`
	LegalEntity  *string         `json:"legal_entity_id"`
	Version      int64           `json:"version"`
	UpdatedAt    string          `json:"updated_at"`
	instance     Instance
}

// TaskDocument is the projection's document reference.
type TaskDocument struct {
	Component string  `json:"component"`
	Type      string  `json:"type"`
	ID        string  `json:"id"`
	Number    *string `json:"number"`
}

// TaskInstance is the projection's instance.
type TaskInstance struct {
	ID    string        `json:"id"`
	State InstanceState `json:"state"`
	Step  int           `json:"step"`
}

// DecisionRecord is a decided task's decision.
type DecisionRecord struct {
	Decision Choice  `json:"decision"`
	By       string  `json:"by"`
	At       string  `json:"at"`
	Comment  *string `json:"comment"`
}

const instanceCols = `i.id::text, i.flow_key, i.flow_version, i.task_type, i.document_type, i.document_id::text,
	coalesce(i.document_number, ''), coalesce(i.legal_entity_id::text, ''), i.key_revision, i.digests, i.assignments,
	i.reject_outcome, i.summary, i.initiator, i.initiator_person, i.state, i.step, i.version`

func scanInstance(row pgx.Row) (Instance, error) {
	var i Instance
	var digests, bindings, summary []byte
	var onReject, state string
	err := row.Scan(&i.ID, &i.FlowKey, &i.FlowVersion, &i.TaskType, &i.Document.Type, &i.Document.ID, &i.Document.Number,
		&i.LegalEntity, &i.KeyRevision, &digests, &bindings, &onReject, &summary, &i.Initiator, &i.InitiatorPerson, &state,
		&i.Step, &i.Version)
	if err != nil {
		return i, err
	}
	i.OnReject, i.State = InstanceState(onReject), InstanceState(state)
	if err := json.Unmarshal(digests, &i.Digests); err != nil {
		return i, err
	}
	if err := json.Unmarshal(bindings, &i.Assignments); err != nil {
		return i, err
	}
	return i, json.Unmarshal(summary, &i.Summary)
}

// readTask reads a task, its instance, candidates and decision; lock takes the instance's and the task's rows.
func (k *Kernel) readTask(ctx context.Context, tx db.Tx, id string, lock bool) (Task, error) {
	var t Task
	q := `SELECT ` + instanceCols + `, t.id::text, t.state, t.paused_reason, t.version, t.updated_at
		FROM be_flow_task t JOIN be_flow_instance i ON i.id = t.instance_id WHERE t.id = $1::uuid`
	if lock {
		q += ` FOR UPDATE OF i, t`
	}
	var st string
	var at time.Time
	var cols []any
	var inst Instance
	var digests, bindings, summary []byte
	var onReject, istate string
	cols = append(cols, &inst.ID, &inst.FlowKey, &inst.FlowVersion, &inst.TaskType, &inst.Document.Type, &inst.Document.ID,
		&inst.Document.Number, &inst.LegalEntity, &inst.KeyRevision, &digests, &bindings, &onReject, &summary, &inst.Initiator,
		&inst.InitiatorPerson, &istate, &inst.Step, &inst.Version, &t.ID, &st, &t.PausedReason, &t.Version, &at)
	if err := tx.QueryRow(ctx, q, id).Scan(cols...); err != nil {
		return t, err
	}
	inst.OnReject, inst.State = InstanceState(onReject), InstanceState(istate)
	_ = json.Unmarshal(digests, &inst.Digests)
	_ = json.Unmarshal(bindings, &inst.Assignments)
	_ = json.Unmarshal(summary, &inst.Summary)
	t.instance, t.State, t.UpdatedAt = inst, TaskState(st), canon.Time(at)
	t.TaskType, t.Component, t.Summary = inst.TaskType, k.Component, inst.Summary
	t.Document = TaskDocument{Component: k.Component, Type: inst.Document.Type, ID: inst.Document.ID, Number: nullable(inst.Document.Number)}
	t.Instance = TaskInstance{ID: inst.ID, State: inst.State, Step: inst.Step}
	t.LegalEntity = nullable(inst.LegalEntity)
	t.Candidates = []string{}
	rows, err := tx.Query(ctx, `SELECT principal FROM be_flow_candidate WHERE task_id = $1 ORDER BY principal`, id)
	if err != nil {
		return t, err
	}
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return t, err
		}
		t.Candidates = append(t.Candidates, p)
	}
	rows.Close()
	var dec DecisionRecord
	var dat time.Time
	err = tx.QueryRow(ctx, `SELECT decision, by_account, decided_at, comment FROM be_flow_decision WHERE task_id = $1`, id).
		Scan(&dec.Decision, &dec.By, &dat, &dec.Comment)
	switch {
	case err == nil:
		dec.At = canon.Time(dat)
		t.Decision = &dec
	case !errors.Is(err, pgx.ErrNoRows):
		return t, err
	}
	return t, nil
}

// publish publishes a task's complete state on the component's stream (P17.15), in the transaction that changed it.
func (k *Kernel) publish(ctx context.Context, tx db.Tx, id string) error {
	t, err := k.readTask(ctx, tx, id, false)
	if err != nil {
		return err
	}
	_, err = k.Events.Emit(ctx, tx, k.Stream, events.Event{Type: k.TaskEvent, RecordID: t.ID, Version: t.Version,
		Projection: events.Full, LegalEntity: t.instance.LegalEntity, Data: t})
	return err
}

// bump changes a task's state and version, its candidates (nil: none, as a task that left pending has), and publishes it.
func (k *Kernel) bump(ctx context.Context, tx db.Tx, id string, state TaskState, reason string, cands []string) error {
	if err := k.change(ctx, tx, id, state, reason, cands); err != nil {
		return err
	}
	return k.publish(ctx, tx, id)
}

// change is bump without the publication: the caller publishes once what else changes with it has changed.
func (k *Kernel) change(ctx context.Context, tx db.Tx, id string, state TaskState, reason string, cands []string) error {
	if _, err := tx.Exec(ctx, `UPDATE be_flow_task SET state = $2, paused_reason = $3, version = version + 1,
		updated_at = now() WHERE id = $1`, id, string(state), nullable(reason)); err != nil {
		return err
	}
	return k.setCandidates(ctx, tx, id, cands)
}

// end ends an instance (P17.6): its state, the rest of its pending and paused tasks superseded, and the component's
// hook for the ends that change the document.
func (k *Kernel) end(ctx context.Context, tx db.Tx, i Instance, to InstanceState, hook bool) error {
	if _, err := tx.Exec(ctx, `UPDATE be_flow_instance SET state = $2, version = version + 1, updated_at = now() WHERE id = $1`,
		i.ID, string(to)); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT id::text FROM be_flow_task WHERE instance_id = $1 AND state IN ('pending', 'paused')`, i.ID)
	if err != nil {
		return err
	}
	var open []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		open = append(open, id)
	}
	rows.Close()
	for _, id := range open {
		if err := k.bump(ctx, tx, id, Superseded, "", nil); err != nil {
			return err
		}
	}
	i.State = to
	if hook {
		return k.Ended(ctx, tx, i)
	}
	return nil
}

// live reads a document's running or approved instance, locked; ok is false when there is none.
func (k *Kernel) live(ctx context.Context, tx db.Tx, d Document) (Instance, bool, error) {
	i, err := scanInstance(tx.QueryRow(ctx, `SELECT `+instanceCols+` FROM be_flow_instance i WHERE i.document_type = $1
		AND i.document_id = $2::uuid AND i.state IN ('running', 'approved') FOR UPDATE`, d.Type, d.ID))
	if errors.Is(err, pgx.ErrNoRows) {
		return i, false, nil
	}
	return i, err == nil, err
}

// WithdrawIn withdraws the document's running instance (P17.6: the initiator withdrew, a pending document was
// cancelled, the component's rule took it back to draft); nothing when there is none. The component changes the
// document itself: the end hook is not called.
func (k *Kernel) WithdrawIn(ctx context.Context, tx db.Tx, d Document) error {
	i, ok, err := k.live(ctx, tx, d)
	if err != nil || !ok || i.State != Running {
		return err
	}
	return k.end(ctx, tx, i, Withdrawn, false)
}

// SupersedeIn supersedes the document's running or approved instance (P17.6: a new key revision, or the document left
// approved by the component's rule); nothing when there is none. The end hook is not called.
func (k *Kernel) SupersedeIn(ctx context.Context, tx db.Tx, d Document) error {
	i, ok, err := k.live(ctx, tx, d)
	if err != nil || !ok {
		return err
	}
	return k.end(ctx, tx, i, SupersededInstance, false)
}

// ReleaseIn is the release check in the transaction that starts the execution (P17.7): an approved instance whose key
// revision and digests are the document's now. The error says why not.
func (k *Kernel) ReleaseIn(ctx context.Context, tx db.Tx, d Document, keyRevision int64, digests []string) error {
	i, ok, err := k.live(ctx, tx, d)
	switch {
	case err != nil:
		return err
	case !ok || i.State != Approved:
		return ErrNotReleased{Why: "no approved instance"}
	case i.KeyRevision != keyRevision:
		return ErrNotReleased{Why: fmt.Sprintf("the approval is of key revision %d, the document is at %d", i.KeyRevision, keyRevision)}
	case len(digests) != len(i.Digests):
		return ErrNotReleased{Why: "the steps' digests differ"}
	}
	for n := range digests {
		if digests[n] != i.Digests[n] {
			return ErrNotReleased{Why: fmt.Sprintf("step %d's approval basis changed", n+1)}
		}
	}
	return nil
}

// ErrNotReleased is a release check that does not release (P17.7).
type ErrNotReleased struct{ Why string }

func (e ErrNotReleased) Error() string { return "flow: not released: " + e.Why }

// LiveOf answers the document's running or approved instance (for the component's reads); ok is false when none.
func (k *Kernel) LiveOf(ctx context.Context, tx db.Tx, d Document) (Instance, bool, error) {
	i, err := scanInstance(tx.QueryRow(ctx, `SELECT `+instanceCols+` FROM be_flow_instance i WHERE i.document_type = $1
		AND i.document_id = $2::uuid AND i.state IN ('running', 'approved')`, d.Type, d.ID))
	if errors.Is(err, pgx.ErrNoRows) {
		return i, false, nil
	}
	return i, err == nil, err
}

func (k *Kernel) logger() interface{ Warn(string, ...any) } {
	if k.Log != nil {
		return k.Log
	}
	return noLog{}
}

type noLog struct{}

func (noLog) Warn(string, ...any) {}
