// Package flow is the flow kernel (be-protocol chapter 17): a simple flow's manager running inside the business
// component that owns the documents. The component declares its flows and a few hooks; the runtime keeps the
// instances, tasks, candidates and decisions in the component's schema (migration 0025), resolves assignments against
// a directory of position holders, checks and records decisions, ends instances through the component's hook in the
// same transaction, and publishes the task projections by the shared task contract (schemas/flow/task.v1).
package flow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	sdkauthz "github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
)

// Form is a step's form (P17.2).
type Form string

const (
	Single Form = "single"
	Any    Form = "any"
)

// RuleKind is an assignment rule (P17.3).
type RuleKind string

const (
	Position RuleKind = "position" // the holders of a position in a legal entity
	Person   RuleKind = "person"   // one principal
)

// Step is one step of a definition: its form and the kind of its assignment; the parameters are bound at the start.
type Step struct {
	Form Form
	Rule RuleKind
}

// Definition is a flow (P17.1): immutable at its version; a change is a new version.
type Definition struct {
	Key           string
	Version       int
	TaskType      string // registered, globally unique
	DocumentType  string
	Operation     string        // what deciding a task of this flow needs (the component's operation)
	ReadOperation string        // who may read the document may see its tasks (P17.14); "" none beyond the parties
	OnReject      InstanceState // Rejected or Returned
	Steps         []Step
}

// Binding is a step's assignment bound at the start (P17.3): a position (legal entity and position ID) or a principal.
type Binding struct {
	LegalEntity string `json:"legal_entity,omitempty"`
	PositionID  string `json:"position_id,omitempty"`
	Principal   string `json:"principal,omitempty"`
}

// Document is the document an instance approves.
type Document struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	Number string `json:"number,omitempty"`
}

// SummaryField is one public field of a task's summary (P17.15).
type SummaryField struct {
	Key   string `json:"key"`
	Label string `json:"label"`
	Value string `json:"value"`
}

// Summary is what the task projections show the candidates: only fields the component makes public.
type Summary struct {
	Title  string         `json:"title"`
	Fields []SummaryField `json:"fields"`
}

// Directory answers who holds a position and who an account is (the employee projection, P17.8); complete is false when
// the projection cannot prove the answer (P9.24).
type Directory interface {
	Holders(ctx context.Context, tx db.Tx, legalEntity, positionID, date string) (accounts []string, complete bool, err error)
	Holds(ctx context.Context, tx db.Tx, account, legalEntity, positionID, date string) (holds, complete bool, err error)
	PersonOf(ctx context.Context, tx db.Tx, account string) (person string, err error)
}

// Hooks are the component's part.
type Hooks struct {
	// Document locks the document in tx (P17.17) and answers its current key revision, its legal entity's business
	// date and the facts a definition's operation is decided on.
	Document func(ctx context.Context, tx db.Tx, d Document) (keyRevision int64, businessDate string, facts sdkauthz.Facts, err error)
	// Ended changes the document when its instance ends (approved, rejected, returned), in the same transaction (P17.6).
	Ended func(ctx context.Context, tx db.Tx, i Instance) error
}

// Kernel is one component's flow manager.
type Kernel struct {
	Component string // the component's ID: the task projections' component and the action entry
	Stream    string // the component's stream the projections and heartbeats go on
	// TaskEvent and HeartbeatEvent are the event types: <domain>.<component>_task.changed.v1 and ..._task.heartbeat.v1
	TaskEvent, HeartbeatEvent string
	// Operation is the component's operation the standard routes are registered with (the decision itself is checked
	// against its definition's operation); ManageOperation lists the paused tasks (P17.14).
	Operation, ManageOperation string
	Definitions                []Definition
	Hooks
	Directory Directory // nil: no employee component; a position rule answers no_assignee
	Audit     Auditor   // nil: no audit (tests)

	DB     *db.DB
	Events interface {
		Emit(ctx context.Context, tx db.Tx, stream string, e events.Event) (string, error)
	}
	Log *slog.Logger

	defs map[string]Definition // by key and version
}

// Check validates the declaration once, at the start of the component.
func (k *Kernel) Check() error {
	if k.Component == "" || k.Stream == "" || k.TaskEvent == "" || k.HeartbeatEvent == "" || k.Operation == "" ||
		k.ManageOperation == "" || k.Document == nil || k.Ended == nil || k.DB == nil || k.Events == nil {
		return errors.New("flow: the kernel needs its component, stream, event types, operations, hooks, database and events")
	}
	k.defs = map[string]Definition{}
	types := map[string]string{}
	for _, d := range k.Definitions {
		switch {
		case d.Key == "" || d.Version < 1 || d.TaskType == "" || d.DocumentType == "" || d.Operation == "" || len(d.Steps) == 0:
			return fmt.Errorf("flow: definition %q version %d is incomplete", d.Key, d.Version)
		case d.OnReject != Rejected && d.OnReject != Returned:
			return fmt.Errorf("flow: definition %q: a rejection ends rejected or returned", d.Key)
		case types[d.TaskType] != "" && types[d.TaskType] != d.Key:
			return fmt.Errorf("flow: task type %s belongs to two flows", d.TaskType)
		}
		for i, s := range d.Steps {
			if s.Form != Single && s.Form != Any {
				return fmt.Errorf("flow: definition %q step %d: form %q is not in this version (P17.2)", d.Key, i+1, s.Form)
			}
			if s.Rule != Position && s.Rule != Person {
				return fmt.Errorf("flow: definition %q step %d: rule %q is not in this version (P17.3)", d.Key, i+1, s.Rule)
			}
		}
		types[d.TaskType] = d.Key
		k.defs[defKey(d.Key, d.Version)] = d
	}
	return nil
}

func defKey(key string, version int) string { return fmt.Sprintf("%s@%d", key, version) }

// definition is the bound definition of an instance.
func (k *Kernel) definition(key string, version int) (Definition, error) {
	d, ok := k.defs[defKey(key, version)]
	if !ok {
		return d, fmt.Errorf("flow: no definition %s version %d (an old version is kept while instances use it)", key, version)
	}
	return d, nil
}

// latest is the newest version of a flow (a new instance binds it, D60).
func (k *Kernel) latest(key string) (Definition, error) {
	var best Definition
	for _, d := range k.Definitions {
		if d.Key == key && d.Version > best.Version {
			best = d
		}
	}
	if best.Key == "" {
		return best, fmt.Errorf("flow: no flow %s", key)
	}
	return best, nil
}
