// Package notify is a sender's side of the notification intents (be-protocol chapter 18): the intent by the shared
// contract (schemas/notification/intent.v1), checked against it and emitted on the sender's own stream through the
// outbox, in the transaction of the business write that causes it (P18.1, P18.2). The sender does not call the
// notification component and does not need it in the deployment.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/ids"
)

// The categories (P18.3).
const (
	ApprovalTask  = "approval_task"
	DueReminder   = "due_reminder"
	SecurityAlert = "security_alert"
	LegalNotice   = "legal_notice"
	ExternalComm  = "external_comm"
	Marketing     = "marketing"
)

// The kinds of intent (P18.1).
const (
	Deliver  = "deliver"
	Withdraw = "withdraw"
)

// Suffix is the intent's event type after the sender's stream (P18.2): erp.purchase_notification.intent.v1.
const Suffix = "_notification.intent.v1"

// DefaultExpiry is how long an intent without a deadline of its own may wait to be delivered (protocol 10's
// parameter, P18.6).
const DefaultExpiry = 24 * time.Hour

// Position is a position recipient (P18.4): its holders on Date (the legal entity's business date; empty: the day of
// the delivery).
type Position struct {
	LegalEntity string `json:"legal_entity_id"`
	PositionID  string `json:"position_id"`
	Date        string `json:"date,omitempty"`
}

// Recipient is a principal or a position (P18.4).
type Recipient struct {
	Principal string    `json:"principal,omitempty"`
	Position  *Position `json:"position,omitempty"`
}

// Intent is one notification intent (P18.1). Emit fills ID and OccurredAt when empty.
type Intent struct {
	ID          string            `json:"intent_id"`
	Sender      string            `json:"sender"`
	Category    string            `json:"category"`
	OccurredAt  string            `json:"occurred_at"`
	Kind        string            `json:"kind"`
	Recipients  []Recipient       `json:"recipients,omitempty"`
	Title       string            `json:"title,omitempty"`
	Link        string            `json:"link,omitempty"`
	Template    string            `json:"template,omitempty"`
	Params      map[string]string `json:"params,omitempty"`
	ExpiresAt   string            `json:"expires_at,omitempty"`
	CollapseKey string            `json:"collapse_key,omitempty"`
	CollapseSeq int64             `json:"collapse_seq,omitempty"`
	// Actions are the action grants by principal recipient (P18.11, chapter 20: not secrets), with the channel members
	// that take their actions (the sender's FLOW_ACTION_SOURCES).
	Actions        map[string]Grant `json:"actions,omitempty"`
	ActionChannels []string         `json:"action_channels,omitempty"`
}

// Grant is one principal's action grant in an intent (P18.11, P20.5): its id and the actions it allows.
type Grant struct {
	ID      string   `json:"grant_id"`
	Actions []Choice `json:"actions"`
}

// Choice is an action with its label on the button.
type Choice struct {
	Action string `json:"action"`
	Label  string `json:"label"`
}

// Emitter is the runtime's events (rt.Events).
type Emitter interface {
	Emit(ctx context.Context, tx db.Tx, stream string, e events.Event) (string, error)
}

// Stream is a component's stream (P18.2): erp/purchase publishes on erp.purchase, integration/im-dingtalk on
// integration.im_dingtalk (a stream name has no "-", chapter 06).
func Stream(component string) string {
	return strings.ReplaceAll(strings.ReplaceAll(component, "/", "."), "-", "_")
}

// Type is the intent's event type of a component (P18.2).
func Type(component string) string { return Stream(component) + Suffix }

// Emit checks the intent against the shared contract and emits it on its sender's stream in tx (P18.2); the intent as
// emitted is answered. A deliver intent's expiry must come after it occurred (P18.6, P18.8).
func Emit(ctx context.Context, tx db.Tx, em Emitter, in Intent) (Intent, error) {
	if in.ID == "" {
		in.ID = ids.New().String()
	}
	if in.OccurredAt == "" {
		in.OccurredAt = canon.Time(time.Now())
	}
	if err := Check(in); err != nil {
		return in, err
	}
	_, err := em.Emit(ctx, tx, Stream(in.Sender), events.Event{Type: Type(in.Sender), RecordID: in.ID, Version: 1,
		Projection: events.Full, Data: in})
	return in, err
}

// Check validates an intent against schemas/notification/intent.v1 and what the schema cannot say (P18.1).
func Check(in Intent) error {
	raw, _ := json.Marshal(in)
	if err := Validate(raw); err != nil {
		return err
	}
	if !ExpiresInTime(in) {
		return errors.New("notify: a deliver intent expires after it occurred (P18.6)")
	}
	return nil
}

// Validate checks a payload as it came against schemas/notification/intent.v1 (the receiver's side, P18.8).
func Validate(raw []byte) error {
	s, err := schema()
	if err != nil {
		return err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("notify: the intent is not JSON: %w", err)
	}
	if err := s.Validate(doc); err != nil {
		return fmt.Errorf("notify: the intent does not follow be.notification.intent.v1: %w", err)
	}
	return nil
}

// ExpiresInTime says whether a deliver intent expires after it occurred (P18.6, what a schema cannot say); a
// withdraw intent has no expiry and always does.
func ExpiresInTime(in Intent) bool {
	if in.Kind != Deliver {
		return true
	}
	at, err1 := time.Parse(time.RFC3339Nano, in.OccurredAt)
	exp, err2 := time.Parse(time.RFC3339Nano, in.ExpiresAt)
	return err1 == nil && err2 == nil && exp.After(at)
}

// SchemaPath is the contract in the protocol module.
const SchemaPath = "schemas/notification/intent.v1.schema.json"

var schema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	raw, err := protocol.Schemas.ReadFile(SchemaPath)
	if err != nil {
		return nil, err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource(SchemaPath, doc); err != nil {
		return nil, err
	}
	return c.Compile(SchemaPath)
})
