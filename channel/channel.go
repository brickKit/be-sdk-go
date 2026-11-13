// Package channel is what the channel members and the owner components share (be-protocol chapter 20): the action
// event a channel member publishes on its own stream when a principal acts on a grant (P20.6), the delivery result
// it publishes when a delivery's external command ends (P20.2), the event types derived from a component's identity,
// and Take, the owner's decision on an arriving action (P20.7, P20.8; the vector channel-action). The delivery's
// system surface is channelpb (be.channel.v1.ChannelService).
package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/notify"
)

// The event types after a channel member's stream (P20.2, P20.6).
const (
	ActionSuffix   = "_action.recorded.v1"
	DeliverySuffix = "_delivery.recorded.v1"
)

// The delivery results (P20.2): sent is as far as a channel can confirm, never "delivered".
const (
	Sent     = "sent"
	Failed   = "failed"
	Rejected = "rejected"
	Expired  = "expired"
)

// ActionType and DeliveryType are a channel member's event types: integration/im-dingtalk publishes
// integration.im_dingtalk_action.recorded.v1 on integration.im_dingtalk.
func ActionType(component string) string   { return notify.Stream(component) + ActionSuffix }
func DeliveryType(component string) string { return notify.Stream(component) + DeliverySuffix }

// Action is the action event (schemas/channel/action.v1): a principal acted on a grant of the audience through the
// channel member. The grant is not a secret; the button's one-time value never leaves the channel member.
type Action struct {
	ID        string `json:"action_id"`
	Channel   string `json:"channel"`
	Audience  string `json:"audience"`
	Principal string `json:"principal"`
	GrantID   string `json:"grant_id"`
	Action    string `json:"action"`
	ClickedAt string `json:"clicked_at"`
}

// Delivery is the delivery result (schemas/channel/delivery.v1).
type Delivery struct {
	DeliveryID string `json:"delivery_id"`
	Caller     string `json:"caller"`
	Sender     string `json:"sender"`
	IntentID   string `json:"intent_id"`
	Principal  string `json:"principal"`
	Result     string `json:"result"`
}

// EmitAction checks an action against its schema and emits it on the channel member's stream in tx (P20.6).
func EmitAction(ctx context.Context, tx db.Tx, em notify.Emitter, a Action) error {
	raw, _ := json.Marshal(a)
	if err := Validate("action", raw); err != nil {
		return err
	}
	_, err := em.Emit(ctx, tx, notify.Stream(a.Channel), events.Event{Type: ActionType(a.Channel), RecordID: a.ID,
		Version: 1, Projection: events.Full, Data: a})
	return err
}

// EmitDelivery checks a delivery result and emits it on the channel member's stream in tx (P20.2); channel is the
// member's own identity.
func EmitDelivery(ctx context.Context, tx db.Tx, em notify.Emitter, channel string, d Delivery) error {
	raw, _ := json.Marshal(d)
	if err := Validate("delivery", raw); err != nil {
		return err
	}
	_, err := em.Emit(ctx, tx, notify.Stream(channel), events.Event{Type: DeliveryType(channel), RecordID: d.DeliveryID,
		Version: 1, Projection: events.Full, Data: d})
	return err
}

// Validate checks a payload against schemas/channel/<name>.v1 (action or delivery).
func Validate(name string, raw []byte) error {
	s, err := schemas()
	if err != nil {
		return err
	}
	sc, ok := s[name]
	if !ok {
		return fmt.Errorf("channel: no schema %q", name)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("channel: the %s is not JSON: %w", name, err)
	}
	if err := sc.Validate(doc); err != nil {
		return fmt.Errorf("channel: the %s does not follow be.channel.%s.v1: %w", name, name, err)
	}
	return nil
}

var schemas = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) {
	out := map[string]*jsonschema.Schema{}
	for _, name := range []string{"action", "delivery"} {
		path := "schemas/channel/" + name + ".v1.schema.json"
		raw, err := protocol.Schemas.ReadFile(path)
		if err != nil {
			return nil, err
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		c := jsonschema.NewCompiler()
		c.AssertFormat()
		if err := c.AddResource(path, doc); err != nil {
			return nil, err
		}
		if out[name], err = c.Compile(path); err != nil {
			return nil, err
		}
	}
	return out, nil
})

// The kernel's decision for the grant's principal, as Take reads it after the action's own checks.
const (
	Approved             = "approved"
	ViewUnprovable       = "view_unprovable"
	QualificationUnknown = "qualification_unknown"
	DatabaseError        = "database_error"
)

// What a grant already has recorded (P20.8).
const (
	PriorNone        = "none"
	PriorSameAction  = "same_action"
	PriorOtherAction = "other_action"
)

// Arrival is an arriving action as the owner sees it (P20.7, P20.8): the vector channel-action's case.
type Arrival struct {
	ShapeOK              bool   `json:"shape_ok"`
	AudienceIsSelf       bool   `json:"audience_is_self"`
	StreamIsSource       bool   `json:"stream_is_source"`
	GrantExists          bool   `json:"grant_exists"`
	PrincipalMatches     bool   `json:"principal_matches"`
	ActionAllowed        bool   `json:"action_allowed"`
	Prior                string `json:"prior"`
	ClickedInTime        bool   `json:"clicked_in_time"`
	ProcessedInTime      bool   `json:"processed_in_time"`
	SecurityVersionNewer bool   `json:"security_version_newer"`
	Decision             string `json:"decision"`
}

// The answers (P20.7, P20.8).
const (
	Ignored  = "ignored"  // another owner's action: acknowledged silently
	Refused  = "refused"  // with Reason; audited
	Replayed = "replayed" // the grant's first result answers again
	Retry    = "retry"    // nothing recorded: the event is nacked and delivered again
	Executed = "executed"
)

// Outcome is what comes of an arriving action: the answer, the reason of a refusal or a retry, whether a security
// record is written, whether the result is recorded on the grant, whether the holder gets an in-app intent saying
// the action was not carried out.
type Outcome struct {
	Answer       string  `json:"answer"`
	Reason       *string `json:"reason"`
	Audit        bool    `json:"audit"`
	Record       bool    `json:"record"`
	NotifyHolder bool    `json:"notify_holder"`
}

// Take is the owner's decision on an arriving action, the checks in order, first failing decides (P20.7, P20.8).
// Decision is only read when every check before it passes: the caller runs the kernel's decision then, and calls
// Take again with its result.
func Take(a Arrival) Outcome {
	refuse := func(reason string, record, notify bool) Outcome {
		return Outcome{Answer: Refused, Reason: &reason, Audit: true, Record: record, NotifyHolder: notify}
	}
	switch {
	case !a.ShapeOK:
		return refuse("malformed", false, false)
	case !a.AudienceIsSelf:
		return Outcome{Answer: Ignored}
	case !a.StreamIsSource:
		return refuse("not_the_sender", false, false)
	case !a.GrantExists:
		return refuse("unknown_grant", false, false)
	case !a.PrincipalMatches:
		return refuse("not_the_holder", false, false)
	case !a.ActionAllowed:
		return refuse("action_not_allowed", false, false)
	case a.Prior == PriorSameAction:
		return Outcome{Answer: Replayed}
	case a.Prior == PriorOtherAction:
		return refuse("GRANT_USED", false, true)
	case !a.ClickedInTime || !a.ProcessedInTime:
		return refuse("expired", true, true)
	case a.SecurityVersionNewer:
		return refuse("GRANT_STALE", true, true)
	}
	switch a.Decision {
	case Approved:
		return Outcome{Answer: Executed, Record: true}
	case ViewUnprovable, QualificationUnknown, DatabaseError:
		r := a.Decision
		return Outcome{Answer: Retry, Reason: &r}
	}
	return refuse(a.Decision, true, true)
}
