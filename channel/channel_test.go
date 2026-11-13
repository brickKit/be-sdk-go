package channel

import (
	"encoding/json"
	"io/fs"
	"testing"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Take answers every case of be-protocol's vector channel-action (P20.7, P20.8).
func TestTheProtocolsVector(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/channel-action.json")
	require.NoError(t, err)
	var v struct {
		Cases []struct {
			ID     string
			Case   Arrival
			Expect Outcome
		}
	}
	require.NoError(t, json.Unmarshal(raw, &v))
	require.NotEmpty(t, v.Cases)
	for _, c := range v.Cases {
		assert.Equal(t, c.Expect, Take(c.Case), c.ID)
	}
}

// The event types derive from the component's identity (P18.2): a "-" in a name becomes "_".
func TestTheEventTypes(t *testing.T) {
	assert.Equal(t, "integration.im_dingtalk_action.recorded.v1", ActionType("integration/im-dingtalk"))
	assert.Equal(t, "integration.im_dingtalk_delivery.recorded.v1", DeliveryType("integration/im-dingtalk"))
}

// The payloads follow their schemas; a delivered result does not exist (P20.2).
func TestThePayloads(t *testing.T) {
	a := Action{ID: "0190f3a1-0000-7000-8000-000000000001", Channel: "integration/im-dingtalk", Audience: "erp/purchase",
		Principal: "u1", GrantID: "0190f3a1-0000-7000-8000-000000000002", Action: "approve", ClickedAt: "2026-11-13T10:00:00Z"}
	raw, _ := json.Marshal(a)
	assert.NoError(t, Validate("action", raw))
	a.GrantID = "a secret"
	raw, _ = json.Marshal(a)
	assert.Error(t, Validate("action", raw))
	d := Delivery{DeliveryID: "0190f3a1-0000-7000-8000-000000000003", Caller: "infra/notification", Sender: "erp/purchase",
		IntentID: "0190f3a1-0000-7000-8000-000000000004", Principal: "u1", Result: "delivered"}
	raw, _ = json.Marshal(d)
	assert.Error(t, Validate("delivery", raw))
}
