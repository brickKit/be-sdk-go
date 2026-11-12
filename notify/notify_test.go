package notify

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/events"
)

type emitted struct {
	stream string
	e      events.Event
}

type recorder struct{ got []emitted }

func (r *recorder) Emit(_ context.Context, _ db.Tx, stream string, e events.Event) (string, error) {
	r.got = append(r.got, emitted{stream, e})
	return "1", nil
}

func deliver() Intent {
	return Intent{Sender: "erp/purchase", Category: ApprovalTask, Kind: Deliver,
		Recipients: []Recipient{{Principal: "u1"}}, Title: "PO-1", Link: "/workflow/tasks/x",
		ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), CollapseKey: "task:x", CollapseSeq: 2}
}

// P18.2: on the sender's own stream, its intent type, the intent as the payload; the ID and the moment filled in.
func TestAnIntentGoesOnTheSendersStream(t *testing.T) {
	r := &recorder{}
	in, err := Emit(context.Background(), db.Tx{}, r, deliver())
	require.NoError(t, err)
	require.Len(t, r.got, 1)
	assert.Equal(t, "erp.purchase", r.got[0].stream)
	assert.Equal(t, "erp.purchase_notification.intent.v1", r.got[0].e.Type)
	assert.Equal(t, in.ID, r.got[0].e.RecordID)
	assert.NotEmpty(t, in.OccurredAt)
	b, _ := json.Marshal(r.got[0].e.Data)
	assert.Contains(t, string(b), `"collapse_seq":2`)
}

// P18.1, P18.6: what does not follow the contract is not emitted.
func TestAnIntentOutsideTheContractIsNotEmitted(t *testing.T) {
	for name, change := range map[string]func(*Intent){
		"no recipient":                 func(i *Intent) { i.Recipients = nil },
		"an unknown category":          func(i *Intent) { i.Category = "newsletter" },
		"a link outside the system":    func(i *Intent) { i.Link = "https://example.com" },
		"no title":                     func(i *Intent) { i.Title = "" },
		"a title too long":             func(i *Intent) { i.Title = strings.Repeat("x", 201) },
		"expires before it occurred":   func(i *Intent) { i.ExpiresAt = "2000-01-01T00:00:00Z" },
		"a merge key with no sequence": func(i *Intent) { i.CollapseSeq = 0 },
		"a withdraw with recipients":   func(i *Intent) { i.Kind = Withdraw },
		"no sender":                    func(i *Intent) { i.Sender = "" },
	} {
		in := deliver()
		change(&in)
		r := &recorder{}
		_, err := Emit(context.Background(), db.Tx{}, r, in)
		assert.Error(t, err, name)
		assert.Empty(t, r.got, name)
	}
}

// P18.7: a withdraw intent carries only its merge key and sequence, and no expiry.
func TestAWithdrawIntent(t *testing.T) {
	r := &recorder{}
	_, err := Emit(context.Background(), db.Tx{}, r, Intent{Sender: "erp/purchase", Category: ApprovalTask, Kind: Withdraw,
		CollapseKey: "task:x", CollapseSeq: 3})
	require.NoError(t, err)
	assert.Len(t, r.got, 1)
}
