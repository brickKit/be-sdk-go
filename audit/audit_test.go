package audit

import (
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/records"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChangesKeepIntegers: a record's state often holds integers (a version, a minor unit); they are allowed in the
// canonical encoding and must survive the round trip, while a fraction is refused (decimals are strings).
func TestChangesKeepIntegersAndLeaveOutNulls(t *testing.T) {
	type state struct {
		Code      string  `json:"code"`
		MinorUnit int     `json:"minor_unit"`
		Version   int64   `json:"version"`
		Rate      string  `json:"rate"`
		CashUnit  *string `json:"cash_unit"` // nil: the member is left out
	}
	r := Record{Category: BusinessChange, Operation: "demo.currency.manage", Outcome: Succeeded,
		Actor:   "0190f3a1-0000-7000-8000-0000000000a1",
		Changes: map[string]any{"after": state{Code: "KWD", MinorUnit: 3, Version: 2, Rate: "7.65"}}}
	b, err := body(r, "0190f3a1-0000-7000-8000-0000000000c1", time.Now())
	require.NoError(t, err)
	raw, _, err := records.Seal("be.audit.record", b)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"minor_unit":3`)
	assert.Contains(t, string(raw), `"version":2`)
	assert.NotContains(t, string(raw), "cash_unit", "a null member is left out")

	r.Changes = map[string]any{"after": map[string]any{"ratio": 0.5}}
	b, err = body(r, "0190f3a1-0000-7000-8000-0000000000c2", time.Now())
	require.NoError(t, err)
	_, _, err = records.Seal("be.audit.record", b)
	assert.Error(t, err, "a fraction is not in the canonical encoding")
}

// P14.3: the target carries the version after the change when the object has one.
func TestTargetOf(t *testing.T) {
	type order struct {
		ID      string `json:"id"`
		Version int64  `json:"version"`
	}
	type ptr struct {
		Version *int64 `json:"version"`
	}
	seven := int64(7)
	assert.Equal(t, map[string]any{"type": "o", "id": "1", "version": "3"}, TargetOf("o", "1", order{ID: "1", Version: 3}))
	assert.Equal(t, map[string]any{"type": "o", "id": "1", "version": "7"}, TargetOf("o", "1", &ptr{Version: &seven}))
	assert.Equal(t, map[string]any{"type": "o", "id": "1", "version": "9"}, TargetOf("o", "1", map[string]any{"version": 9}))
	assert.Equal(t, map[string]any{"type": "o", "id": "1"}, TargetOf("o", "1", ptr{}), "no version")
	assert.Equal(t, map[string]any{"type": "o", "id": "1"}, TargetOf("o", "1", nil))
	assert.Equal(t, map[string]any{"type": "o", "id": "1"}, TargetOf("o", "1", map[string]any{"version": "v2"}), "not a number")
}

// P14.5: the issued number in a business change's audit.
func TestDocumentNumber(t *testing.T) {
	assert.Equal(t, map[string]any{"legal_entity": "le-1", "code": "INV", "period": "2026", "number": "42"},
		DocumentNumber(db.Series{LegalEntity: "le-1", Code: "INV", Period: "2026"}, 42))
}
