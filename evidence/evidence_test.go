package evidence

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An intent the canonical form cannot encode reaches neither the store nor the alert channel: the writer logs it at
// error level itself (P12.16, evidence_intent_unsealed), so a caller that drops the error does not make it silent.
func TestAnIntentThatCannotBeEncodedIsLogged(t *testing.T) {
	var logs bytes.Buffer
	store, alert := &stubStore{}, &stubAlert{}
	w := &Writer{Store: store, Alert: alert, Component: "demo/orders", Instance: "orders-1",
		Log: slog.New(slog.NewJSONHandler(&logs, nil))}
	parent := "0190f3a1-0000-7000-8000-0000000000f2"
	p, err := w.Intent(context.Background(), "org_tightening", "", order, map[string]any{"parent_after": &parent})
	require.Error(t, err)
	assert.Nil(t, p)
	assert.Empty(t, store.got, "nothing reached the store")
	assert.Empty(t, alert.got, "nor the alert channel")
	assert.Contains(t, logs.String(), `"level":"ERROR"`)
	assert.Contains(t, logs.String(), `"event":"evidence_intent_unsealed"`)
	assert.Contains(t, logs.String(), `"category":"org_tightening"`)
	assert.Contains(t, logs.String(), order.ID)
}
