package envelope_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/vectors"
)

func TestIDsVectors(t *testing.T) {
	vectors.Run(t, "envelope", "ids", map[string]func(*testing.T, vectors.Case){
		"uuid7": func(t *testing.T, c vectors.Case) {
			var in struct {
				ID string `json:"id"`
			}
			mustUnmarshal(t, c.Input, &in)
			id, err := envelope.ParseID(in.ID)
			if err != nil {
				vectors.RequireReason(t, c, envelope.ReasonOf(err))
				return
			}
			if c.ExpectedError != nil {
				t.Fatalf("%s: expected %s, got %s", c.Description, c.ExpectedError.Reason, id)
			}
			at := envelope.IDTime(id)
			vectors.RequireJSON(t, c, map[string]any{
				"canonical":  id.String(),
				"unix_ms":    at.UnixMilli(),
				"created_at": at.Format("2006-01-02T15:04:05.999Z07:00"),
			})
		},
	})
}

func TestNewIDIsAValidV7NearNow(t *testing.T) {
	before := time.Now().Truncate(time.Millisecond)
	id := envelope.NewID()
	after := time.Now()
	parsed, err := envelope.ParseID(id.String())
	if err != nil {
		t.Fatalf("NewID gave an id ParseID rejects: %v", err)
	}
	if parsed != id {
		t.Fatalf("round trip changed the id: %s -> %s", id, parsed)
	}
	at := envelope.IDTime(id)
	if at.Before(before) || at.After(after) {
		t.Fatalf("IDTime %v not within [%v, %v]", at, before, after)
	}
	if at.Location() != time.UTC {
		t.Fatalf("IDTime location %v, want UTC", at.Location())
	}
}

func TestNewIDsIncrease(t *testing.T) {
	prev := envelope.NewID()
	for range 1000 {
		next := envelope.NewID()
		if next.String() <= prev.String() {
			t.Fatalf("ids not increasing: %s then %s", prev, next)
		}
		prev = next
	}
}

func mustUnmarshal(t *testing.T, raw json.RawMessage, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("decode input: %v", err)
	}
}
