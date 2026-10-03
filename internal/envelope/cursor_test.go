package envelope_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/vectors"
)

func TestCursorVectors(t *testing.T) {
	vectors.Run(t, "envelope", "cursor", map[string]func(*testing.T, vectors.Case){
		"cursor_sequence": func(t *testing.T, c vectors.Case) {
			var in struct {
				Start    *int64  `json:"start"`
				Versions []int64 `json:"versions"`
			}
			mustUnmarshal(t, c.Input, &in)
			start := envelope.Cursor{}
			if in.Start != nil {
				start = envelope.Cursor{Version: *in.Start, Exists: true}
			}
			applied, final := deliver(start, in.Versions)
			var finalJSON any
			if final.Exists {
				finalJSON = final.Version
			}
			vectors.RequireJSON(t, c, map[string]any{"applied": applied, "final": finalJSON})
		},
		"redelivery": runRedeliveryCase,
	})
}

func runRedeliveryCase(t *testing.T, c vectors.Case) {
	var in struct {
		Delivery   int      `json:"delivery"`
		MaxDeliver int      `json:"max_deliver"`
		Backoff    []string `json:"backoff"`
		Outcome    string   `json:"outcome"`
		Durable    string   `json:"durable"`
		StreamSeq  uint64   `json:"stream_seq"`
	}
	mustUnmarshal(t, c.Input, &in)
	backoff := make([]time.Duration, len(in.Backoff))
	for i, s := range in.Backoff {
		d, err := time.ParseDuration(s)
		if err != nil {
			t.Fatalf("backoff %q: %v", s, err)
		}
		backoff[i] = d
	}
	outcomes := map[string]envelope.Outcome{"ok": envelope.OutcomeOK, "error": envelope.OutcomeError, "permanent": envelope.OutcomePermanent}
	outcome, ok := outcomes[in.Outcome]
	if !ok {
		t.Fatalf("unknown outcome %q", in.Outcome)
	}
	a := envelope.Redelivery(in.Delivery, in.MaxDeliver, backoff, outcome, in.Durable, in.StreamSeq)
	got := map[string]any{"handled": a.Handled}
	switch a.Kind {
	case envelope.ActionAck:
		got["action"] = "ack"
	case envelope.ActionNak:
		got["action"], got["delay"] = "nak", compactDuration(a.Delay)
	case envelope.ActionDeadLetter:
		got["action"], got["reason"], got["dlq_msg_id"] = "dlq", a.Reason, a.DLQMsgID
	}
	vectors.RequireJSON(t, c, got)
}

// deliver folds versions through the state-mode cursor, returning the versions that ran the handler.
func deliver(c envelope.Cursor, versions []int64) ([]int64, envelope.Cursor) {
	applied := []int64{}
	for _, v := range versions {
		var ok bool
		if c, ok = c.Advance(v); ok {
			applied = append(applied, v)
		}
	}
	return applied, c
}

// compactDuration writes a duration the way the vectors do: 1h, 5m, 10s, 200ms.
func compactDuration(d time.Duration) string {
	for _, u := range []struct {
		unit time.Duration
		name string
	}{{time.Hour, "h"}, {time.Minute, "m"}, {time.Second, "s"}, {time.Millisecond, "ms"}} {
		if d%u.unit == 0 {
			return fmt.Sprintf("%d%s", d/u.unit, u.name)
		}
	}
	return d.String()
}

// Any shuffle with duplicates of a version sequence ends in the same final state as one in-order
// delivery (P12.6).
func TestCursorShuffleWithDuplicatesEndsInOrderState(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.Int64Range(1, 30).Draw(t, "n")
		var start envelope.Cursor
		if rapid.Bool().Draw(t, "has-start") {
			start = envelope.Cursor{Version: rapid.Int64Range(0, 40).Draw(t, "start"), Exists: true}
		}
		inOrder := make([]int64, 0, n)
		for v := int64(1); v <= n; v++ {
			inOrder = append(inOrder, v)
		}
		dups := rapid.SliceOf(rapid.Int64Range(1, n)).Draw(t, "duplicates")
		shuffled := rapid.Permutation(append(slices.Clone(inOrder), dups...)).Draw(t, "shuffled")

		_, want := deliver(start, inOrder)
		applied, got := deliver(start, shuffled)
		if got != want {
			t.Fatalf("final %+v, in order %+v", got, want)
		}
		if !slices.IsSorted(applied) || len(slices.Compact(slices.Clone(applied))) != len(applied) {
			t.Fatalf("applied versions not strictly increasing: %v", applied)
		}
	})
}

func TestMaxDeliverReached(t *testing.T) {
	cases := []struct {
		delivery, max int
		want          bool
	}{{8, 8, false}, {9, 8, true}, {1, 1, false}, {2, 1, true}, {8, 0, false}, {9, 0, true}, {9, -1, true}}
	for _, c := range cases {
		if got := envelope.MaxDeliverReached(c.delivery, c.max); got != c.want {
			t.Errorf("MaxDeliverReached(%d, %d) = %v", c.delivery, c.max, got)
		}
	}
}

func TestRedeliveryDefaults(t *testing.T) {
	a := envelope.Redelivery(2, 0, nil, envelope.OutcomeError, "d", 1)
	if a.Kind != envelope.ActionNak || a.Delay != 10*time.Second {
		t.Fatalf("defaults not applied: %+v", a)
	}
	a = envelope.Redelivery(9, 0, nil, envelope.OutcomeOK, "d", 7)
	if a.Kind != envelope.ActionDeadLetter || a.Reason != envelope.ReasonMaxDeliver || a.Handled || a.DLQMsgID != "dlq:d:7" {
		t.Fatalf("default max deliver not applied: %+v", a)
	}
}
