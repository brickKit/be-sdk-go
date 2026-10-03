package envelope_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/vectors"
)

func TestNamesVectors(t *testing.T) {
	vectors.Run(t, "envelope", "names", map[string]func(*testing.T, vectors.Case){
		"stream": func(t *testing.T, c vectors.Case) {
			var in struct {
				Subject string `json:"subject"`
			}
			mustUnmarshal(t, c.Input, &in)
			stream, filter, err := envelope.Stream(in.Subject)
			requireResult(t, c, err, map[string]any{"stream": stream, "filter": filter})
		},
		"durable": func(t *testing.T, c vectors.Case) {
			var in struct {
				ComponentID string `json:"component_id"`
				Subject     string `json:"subject"`
			}
			mustUnmarshal(t, c.Input, &in)
			durable, dlq, err := envelope.Durable(in.ComponentID, in.Subject)
			requireResult(t, c, err, map[string]any{"durable": durable, "dlq_subject": dlq})
		},
	})
}

func TestValidComponentID(t *testing.T) {
	for _, ok := range []string{"erp/finance", "integration/im-dingtalk", "conformance/widget-go", "a/b1"} {
		if err := envelope.ValidComponentID(ok); err != nil {
			t.Errorf("%q rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "erp", "erp.finance", "erp_x/finance", "Erp/finance", "erp/", "/x", "erp/fi/nance", "1erp/x", "erp/-x"} {
		if got := envelope.ReasonOf(envelope.ValidComponentID(bad)); got != envelope.ReasonComponentInvalid {
			t.Errorf("%q: reason %q, want COMPONENT_INVALID", bad, got)
		}
	}
}

func TestProtocolConstants(t *testing.T) {
	if envelope.DLQStream != "BE_DLQ" || envelope.DLQFilter != "dlq.>" {
		t.Fatalf("dead-letter stream %s %s", envelope.DLQStream, envelope.DLQFilter)
	}
	if envelope.AckWait != 30*time.Second || envelope.MaxAckPending != 256 || envelope.ServerMaxDeliver != -1 {
		t.Fatalf("consumer constants %v %d %d", envelope.AckWait, envelope.MaxAckPending, envelope.ServerMaxDeliver)
	}
	if envelope.InactiveThreshold != 30*24*time.Hour || envelope.HopLimit != 10 || envelope.DefaultMaxDeliver != 8 {
		t.Fatalf("constants %v %d %d", envelope.InactiveThreshold, envelope.HopLimit, envelope.DefaultMaxDeliver)
	}
	want := []time.Duration{time.Second, 10 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute, time.Hour}
	got := envelope.DefaultBackoff()
	if len(got) != len(want) {
		t.Fatalf("DefaultBackoff %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("DefaultBackoff %v", got)
		}
	}
	got[0] = time.Hour // a caller may not change the defaults of the next caller
	if envelope.DefaultBackoff()[0] != time.Second {
		t.Fatal("DefaultBackoff returned shared state")
	}
}

// Durable names are injective: the name splits back at every "__" into the component id and the
// subject (P12.5).
func TestDurableNamesSplitBack(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		component := genComponentID().Draw(t, "component")
		subject := genSubject().Draw(t, "subject")
		durable, dlq, err := envelope.Durable(component, subject)
		if err != nil {
			t.Fatalf("Durable(%q, %q): %v", component, subject, err)
		}
		parts := strings.Split(durable, "__")
		gotComponent := strings.Replace(parts[0], "_", "/", 1)
		gotSubject := strings.Join(parts[1:], ".")
		if gotComponent != component || gotSubject != subject {
			t.Fatalf("%q split back to %q %q", durable, gotComponent, gotSubject)
		}
		if dlq != "dlq."+durable+"."+subject {
			t.Fatalf("dlq subject %q", dlq)
		}
	})
}

func genSegment() *rapid.Generator[string] {
	return rapid.StringMatching(`[a-z][a-z0-9]{0,3}(_[a-z0-9]{1,3}){0,2}`)
}

func genComponentID() *rapid.Generator[string] {
	return rapid.StringMatching(`[a-z][a-z0-9-]{0,6}/[a-z][a-z0-9-]{0,6}`)
}

func genSubject() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		segs := rapid.SliceOfN(genSegment(), 3, 5).Draw(t, "segments")
		version := rapid.IntRange(1, 99).Draw(t, "version")
		return strings.Join(segs, ".") + ".v" + strconv.Itoa(version)
	})
}

// requireResult checks a vector case that expects either a result object or an error reason.
func requireResult(t *testing.T, c vectors.Case, err error, got any) {
	t.Helper()
	if err != nil {
		vectors.RequireReason(t, c, envelope.ReasonOf(err))
		return
	}
	if c.ExpectedError != nil {
		t.Fatalf("%s: expected %s, got %v", c.Description, c.ExpectedError.Reason, got)
	}
	vectors.RequireJSON(t, c, got)
}
