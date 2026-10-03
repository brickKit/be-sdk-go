package envelope_test

import (
	"testing"

	"github.com/brickKit/be-sdk-go/internal/envelope"
)

// stage-B ruling: a subscription's aggregate type is optional; when the contract does not give it,
// the consumer takes the inbound ce-aggregatetype (any well-formed one) and keys its cursor by it.
func TestAcceptWithoutAggregateTypeFallsBackToTheHeader(t *testing.T) {
	s, h, p := validInbound()
	s.AggregateType = ""
	h["ce-aggregatetype"] = "conformance.widget.gadget"
	d, err := envelope.Accept(s, h, p, 1)
	if err != nil || d.DeadLetter || d.Event.AggregateType != "conformance.widget.gadget" {
		t.Fatalf("%+v %v", d, err)
	}
	h["ce-aggregatetype"] = "Not-Well-Formed"
	if d, _ := envelope.Accept(s, h, p, 1); !d.DeadLetter || d.AddedHeaders["be-dlq-reason"] != envelope.ReasonEnvelopeInvalid {
		t.Fatalf("a malformed header is still refused: %+v", d)
	}
}
