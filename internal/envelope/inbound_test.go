package envelope_test

import (
	"maps"
	"testing"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/vectors"
)

func TestInboundVectors(t *testing.T) {
	vectors.Run(t, "envelope", "inbound", map[string]func(*testing.T, vectors.Case){
		"accept": func(t *testing.T, c vectors.Case) {
			var in struct {
				Subscription struct {
					ComponentID         string `json:"component_id"`
					Subject             string `json:"subject"`
					AggregateType       string `json:"aggregate_type"`
					TransactionDocument bool   `json:"transaction_document"`
				} `json:"subscription"`
				Headers     map[string]string `json:"headers"`
				PayloadJSON string            `json:"payload_json"`
				Delivery    int               `json:"delivery"`
			}
			mustUnmarshal(t, c.Input, &in)
			s := envelope.Subscription{
				ComponentID: in.Subscription.ComponentID, Subject: in.Subscription.Subject,
				AggregateType: in.Subscription.AggregateType, TransactionDocument: in.Subscription.TransactionDocument,
			}
			d, err := envelope.Accept(s, in.Headers, []byte(in.PayloadJSON), in.Delivery)
			if err != nil {
				t.Fatalf("Accept: %v", err)
			}
			vectors.RequireJSON(t, c, decisionJSON(d))
		},
	})
}

// decisionJSON renders a Decision in the vector's shape.
func decisionJSON(d envelope.Decision) map[string]any {
	if d.DeadLetter {
		return map[string]any{"action": "dlq", "dlq_subject": d.DLQSubject, "added_headers": d.AddedHeaders}
	}
	e := d.Event
	return map[string]any{"action": "handle", "event": map[string]any{
		"id": e.ID, "subject": e.Subject, "source": e.Source, "aggregate_type": e.AggregateType,
		"aggregate_id": e.AggregateID, "version": e.Version, "hop_count": e.HopCount,
		"causation_id": e.CausationID, "occurred_at": envelope.FormatTime(e.OccurredAt),
		"legal_entity": e.LegalEntity, "delivery": e.Delivery,
	}}
}

func validInbound() (envelope.Subscription, map[string]string, []byte) {
	s := envelope.Subscription{ComponentID: "conformance/peer", Subject: "conformance.widget.created.v1", AggregateType: "conformance.widget.widget"}
	h := map[string]string{
		"ce-aggregatetype": "conformance.widget.widget", "ce-aggregateversion": "1",
		"ce-dataschema": "conformance/widget@1.0.0/contracts/events/widget.events.json#conformance.widget.created.v1",
		"ce-hopcount":   "0", "ce-id": "01a0fba0-947b-77cc-9a52-3f1d2e4b5a60", "ce-source": "conformance/widget",
		"ce-specversion": "1.0", "ce-subject": "w1", "ce-time": "2026-10-02T08:00:00.123456Z",
		"ce-type": "conformance.widget.created.v1", "content-type": "application/json",
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "tracestate": "k=v",
	}
	return s, h, []byte(`{"widget_id":"w1"}`)
}

func TestAcceptCarriesTraceContextAndPayload(t *testing.T) {
	s, h, p := validInbound()
	d, err := envelope.Accept(s, h, p, 2)
	if err != nil || d.DeadLetter {
		t.Fatalf("not handled: %+v %v", d, err)
	}
	e := d.Event
	if e.TraceParent != h["traceparent"] || e.TraceState != "k=v" || string(e.Payload) != string(p) || e.Delivery != 2 {
		t.Fatalf("event %+v", e)
	}
}

func TestAcceptIgnoresMalformedTraceParent(t *testing.T) {
	s, h, p := validInbound()
	h["traceparent"] = "garbage"
	d, _ := envelope.Accept(s, h, p, 1)
	if d.DeadLetter || d.Event.TraceParent != "" {
		t.Fatalf("decision %+v", d)
	}
}

func TestAcceptRejectsMalformedHeaders(t *testing.T) {
	cases := map[string]func(h map[string]string){
		"upper-case id":      func(h map[string]string) { h["ce-id"] = "01A0FBA0-947B-77CC-9A52-3F1D2E4B5A60" },
		"bad source":         func(h map[string]string) { h["ce-source"] = "widget" },
		"bad dataschema":     func(h map[string]string) { h["ce-dataschema"] = "https://example.com/x" },
		"empty subject":      func(h map[string]string) { h["ce-subject"] = "" },
		"offset time":        func(h map[string]string) { h["ce-time"] = "2026-10-02T08:00:00+08:00" },
		"bad causation":      func(h map[string]string) { h["ce-causationid"] = "nope" },
		"version overflow":   func(h map[string]string) { h["ce-aggregateversion"] = "99999999999999999999" },
		"hop overflow":       func(h map[string]string) { h["ce-hopcount"] = "99999999999999999999" },
		"missing time":       func(h map[string]string) { delete(h, "ce-time") },
		"missing dataschema": func(h map[string]string) { delete(h, "ce-dataschema") },
	}
	for name, mutate := range cases {
		s, h, p := validInbound()
		mutate(h)
		d, err := envelope.Accept(s, h, p, 1)
		if err != nil || !d.DeadLetter || d.Reason != envelope.ReasonEnvelopeInvalid {
			t.Errorf("%s: decision %+v %v", name, d, err)
		}
	}
}

func TestAcceptRejectsAnInvalidSubscription(t *testing.T) {
	_, h, p := validInbound()
	if _, err := envelope.Accept(envelope.Subscription{ComponentID: "x", Subject: "conformance.widget.created.v1"}, h, p, 1); envelope.ReasonOf(err) != envelope.ReasonComponentInvalid {
		t.Fatalf("err %v", err)
	}
}

func TestDeadLetterHeaders(t *testing.T) {
	_, h, _ := validInbound()
	h["X-Legacy"] = "1"
	h["Nats-Msg-Id"] = h["ce-id"]
	got := envelope.DeadLetterHeaders(h, "conformance_peer__conformance__widget__created__v1", 3, envelope.ReasonHopLimit)
	want := maps.Clone(h)
	delete(want, "X-Legacy")
	delete(want, "Nats-Msg-Id")
	want["be-dlq-consumer"] = "conformance_peer__conformance__widget__created__v1"
	want["be-dlq-delivery"] = "3"
	want["be-dlq-reason"] = "HOP_LIMIT"
	if !maps.Equal(got, want) {
		t.Fatalf("got  %v\nwant %v", got, want)
	}
	if envelope.DLQMsgID("d", 42) != "dlq:d:42" {
		t.Fatalf("DLQMsgID %s", envelope.DLQMsgID("d", 42))
	}
}
