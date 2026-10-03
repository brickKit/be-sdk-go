package envelope_test

import (
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/vectors"
)

type vectorRow struct {
	ID               string `json:"id"`
	Subject          string `json:"subject"`
	AggregateType    string `json:"aggregate_type"`
	AggregateID      string `json:"aggregate_id"`
	AggregateVersion int64  `json:"aggregate_version"`
	OccurredAt       string `json:"occurred_at"`
	TraceParent      string `json:"traceparent"`
	TraceState       string `json:"tracestate"`
	CausationID      string `json:"causation_id"`
	HopCount         int    `json:"hop_count"`
	PayloadJSON      string `json:"payload_json"`
}

func TestHeadersVectors(t *testing.T) {
	vectors.Run(t, "envelope", "headers", map[string]func(*testing.T, vectors.Case){
		"envelope": func(t *testing.T, c vectors.Case) {
			var in struct {
				Producer struct {
					ComponentID string `json:"component_id"`
					Version     string `json:"version"`
					EventsFile  string `json:"events_file"`
				} `json:"producer"`
				Row      vectorRow `json:"row"`
				Contract struct {
					TransactionDocument bool `json:"transaction_document"`
				} `json:"contract"`
			}
			mustUnmarshal(t, c.Input, &in)
			occurred, err := time.Parse(time.RFC3339Nano, in.Row.OccurredAt)
			if err != nil {
				t.Fatalf("occurred_at: %v", err)
			}
			p := envelope.Producer{ComponentID: in.Producer.ComponentID, Version: in.Producer.Version, EventsFile: in.Producer.EventsFile}
			r := envelope.Row{
				ID: in.Row.ID, Subject: in.Row.Subject, AggregateType: in.Row.AggregateType,
				AggregateID: in.Row.AggregateID, AggregateVersion: in.Row.AggregateVersion, OccurredAt: occurred,
				TraceParent: in.Row.TraceParent, TraceState: in.Row.TraceState, CausationID: in.Row.CausationID, HopCount: in.Row.HopCount,
				Payload: []byte(in.Row.PayloadJSON),
			}
			h, err := envelope.Headers(p, r, in.Contract.TransactionDocument)
			requireResult(t, c, err, map[string]any{"headers": h})
		},
	})
}

func TestFormatTime(t *testing.T) {
	cases := map[string]time.Time{
		"2026-10-02T08:00:00.123456Z": time.Date(2026, 10, 2, 8, 0, 0, 123456789, time.UTC), // at most 6 digits, truncated
		"2026-10-02T08:00:00.1Z":      time.Date(2026, 10, 2, 8, 0, 0, 100000000, time.UTC),
		"2026-10-02T08:00:00Z":        time.Date(2026, 10, 2, 8, 0, 0, 999, time.UTC), // below a microsecond
		"2026-10-01T23:30:00Z":        time.Date(2026, 10, 2, 7, 30, 0, 0, time.FixedZone("CST", 8*3600)),
	}
	for want, in := range cases {
		if got := envelope.FormatTime(in); got != want {
			t.Errorf("FormatTime(%v) = %s, want %s", in, got, want)
		}
	}
}

func TestHeadersRejectsMalformedRows(t *testing.T) {
	p := envelope.Producer{ComponentID: "conformance/widget", Version: "1.0.0", EventsFile: "widget.events.json"}
	good := envelope.Row{
		ID: "01a0fba0-947b-77cc-9a52-3f1d2e4b5a60", Subject: "conformance.widget.created.v1",
		AggregateType: "conformance.widget.widget", AggregateID: "w1", AggregateVersion: 1,
		OccurredAt: time.Date(2026, 10, 2, 8, 0, 0, 0, time.UTC), Payload: []byte(`{}`),
	}
	if _, err := envelope.Headers(p, good, false); err != nil {
		t.Fatalf("good row rejected: %v", err)
	}
	cases := map[string]struct {
		p      envelope.Producer
		mutate func(r *envelope.Row)
		reason string
	}{
		"bad component":      {envelope.Producer{ComponentID: "widget", Version: "1.0.0", EventsFile: "w.events.json"}, func(*envelope.Row) {}, envelope.ReasonComponentInvalid},
		"bad version":        {envelope.Producer{ComponentID: "a/b", Version: "1.0", EventsFile: "w.events.json"}, func(*envelope.Row) {}, envelope.ReasonEnvelopeInvalid},
		"no events file":     {envelope.Producer{ComponentID: "a/b", Version: "1.0.0"}, func(*envelope.Row) {}, envelope.ReasonEnvelopeInvalid},
		"bad aggregate type": {p, func(r *envelope.Row) { r.AggregateType = "widget" }, envelope.ReasonEnvelopeInvalid},
		"no aggregate id":    {p, func(r *envelope.Row) { r.AggregateID = "" }, envelope.ReasonEnvelopeInvalid},
		"negative hop":       {p, func(r *envelope.Row) { r.HopCount = -1 }, envelope.ReasonEnvelopeInvalid},
		"zero time":          {p, func(r *envelope.Row) { r.OccurredAt = time.Time{} }, envelope.ReasonEnvelopeInvalid},
		"bad causation":      {p, func(r *envelope.Row) { r.CausationID = "x" }, envelope.ReasonEnvelopeInvalid},
		"payload array":      {p, func(r *envelope.Row) { r.Payload = []byte(`[1]`) }, envelope.ReasonEnvelopeInvalid},
		"payload not json":   {p, func(r *envelope.Row) { r.Payload = []byte(`{`) }, envelope.ReasonEnvelopeInvalid},
	}
	for name, tc := range cases {
		r := good
		tc.mutate(&r)
		_, err := envelope.Headers(tc.p, r, false)
		if got := envelope.ReasonOf(err); got != tc.reason {
			t.Errorf("%s: reason %q, want %q (%v)", name, got, tc.reason, err)
		}
	}
}

func TestHeadersDropsMalformedTraceParent(t *testing.T) {
	p := envelope.Producer{ComponentID: "a/b", Version: "1.0.0", EventsFile: "b.events.json"}
	r := envelope.Row{
		ID: "01a0fba0-947b-77cc-9a52-3f1d2e4b5a60", Subject: "a.b.created.v1", AggregateType: "a.b",
		AggregateID: "1", AggregateVersion: 1, OccurredAt: time.Unix(0, 0), TraceParent: "garbage", Payload: []byte(`{}`),
	}
	h, err := envelope.Headers(p, r, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := h["traceparent"]; ok {
		t.Fatalf("malformed traceparent kept: %v", h)
	}
}

func TestHeadersNormalisesUpperCaseID(t *testing.T) {
	p := envelope.Producer{ComponentID: "a/b", Version: "1.0.0", EventsFile: "b.events.json"}
	r := envelope.Row{
		ID: "01A0FBA0-947B-77CC-9A52-3F1D2E4B5A60", Subject: "a.b.created.v1", AggregateType: "a.b",
		AggregateID: "1", AggregateVersion: 1, OccurredAt: time.Unix(0, 0), Payload: []byte(`{}`),
	}
	h, err := envelope.Headers(p, r, false)
	if err != nil {
		t.Fatal(err)
	}
	if h["ce-id"] != "01a0fba0-947b-77cc-9a52-3f1d2e4b5a60" || h["Nats-Msg-Id"] != h["ce-id"] {
		t.Fatalf("id not normalised: %v", h)
	}
}
