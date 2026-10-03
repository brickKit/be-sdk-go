package envelope

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// Subscription is what Accept checks an inbound message against (P12.5, P12.7, P11.8).
type Subscription struct {
	ComponentID         string // the consuming component, in a shell the member: names the durable
	Subject             string // the subscribed subject: ce-type must equal it
	AggregateType       string // the contract's x-aggregate-type: ce-aggregatetype must equal it; "" = any well-formed one
	TransactionDocument bool   // the contract's x-transaction-document: ce-legalentity is required
}

// Event is an inbound event decoded from its envelope (P12 envelope, P12.7). Payload is the raw
// business JSON object.
type Event struct {
	ID            string // ce-id
	Subject       string // ce-type
	Source        string // ce-source
	AggregateType string // ce-aggregatetype
	AggregateID   string // ce-subject
	Version       int64  // ce-aggregateversion, ≥ 1
	HopCount      int    // ce-hopcount, 0..HopLimit
	CausationID   string // ce-causationid, "" when absent
	OccurredAt    time.Time
	LegalEntity   string // ce-legalentity, "" when absent
	TraceParent   string // "" when absent or malformed (a consumer accepts its absence)
	TraceState    string
	Payload       json.RawMessage
	Delivery      int // the broker's delivery count, 1-based
}

// Decision is what the subscription runtime does with one message (P12.7): run the handler with
// Event, or dead-letter it on DLQSubject with the original headers plus AddedHeaders (see
// DeadLetterHeaders and DLQMsgID).
type Decision struct {
	DeadLetter   bool
	Event        Event             // set when !DeadLetter
	Reason       string            // be-dlq-reason when DeadLetter
	Durable      string            // the subscription's durable name
	DLQSubject   string            // dlq.<durable>.<subject>
	AddedHeaders map[string]string // be-dlq-consumer, be-dlq-delivery, be-dlq-reason when DeadLetter
}

// Accept decides, before any handler runs, whether an inbound message is handled or dead-lettered
// (P12.7, P12.8, P12.14, P11.8; vectors envelope/inbound). Header names are matched exactly in
// lower case; a message with only the old X- headers is never read. The checks, in order:
//  1. the envelope (ENVELOPE_INVALID): every required header present and well formed,
//     ce-specversion 1.0, JSON content type, ce-type = the subscribed subject, ce-aggregatetype =
//     the contract's, decimal integers without sign or leading zero;
//  2. the hop count above HopLimit (HOP_LIMIT);
//  3. the payload is a JSON object (PAYLOAD_INVALID);
//  4. a transaction-document event whose ce-legalentity is missing or differs from the payload's
//     legal_entity_id (LEGAL_ENTITY_MISSING).
//
// The error is non-nil only for an invalid Subscription (COMPONENT_INVALID, SUBJECT_INVALID): a
// configuration fault, not a message fault. The delivery-count check is MaxDeliverReached.
func Accept(s Subscription, headers map[string]string, payload []byte, delivery int) (Decision, error) {
	durable, dlqSubject, err := Durable(s.ComponentID, s.Subject)
	if err != nil {
		return Decision{}, err
	}
	reject := func(reason string) (Decision, error) {
		return Decision{DeadLetter: true, Reason: reason, Durable: durable, DLQSubject: dlqSubject,
			AddedHeaders: DLQAddedHeaders(durable, delivery, reason)}, nil
	}
	ev, err := parseEnvelope(s, headers)
	if err != nil {
		return reject(ReasonEnvelopeInvalid)
	}
	if ev.HopCount > HopLimit {
		return reject(ReasonHopLimit)
	}
	obj, ok := jsonObject(payload)
	if !ok {
		return reject(ReasonPayloadInvalid)
	}
	if s.TransactionDocument && (ev.LegalEntity == "" || stringField(obj, "legal_entity_id") != ev.LegalEntity) {
		return reject(ReasonLegalEntityMissing)
	}
	ev.Payload = json.RawMessage(payload)
	ev.Delivery = delivery
	return Decision{Event: ev, Durable: durable, DLQSubject: dlqSubject}, nil
}

// DLQAddedHeaders returns the headers dead-lettering adds (P12.7): be-dlq-consumer (the durable
// name), be-dlq-delivery (the delivery count) and be-dlq-reason.
func DLQAddedHeaders(durable string, delivery int, reason string) map[string]string {
	return map[string]string{
		HeaderDLQConsumer: durable,
		HeaderDLQDelivery: strconv.Itoa(delivery),
		HeaderDLQReason:   reason,
	}
}

// DeadLetterHeaders returns the headers of a dead-letter message (P12.7): the original ce-*
// headers, content-type and the W3C trace context, plus DLQAddedHeaders. Other headers (Nats-Msg-Id,
// old X- headers) are not copied; the dead-letter message ID is DLQMsgID.
func DeadLetterHeaders(original map[string]string, durable string, delivery int, reason string) map[string]string {
	out := DLQAddedHeaders(durable, delivery, reason)
	for name, value := range original {
		if strings.HasPrefix(name, "ce-") || name == HeaderContentType || name == HeaderTraceParent || name == HeaderTraceState {
			out[name] = value
		}
	}
	return out
}

// DLQMsgID returns the message ID of a dead-letter message, dlq:<durable>:<stream sequence> (P12.7):
// a crash between the dead-letter publish and the Term never duplicates it.
func DLQMsgID(durable string, streamSeq uint64) string {
	return "dlq:" + durable + ":" + strconv.FormatUint(streamSeq, 10)
}

// jsonObject decodes a payload that must be a JSON object.
func jsonObject(payload []byte) (map[string]json.RawMessage, bool) {
	var obj map[string]json.RawMessage
	if !bytes.HasPrefix(bytes.TrimSpace(payload), []byte("{")) || json.Unmarshal(payload, &obj) != nil {
		return nil, false
	}
	return obj, true
}

// stringField returns obj[name] when it is a JSON string, else "".
func stringField(obj map[string]json.RawMessage, name string) string {
	var s string
	if raw, ok := obj[name]; ok {
		_ = json.Unmarshal(raw, &s) // a non-string value leaves s empty
	}
	return s
}
