package envelope

import (
	"encoding/json"
	"strconv"
	"time"
)

// Header names of the envelope (P12 envelope table).
const (
	HeaderSpecVersion      = "ce-specversion"
	HeaderID               = "ce-id"
	HeaderSource           = "ce-source"
	HeaderType             = "ce-type"
	HeaderTime             = "ce-time"
	HeaderSubject          = "ce-subject"
	HeaderContentType      = "content-type"
	HeaderDataSchema       = "ce-dataschema"
	HeaderAggregateType    = "ce-aggregatetype"
	HeaderAggregateVersion = "ce-aggregateversion"
	HeaderCausationID      = "ce-causationid"
	HeaderHopCount         = "ce-hopcount"
	HeaderLegalEntity      = "ce-legalentity"
	HeaderTraceParent      = "traceparent"
	HeaderTraceState       = "tracestate"
	HeaderNatsMsgID        = "Nats-Msg-Id"
	HeaderDLQReason        = "be-dlq-reason"
	HeaderDLQConsumer      = "be-dlq-consumer"
	HeaderDLQDelivery      = "be-dlq-delivery"

	SpecVersion     = "1.0"              // ce-specversion
	ContentTypeJSON = "application/json" // content-type
)

// Producer identifies the publishing component, in a shell the member (P12 envelope: ce-source,
// ce-dataschema).
type Producer struct {
	ComponentID string // ce-source
	Version     string // the component's version, x.y.z
	EventsFile  string // the contract file under contracts/events/, e.g. "sales.events.json"
}

// Row is the part of a besdk_outbox row the envelope is built from (P12.1, P12 envelope).
type Row struct {
	ID               string    // besdk_outbox.id, a UUIDv7: ce-id and Nats-Msg-Id
	Subject          string    // ce-type
	AggregateType    string    // the contract's x-aggregate-type: ce-aggregatetype
	AggregateID      string    // ce-subject
	AggregateVersion int64     // ≥ 1: ce-aggregateversion
	OccurredAt       time.Time // ce-time
	TraceParent      string    // W3C traceparent of the producing span; "" for none
	CausationID      string    // "" from a request (P12.8)
	HopCount         int       // ≥ 0 (P12.8)
	Payload          []byte    // the business JSON object
}

// Headers builds the complete header map of one outbox row (P12 envelope table, vectors
// envelope/headers): ce-time by FormatTime, ce-causationid and traceparent omitted when empty (a
// malformed traceparent is dropped too, a consumer accepts its absence), ce-legalentity from the
// payload's legal_entity_id when that is a non-empty string, Nats-Msg-Id = ce-id. txDocument is the
// contract's x-transaction-document: such an event without legal_entity_id is LEGAL_ENTITY_MISSING
// (P11.8). Other invalid rows are ID_INVALID, SUBJECT_INVALID, COMPONENT_INVALID or ENVELOPE_INVALID.
func Headers(p Producer, r Row, txDocument bool) (map[string]string, error) {
	id, err := ParseID(r.ID)
	if err != nil {
		return nil, err
	}
	r.ID = id.String() // the canonical lower-case form (P11.5)
	if err := checkRow(p, r); err != nil {
		return nil, err
	}
	legalEntity, err := payloadLegalEntity(r.Payload)
	if err != nil {
		return nil, err
	}
	if txDocument && legalEntity == "" {
		return nil, fail(ReasonLegalEntityMissing, "transaction-document event without legal_entity_id (P11.8)")
	}
	h := map[string]string{
		HeaderSpecVersion:      SpecVersion,
		HeaderID:               r.ID,
		HeaderNatsMsgID:        r.ID,
		HeaderSource:           p.ComponentID,
		HeaderType:             r.Subject,
		HeaderTime:             FormatTime(r.OccurredAt),
		HeaderSubject:          r.AggregateID,
		HeaderContentType:      ContentTypeJSON,
		HeaderDataSchema:       p.ComponentID + "@" + p.Version + "/contracts/events/" + p.EventsFile + "#" + r.Subject,
		HeaderAggregateType:    r.AggregateType,
		HeaderAggregateVersion: strconv.FormatInt(r.AggregateVersion, 10),
		HeaderHopCount:         strconv.Itoa(r.HopCount),
	}
	setIf(h, HeaderCausationID, r.CausationID)
	setIf(h, HeaderLegalEntity, legalEntity)
	if traceParentPattern.MatchString(r.TraceParent) {
		h[HeaderTraceParent] = r.TraceParent
	}
	return h, nil
}

// FormatTime formats ce-time (P12 envelope, vectors envelope/headers): RFC 3339 in UTC with Z,
// fractional seconds only when non-zero, trailing zeros removed, at most 6 digits (truncated).
func FormatTime(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.999999Z07:00")
}

// checkRow validates everything except the id and the legal entity: subject and component first
// (their own error classes), then the fields that are ENVELOPE_INVALID.
func checkRow(p Producer, r Row) error {
	if err := ValidSubject(r.Subject); err != nil {
		return err
	}
	if err := ValidComponentID(p.ComponentID); err != nil {
		return err
	}
	switch {
	case !versionPattern.MatchString(p.Version):
		return fail(ReasonEnvelopeInvalid, "producer version "+strconv.Quote(p.Version)+" is not x.y.z")
	case !eventsFilePattern.MatchString(p.EventsFile):
		return fail(ReasonEnvelopeInvalid, "events file "+strconv.Quote(p.EventsFile)+" is not a file name")
	case !aggregateTypePattern.MatchString(r.AggregateType):
		return fail(ReasonEnvelopeInvalid, "aggregate type "+strconv.Quote(r.AggregateType)+" is malformed")
	case r.AggregateID == "":
		return fail(ReasonEnvelopeInvalid, "aggregate id is empty")
	case r.AggregateVersion < 1:
		return fail(ReasonEnvelopeInvalid, "aggregate versions start at 1")
	case r.HopCount < 0:
		return fail(ReasonEnvelopeInvalid, "hop count is negative")
	case r.OccurredAt.IsZero():
		return fail(ReasonEnvelopeInvalid, "occurred_at is not set")
	case r.CausationID != "" && !idPattern.MatchString(r.CausationID):
		return fail(ReasonEnvelopeInvalid, "causation id is not a lower-case UUIDv7")
	}
	return nil
}

// payloadLegalEntity returns the payload's legal_entity_id when it is a non-empty string, "" when it
// is absent, empty or not a string; a payload that is not a JSON object is ENVELOPE_INVALID.
func payloadLegalEntity(payload []byte) (string, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil || obj == nil {
		return "", fail(ReasonEnvelopeInvalid, "payload is not a JSON object")
	}
	var le string
	if raw, ok := obj["legal_entity_id"]; ok {
		_ = json.Unmarshal(raw, &le) // a non-string value leaves le empty
	}
	return le, nil
}

func setIf(h map[string]string, name, value string) {
	if value != "" {
		h[name] = value
	}
}
