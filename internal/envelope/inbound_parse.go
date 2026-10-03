package envelope

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

var errEnvelope = errors.New("envelope invalid")

// parseEnvelope reads and checks the envelope headers of an inbound message against s; any failure
// is errEnvelope (dead-letter reason ENVELOPE_INVALID). Patterns are those of envelope.schema.json.
func parseEnvelope(s Subscription, h map[string]string) (Event, error) {
	var ev Event
	var ok bool
	r := headerReader{h: h}
	if r.get(HeaderSpecVersion) != SpecVersion || r.get(HeaderContentType) != ContentTypeJSON {
		return ev, errEnvelope
	}
	ev.ID = r.matching(HeaderID, idPattern.MatchString)
	ev.Subject = r.matching(HeaderType, func(v string) bool { return v == s.Subject })
	ev.Source = r.matching(HeaderSource, componentPattern.MatchString)
	ev.AggregateType = r.matching(HeaderAggregateType, func(v string) bool { return v == s.AggregateType })
	ev.AggregateID = r.matching(HeaderSubject, func(v string) bool { return v != "" })
	r.matching(HeaderDataSchema, dataSchemaPattern.MatchString)
	ev.Version = r.decimal(HeaderAggregateVersion, 1, 64)
	ev.HopCount = int(r.decimal(HeaderHopCount, 0, 32))
	if ev.OccurredAt, ok = parseUTC(r.get(HeaderTime)); !ok {
		r.bad = true
	}
	if v, present := h[HeaderCausationID]; present && !idPattern.MatchString(v) {
		r.bad = true
	}
	if r.bad {
		return ev, errEnvelope
	}
	ev.CausationID = h[HeaderCausationID]
	ev.LegalEntity = h[HeaderLegalEntity]
	if traceParentPattern.MatchString(h[HeaderTraceParent]) {
		ev.TraceParent, ev.TraceState = h[HeaderTraceParent], h[HeaderTraceState]
	}
	return ev, nil
}

// headerReader reads required headers and remembers whether any was missing or malformed.
type headerReader struct {
	h   map[string]string
	bad bool
}

// get returns a required header; a missing one marks the envelope bad.
func (r *headerReader) get(name string) string {
	v, ok := r.h[name]
	if !ok {
		r.bad = true
	}
	return v
}

// matching returns a required header that must satisfy valid.
func (r *headerReader) matching(name string, valid func(string) bool) string {
	v := r.get(name)
	if !valid(v) {
		r.bad = true
	}
	return v
}

// decimal returns a required decimal integer header without sign or leading zero, at least min,
// that fits a signed integer of bitSize bits.
func (r *headerReader) decimal(name string, min int64, bitSize int) int64 {
	v := r.matching(name, decimalPattern.MatchString)
	n, err := strconv.ParseInt(v, 10, bitSize)
	if err != nil || n < min {
		r.bad = true
	}
	return n
}

// parseUTC parses ce-time: RFC 3339 in UTC with a Z suffix.
func parseUTC(v string) (time.Time, bool) {
	if !strings.HasSuffix(v, "Z") {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	return t.UTC(), err == nil
}
