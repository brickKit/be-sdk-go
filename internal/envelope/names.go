package envelope

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Dead-letter stream (P12.4): stream BE_DLQ holds subjects dlq.>, kept 30 days.
const (
	DLQStream    = "BE_DLQ"
	DLQFilter    = "dlq.>"
	DLQRetention = 30 * 24 * time.Hour
)

// Durable consumer constants (P12.5, P12.8). The broker is configured with ServerMaxDeliver and no
// backoff; the runtime counts deliveries itself against EVENTS_MAX_DELIVER (DefaultMaxDeliver) and
// naks with EVENTS_BACKOFF (DefaultBackoff), see Redelivery.
const (
	AckWait           = 30 * time.Second    // P12.5: the real value, never inflated
	MaxAckPending     = 256                 // P12.5
	ServerMaxDeliver  = -1                  // P12.5: unlimited on the broker
	InactiveThreshold = 30 * 24 * time.Hour // P12.5
	HopLimit          = 10                  // P12.8: an inbound hop count above it is dead-lettered
	DefaultMaxDeliver = 8                   // P12.5: EVENTS_MAX_DELIVER default
)

// DefaultBackoff returns a fresh copy of the default EVENTS_BACKOFF, 1s,10s,1m,5m,15m,30m,1h (P12.5).
func DefaultBackoff() []time.Duration {
	return []time.Duration{time.Second, 10 * time.Second, time.Minute, 5 * time.Minute,
		15 * time.Minute, 30 * time.Minute, time.Hour}
}

// Patterns of envelope.schema.json; compiled regexps are immutable and safe to share.
var (
	subjectPattern   = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*(\.[a-z][a-z0-9]*(_[a-z0-9]+)*){2,}\.v[1-9][0-9]*$`)
	componentPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*/[a-z][a-z0-9-]*$`)
)

// ValidSubject checks a subject against P12.3: <domain>.<name>.<event…>.v<n>, at least four
// segments, each [a-z][a-z0-9]*(_[a-z0-9]+)*, n ≥ 1. Otherwise SUBJECT_INVALID.
func ValidSubject(s string) error {
	if !subjectPattern.MatchString(s) {
		return fail(ReasonSubjectInvalid, "subject "+strconv.Quote(s)+" breaks <domain>.<name>.<event…>.v<n> (P12.3)")
	}
	return nil
}

// ValidComponentID checks a component ID, <scope>/<name> with lowercase letters, digits and
// hyphens and no underscore (P12.5, envelope ce-source). Otherwise COMPONENT_INVALID.
func ValidComponentID(id string) error {
	if !componentPattern.MatchString(id) {
		return fail(ReasonComponentInvalid, "component ID "+strconv.Quote(id)+" is not <scope>/<name>")
	}
	return nil
}

// Stream returns the stream of a subject, BE_<FIRST SEGMENT IN CAPITALS>, and its filter
// <first segment>.> (P12.4).
func Stream(subject string) (stream, filter string, err error) {
	if err := ValidSubject(subject); err != nil {
		return "", "", err
	}
	first, _, _ := strings.Cut(subject, ".")
	return "BE_" + strings.ToUpper(first), first + ".>", nil
}

// Durable returns the durable name of a (component, subject) subscription, the component ID with /
// as _, then __, then the subject with every . as __ (P12.5), and its dead-letter subject
// dlq.<durable>.<subject> (P12.7). The name is injective over valid inputs.
func Durable(componentID, subject string) (durable, dlqSubject string, err error) {
	if err := ValidComponentID(componentID); err != nil {
		return "", "", err
	}
	if err := ValidSubject(subject); err != nil {
		return "", "", err
	}
	durable = strings.Replace(componentID, "/", "_", 1) + "__" + strings.ReplaceAll(subject, ".", "__")
	return durable, "dlq." + durable + "." + subject, nil
}
