package envelope

import "errors"

// Error reasons used by this package; the names are the be-protocol vector error classes and the
// be-dlq-reason vocabulary (vectors/envelope README "Errors" and "Decided here").
const (
	ReasonIDInvalid          = "ID_INVALID"           // P11.5
	ReasonSubjectInvalid     = "SUBJECT_INVALID"      // P12.3
	ReasonComponentInvalid   = "COMPONENT_INVALID"    // P12.5
	ReasonEnvelopeInvalid    = "ENVELOPE_INVALID"     // P12 envelope, P12.14
	ReasonLegalEntityMissing = "LEGAL_ENTITY_MISSING" // P11.8
	ReasonHopLimit           = "HOP_LIMIT"            // P12.8
	ReasonPayloadInvalid     = "PAYLOAD_INVALID"      // P12.7
	ReasonMaxDeliver         = "MAX_DELIVER"          // P12.7
	ReasonPermanent          = "PERMANENT"            // P12.7
)

// Error is a validation failure of this package, carrying one of the Reason* classes.
type Error struct {
	Reason string // one of the Reason* constants
	Detail string // what was wrong, for logs
}

// Error implements error.
func (e *Error) Error() string { return e.Reason + ": " + e.Detail }

// ReasonOf returns the Reason of an *Error in err's chain, or "" when there is none.
func ReasonOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Reason
	}
	return ""
}

func fail(reason, detail string) error { return &Error{Reason: reason, Detail: detail} }
