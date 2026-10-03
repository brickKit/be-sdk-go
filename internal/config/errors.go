package config

import (
	"errors"
	"strings"
)

// Error classes of vectors/config (README "Errors") plus MANIFEST_INVALID for a component.yaml the
// SDK cannot read. Every one of them is a configuration error at start: exit 78 (P1.2).
const (
	ReasonMissing          = "CONFIG_MISSING"       // P2.3: required key absent or empty
	ReasonInvalid          = "CONFIG_INVALID"       // P2.3: present value that does not parse
	ReasonUndeclared       = "CONFIG_UNDECLARED"    // P2.2: read of a key not in configSchema
	ReasonComponentInvalid = "COMPONENT_INVALID"    // malformed dependency id
	ReasonPortNameInvalid  = "PORT_NAME_INVALID"    // malformed port name
	ReasonKeyInvalid       = "CONFIG_KEY_INVALID"   // P2.4: a key a component may not declare
	ReasonKeyReserved      = "CONFIG_KEY_RESERVED"  // P2.4: a platform name
	ReasonSecretNotFile    = "SECRET_NOT_FILE"      // P2.12
	ReasonFileSuffixReq    = "FILE_SUFFIX_REQUIRED" // P2.12
	ReasonFileSuffixRsv    = "FILE_SUFFIX_RESERVED" // P2.12
	ReasonMountInvalid     = "MOUNT_INVALID"        // P2.12 (brickKit: mount is only file)
	ReasonMountNeedsSecret = "MOUNT_NEEDS_SECRET"   // P2.12 (brickKit: mount only with secret)
	ReasonMountNeedsString = "MOUNT_NEEDS_STRING"   // P2.12 (brickKit: mount only on a string)
	ReasonManifestInvalid  = "MANIFEST_INVALID"     // component.yaml unreadable or malformed
)

// Error is one configuration problem naming its key (P1.2: one JSON log line per problem). Detail
// never contains a configuration value, so a secret can never leak through it (P2.7).
type Error struct {
	Reason string // one of the Reason* classes
	Key    string // the configuration key, "" when the problem is not about one key
	Detail string // what is wrong, without the value
}

// Error implements error.
func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString(e.Reason)
	if e.Key != "" {
		b.WriteString(": ")
		b.WriteString(e.Key)
	}
	if e.Detail != "" {
		b.WriteString(": ")
		b.WriteString(e.Detail)
	}
	return b.String()
}

// Errors is several configuration problems reported together (P2.3: all errors at once).
type Errors []*Error

// Error implements error: one problem per line.
func (es Errors) Error() string {
	parts := make([]string, len(es))
	for i, e := range es {
		parts[i] = e.Error()
	}
	return strings.Join(parts, "\n")
}

// AsError returns the *Error carried by v: an *Error, an error wrapping one, or a value recovered
// from a getter's panic. It is how the runtime's start phase turns a programming error into exit 78.
func AsError(v any) (*Error, bool) {
	switch x := v.(type) {
	case *Error:
		return x, x != nil
	case error:
		var e *Error
		if errors.As(x, &e) {
			return e, true
		}
	}
	return nil, false
}

func newErr(reason, key, detail string) *Error {
	return &Error{Reason: reason, Key: key, Detail: detail}
}
