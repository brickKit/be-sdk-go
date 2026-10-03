package config

import (
	"encoding/json"
	"time"
)

// entry is one declared key after Load.
type entry struct {
	decl          Decl
	v             value
	sourceRaw     string
	sourcePresent bool
}

// Values holds every declared key parsed at start (P2.3). Its getters return no error: Load already
// validated the values. Reading an undeclared key that is not a platform name (P2.2), or reading a
// key with a getter that does not fit its format, is a programming error: the getter panics with an
// *Error (CONFIG_UNDECLARED / CONFIG_INVALID), which the runtime's start phase recovers with AsError
// and turns into exit 78. A component's own string key may be read with a typed getter: it is parsed
// then by the same strict rules, and a value that does not parse panics with CONFIG_INVALID. Platform
// names (COMPONENT_ID, COMPONENT_VERSION, PORT, *_ENDPOINT) are read from the Source. Values is
// immutable and safe for concurrent use.
type Values struct {
	schema  Schema
	src     Source
	entries map[string]entry
}

// Schema returns the schema the values were loaded with.
func (vs *Values) Schema() Schema { return vs.schema }

// Decl returns the declaration of a declared key.
func (vs *Values) Decl(key string) (Decl, bool) { return vs.schema.Lookup(key) }

// Has reports whether key has a value (after defaults).
func (vs *Values) Has(key string) bool {
	_, ok := vs.String(key)
	return ok
}

// Raw returns the key's value exactly as the Source held it, before defaults, and whether the
// variable exists.
func (vs *Values) Raw(key string) (string, bool) {
	e, platform := vs.lookup(key)
	if platform {
		return vs.src(key)
	}
	return e.sourceRaw, e.sourcePresent
}

// String returns the text value of key (after defaults) and whether it is set. For a secret key it is
// the file's path (P2.7); read the secret itself with OpenSecret.
func (vs *Values) String(key string) (string, bool) {
	e, platform := vs.lookup(key)
	if platform {
		return vs.src(key)
	}
	return e.v.raw, e.v.set
}

// Int returns an int value (P2.3).
func (vs *Values) Int(key string) (int64, bool) {
	v := vs.typed(key, FormatInt)
	return v.i, v.set
}

// Bool returns a bool value (P2.3).
func (vs *Values) Bool(key string) (bool, bool) {
	v := vs.typed(key, FormatBool)
	return v.b, v.set
}

// Duration returns a duration value (P2.3).
func (vs *Values) Duration(key string) (time.Duration, bool) {
	v := vs.typed(key, FormatDuration)
	return v.d, v.set
}

// Durations returns a durations value (P2.3, EVENTS_BACKOFF); the slice is the caller's.
func (vs *Values) Durations(key string) ([]time.Duration, bool) {
	v := vs.typed(key, FormatDurations)
	return append([]time.Duration(nil), v.ds...), v.set
}

// Location returns a zone value (BUSINESS_TIMEZONE).
func (vs *Values) Location(key string) (*time.Location, bool) {
	v := vs.typed(key, FormatZone)
	return v.zone, v.set
}

// JSON decodes a json value into into and reports whether the key is set; the error is the decoding
// into the caller's type (the value itself was validated at start).
func (vs *Values) JSON(key string, into any) (bool, error) {
	v := vs.typed(key, FormatJSON)
	if !v.set {
		return false, nil
	}
	return true, json.Unmarshal(v.json, into)
}

// Family returns a slot-family address (P2.10): the REST base http://host:port for AUTHZ_URL and
// IAM_URL, the gRPC dial target host:port for AUTHZ_GRPC_URL and IAM_GRPC_URL; ok == false when the
// member does not run (the key is absent). Load validated the value; any other key panics with
// CONFIG_KEY_INVALID.
func (vs *Values) Family(key string) (addr string, ok bool) {
	if _, known := familyKind(key); !known {
		panic(newErr(ReasonKeyInvalid, key, "not a slot-family address key"))
	}
	raw, set := vs.String(key)
	if !set {
		return "", false
	}
	addr, _, err := FamilyAddress(key, raw, true)
	if err != nil {
		panic(err)
	}
	return addr, true
}

// typed returns key's value in format, parsing a plain string key (or a platform name) on read.
func (vs *Values) typed(key, format string) value {
	e, platform := vs.lookup(key)
	if platform {
		raw, ok := vs.src(key)
		e = entry{decl: Decl{Name: key, Format: FormatString}, v: value{set: ok, raw: raw}}
	}
	switch {
	case e.decl.Secret:
		panic(newErr(ReasonInvalid, key, "a secret key is read with its Secret, not as "+format))
	case e.decl.Format == format:
		return e.v
	case e.decl.Format != FormatString:
		panic(newErr(ReasonInvalid, key, "declared format "+e.decl.Format+", read as "+format))
	case !e.v.set:
		return value{}
	}
	v, err := parseFormat(Decl{Name: key, Format: format}, e.v.raw)
	if err != nil {
		panic(err)
	}
	v.set, v.raw = true, e.v.raw
	return v
}

// lookup returns the entry of a declared key, or platform == true for a platform name, and panics with
// CONFIG_UNDECLARED for any other key (P2.2).
func (vs *Values) lookup(key string) (e entry, platform bool) {
	if e, ok := vs.entries[key]; ok {
		return e, false
	}
	if IsPlatformName(key) {
		return entry{}, true
	}
	panic(newErr(ReasonUndeclared, key, "key is not declared in configSchema"))
}
