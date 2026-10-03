package config

import (
	"regexp"
	"time"

	// The IANA database is embedded so a zone parses in any image, including one without tzdata
	// (BUSINESS_TIMEZONE, P14.6). It is read-only data, not process state.
	_ "time/tzdata"
)

// Further protocol formats (schemas/config-keys.yaml): zone and locale.
const (
	FormatZone   = "zone"
	FormatLocale = "locale"
)

var (
	zoneRe   = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+-]*(/[A-Za-z0-9_+-]+)*$`)
	localeRe = regexp.MustCompile(`^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$`)
)

// ParseZone checks a zone value: an IANA time-zone name the runtime can load (BUSINESS_TIMEZONE).
// "Local" is not a zone name.
func ParseZone(key, raw string) (*time.Location, error) {
	if raw == "Local" || !zoneRe.MatchString(raw) {
		return nil, newErr(ReasonInvalid, key, "not an IANA time-zone name")
	}
	loc, err := time.LoadLocation(raw)
	if err != nil {
		return nil, newErr(ReasonInvalid, key, "unknown IANA time-zone name")
	}
	return loc, nil
}

// ParseLocale checks a locale value: a BCP 47 tag in its common form (DEFAULT_LOCALE, P4.1).
func ParseLocale(key, raw string) (string, error) {
	if !localeRe.MatchString(raw) {
		return "", newErr(ReasonInvalid, key, "not a BCP 47 language tag")
	}
	return raw, nil
}
