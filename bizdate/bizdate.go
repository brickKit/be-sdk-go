// Package bizdate gives business dates (chapter 05, P5.29): the local date of an instant in the legal entity's IANA
// time zone. SQL never asks the database for the current date; "today" is computed here and passed in.
package bizdate

import (
	"fmt"
	"time"
	_ "time/tzdata" // images need not ship a zone database
)

// Of is the business date (YYYY-MM-DD) of t in the zone tz.
func Of(t time.Time, tz string) (string, error) {
	loc, err := time.LoadLocation(tz)
	if err != nil || tz == "" || tz == "Local" {
		return "", fmt.Errorf("bizdate: %q is not an IANA time zone", tz)
	}
	return t.In(loc).Format(time.DateOnly), nil
}

// Today is the current business date in the zone tz.
func Today(tz string) (string, error) { return Of(time.Now(), tz) }
