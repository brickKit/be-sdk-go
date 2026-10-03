package envelope

import (
	"time"

	"github.com/google/uuid"
)

// NewID returns a new UUIDv7 (P11.5): the id of an outbox row, an event (ce-id) or any own primary
// key. Ids made in one process increase strictly (google/uuid keeps a monotonic counter within one
// millisecond).
func NewID() uuid.UUID {
	// NewV7 fails only when crypto/rand fails, which Go (1.24+) never reports: it aborts instead.
	return uuid.Must(uuid.NewV7())
}

// ParseID parses an id received as text (P11.5): exactly the 36-character hyphenated form, version
// 7, variant 10xx. Upper-case hex is accepted and normalised (RFC 9562); braces, "urn:uuid:", the
// 32-digit form and surrounding whitespace are ID_INVALID.
func ParseID(s string) (uuid.UUID, error) {
	if len(s) != 36 {
		return uuid.Nil, fail(ReasonIDInvalid, "id is not 36 characters")
	}
	id, err := uuid.Parse(s) // with 36 characters uuid.Parse accepts only the hyphenated hex form
	if err != nil {
		return uuid.Nil, fail(ReasonIDInvalid, err.Error())
	}
	if id.Version() != 7 {
		return uuid.Nil, fail(ReasonIDInvalid, "id is not a UUIDv7")
	}
	if id.Variant() != uuid.RFC4122 {
		return uuid.Nil, fail(ReasonIDInvalid, "id variant is not 10xx")
	}
	return id, nil
}

// IDTime returns the 48-bit Unix millisecond timestamp of a UUIDv7, in UTC (P11.5): the created_at
// of a row on a partitioned table.
func IDTime(id uuid.UUID) time.Time {
	var ms int64
	for _, b := range id[:6] {
		ms = ms<<8 | int64(b)
	}
	return time.UnixMilli(ms).UTC()
}
