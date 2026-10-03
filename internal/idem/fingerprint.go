package idem

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// Canonicalize parses raw JSON text as I-JSON (RFC 7493) and returns its RFC 8785 (JCS) form (P13.2
// "Fingerprint"). Duplicate member names, NaN / Infinity, numbers that overflow a double, invalid
// UTF-8, lone surrogates and any non-JSON text are a *problem.Invalid with reason JSON_INVALID.
func Canonicalize(jsonText []byte) ([]byte, error) {
	v, err := parseIJSON(jsonText)
	if err != nil {
		return nil, err
	}
	return writeJCS(make([]byte, 0, len(jsonText)), v), nil
}

// Fingerprint is the request_hash of P13.2: SHA-256 over the UTF-8 bytes of the JCS form of the
// fingerprint object, 32 raw bytes (stored as BYTEA).
func Fingerprint(jsonText []byte) ([]byte, error) {
	c, err := Canonicalize(jsonText)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(c)
	return sum[:], nil
}

// FingerprintValue fingerprints a Go value (besdk.Command.Request): encoding/json first, then the
// same canonicalisation, so a struct, a map and the raw JSON text of the same fields hash alike. A
// value encoding/json cannot encode (a channel, a NaN) is a programming error and comes back as a
// plain error (INTERNAL at the edge). A nil value fingerprints as JSON null.
func FingerprintValue(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("idem: fingerprint fields: %w", err)
	}
	return Fingerprint(b)
}
