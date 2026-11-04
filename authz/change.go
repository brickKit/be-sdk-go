package authz

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/canon"
)

// CandidateDigest is the digest a change request's candidate carries (security 09a's be.security.change_request):
// the candidate's canonical form under its record type, be.security.<record type>, major version 1. The candidate is
// JSON as sent: integers stay numbers; floating point and null are refused (09a: neither appears in a record).
func CandidateDigest(recordType string, candidate json.RawMessage) (string, error) {
	d := json.NewDecoder(bytes.NewReader(candidate))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return "", fmt.Errorf("the candidate is not JSON: %w", err)
	}
	if _, ok := v.(map[string]any); !ok {
		return "", fmt.Errorf("the candidate is not an object")
	}
	return canon.Digest("be.security."+recordType, 1, v)
}
