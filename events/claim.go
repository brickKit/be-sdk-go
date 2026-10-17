package events

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// ClaimRef is the payload of a claim-checked event (P6.14).
type ClaimRef struct {
	URI    string `json:"uri"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

// ClaimReader fetches a claim-checked payload with the producer's short-lived read credential (protocol 12).
type ClaimReader interface {
	Get(ctx context.Context, uri string) ([]byte, error)
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// ResolveClaim returns the real payload of a delivery: as is, or fetched and checked against the reference's size
// and digest. A mismatch is a processing failure (P6.14).
func ResolveClaim(ctx context.Context, m Message, r ClaimReader) ([]byte, error) {
	if m.Headers[HeaderClaim] != "true" {
		return m.Data, nil
	}
	var ref struct{ Claim ClaimRef }
	if err := json.Unmarshal(m.Data, &ref); err != nil || ref.Claim.URI == "" {
		return nil, fmt.Errorf("event %s: the claim reference cannot be read", m.ID())
	}
	if r == nil {
		return nil, fmt.Errorf("event %s: a claim-checked payload and no object storage reader", m.ID())
	}
	data, err := r.Get(ctx, ref.Claim.URI)
	if err != nil {
		return nil, fmt.Errorf("event %s: fetching the claim: %w", m.ID(), err)
	}
	if len(data) != ref.Claim.Size || sha256Hex(data) != ref.Claim.SHA256 {
		return nil, fmt.Errorf("event %s: the claimed object does not match its size and digest", m.ID())
	}
	return data, nil
}
