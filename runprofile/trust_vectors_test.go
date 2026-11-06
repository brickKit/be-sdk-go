package runprofile

import (
	"crypto/x509"
	"encoding/json"
	"io/fs"
	"net/url"
	"testing"
	"time"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// P12.4, P12.24-P12.26 on the protocol's trust-history vectors (the writer is the signer): the trusted CAs (from the configured root along doubly signed
// rotations), and whether a record received at t holds (its chain ends at a trusted CA, nothing of it revoked at r
// with t > r - skew; a revocation counts only when its signer was trusted).
func TestTheTrustHistoryVectors(t *testing.T) {
	raw, err := fs.ReadFile(protocol.Vectors, "vectors/trust-history.json")
	require.NoError(t, err)
	var doc struct {
		Cases []struct {
			ID    string
			Input struct {
				Record struct {
					Chain    []string
					Received int64
					Writer   *string
					Signer   string
				}
				Configured []string
				Rotations  []struct {
					Previous, Next   string
					SignedByPrevious bool `json:"signed_by_previous"`
					AcceptedByNext   bool `json:"accepted_by_next"`
				}
				Revocations []struct {
					Fingerprint   string
					Received      int64
					SignerTrusted bool `json:"signer_trusted"`
				}
				Skew int64
			}
			Expected struct {
				Holds  bool
				Reason string
			}
		}
	}
	require.NoError(t, json.Unmarshal(raw, &doc))
	require.NotEmpty(t, doc.Cases)
	at := func(s int64) time.Time { return time.Unix(s, 0).UTC() }
	for _, c := range doc.Cases {
		t.Run(c.ID, func(t *testing.T) {
			var rot []Rotation
			for _, r := range c.Input.Rotations {
				rot = append(rot, Rotation{Previous: r.Previous, Next: r.Next, Signed: r.SignedByPrevious, Accepted: r.AcceptedByNext})
			}
			var rev []Revocation
			for _, r := range c.Input.Revocations {
				if r.SignerTrusted {
					rev = append(rev, Revocation{Fingerprint: r.Fingerprint, At: at(r.Received)})
				}
			}
			trusted := TrustedCAs(c.Input.Configured, rot)
			chain := c.Input.Record.Chain
			reason := ""
			switch {
			case !trusted[chain[len(chain)-1]]:
				reason = "untrusted_ca"
			case revokedAt(chain, at(c.Input.Record.Received), rev, time.Duration(c.Input.Skew)*time.Second):
				reason = "revoked"
			case c.Input.Record.Writer != nil && writerIsSigner(Record{Deployment: "d", Writer: Writer{Component: *c.Input.Record.Writer}},
				&x509.Certificate{URIs: []*url.URL{{Scheme: "spiffe", Host: "d", Path: "/component/" + c.Input.Record.Signer}}}) != nil:
				reason = "writer_not_signer"
			}
			assert.Equal(t, c.Expected.Holds, reason == "")
			assert.Equal(t, c.Expected.Reason, reason)
		})
	}
}
