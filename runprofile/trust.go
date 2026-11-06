package runprofile

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
)

// The trust material's history (be-protocol P12.24-P12.27): what a signed record is checked against is not the
// current trust root alone. A CA rotated out after T still vouches for the records signed under it (a rotation is
// signed by the previous CA and accepted by the next one), and a credential revoked after T no longer vouches for
// what it signed after its revocation (less the declared clock skew).

// The trust material's kinds (P12.3) and events (P12.24).
const (
	KindTrustCA         = "trust.ca"
	KindTrustCredential = "trust.credential"
	EventAnchor         = "anchor"
	EventRotation       = "rotation"
	EventIssue          = "issue"
	EventRevoke         = "revoke"
	// DefaultSkew is the declared clock skew between the channels (assembly protocol 15's parameters: 2 s).
	DefaultSkew = 2 * time.Second
)

// TrustBody is a trust record's body (P12.24).
type TrustBody struct {
	Event       string     `json:"event"`
	CA          string     `json:"ca,omitempty"`          // anchor, rotation: the (next) CA's certificate, PEM
	Previous    string     `json:"previous,omitempty"`    // rotation: the previous CA's fingerprint
	Next        string     `json:"next,omitempty"`        // rotation: the next CA's fingerprint
	Acceptance  *Signature `json:"acceptance,omitempty"`  // rotation: the next CA's signature (no cert_chain)
	Fingerprint string     `json:"fingerprint,omitempty"` // issue, revoke
	Component   string     `json:"component,omitempty"`   // issue
	Role        string     `json:"role,omitempty"`        // issue
	NotAfter    string     `json:"not_after,omitempty"`   // issue
}

// Fingerprint is a certificate's SHA-256 over its DER, lower-case hex (P12.24).
func Fingerprint(der []byte) string {
	s := sha256.Sum256(der)
	return hex.EncodeToString(s[:])
}

// Rotation is what the history knows of a rotation record: which CA to which, and whether both signatures hold.
type Rotation struct {
	Previous, Next   string
	Signed, Accepted bool
}

// Revocation is a revocation that counts (its own signer was trusted): which certificate, received when.
type Revocation struct {
	Fingerprint string
	At          time.Time
}

// TrustedCAs are the CAs trusted: the configured roots, and walking back along every rotation whose next CA is
// trusted and whose two signatures hold (P12.25).
func TrustedCAs(configured []string, rotations []Rotation) map[string]bool {
	trusted := map[string]bool{}
	for _, c := range configured {
		trusted[c] = true
	}
	for changed := true; changed; {
		changed = false
		for _, r := range rotations {
			if trusted[r.Next] && r.Signed && r.Accepted && !trusted[r.Previous] {
				trusted[r.Previous], changed = true, true
			}
		}
	}
	return trusted
}

// revokedAt: a certificate of the chain was revoked at r and the record was received at t > r - skew (P12.26).
func revokedAt(chain []string, t time.Time, revocations []Revocation, skew time.Duration) bool {
	for _, fp := range chain {
		for _, r := range revocations {
			if r.Fingerprint == fp && t.After(r.At.Add(-skew)) {
				return true
			}
		}
	}
	return false
}

// TrustHistory is the trust material as the run profile records it, read against the configured roots.
type TrustHistory struct {
	Skew        time.Duration
	trusted     map[string]*x509.Certificate // by fingerprint
	revocations []Revocation
	Problems    []string // records not used and why (P12.25: not on the chain, a signature that does not hold)
}

// TrustRecord is one trust record read from the run profile with the time the store received it (P12.7).
type TrustRecord struct {
	Record   Record
	Received time.Time
}

// BuildTrust reads the trust records against the configured roots (P12.25): the CAs trusted, and the revocations
// whose own signer held when they were received (P12.26). Records that do not hold are listed in Problems.
func BuildTrust(roots []*x509.Certificate, records []TrustRecord, skew time.Duration) *TrustHistory {
	h := &TrustHistory{Skew: skew, trusted: map[string]*x509.Certificate{}}
	known := map[string]*x509.Certificate{}
	var configured []string
	for _, c := range roots {
		fp := Fingerprint(c.Raw)
		known[fp], configured = c, append(configured, fp)
	}
	var rotations []Rotation
	type revoke struct {
		fp  string
		rec TrustRecord
	}
	var revokes []revoke
	for _, tr := range records {
		var b TrustBody
		if json.Unmarshal(tr.Record.Body, &b) != nil {
			h.Problems = append(h.Problems, tr.Record.ID+": not a trust record")
			continue
		}
		switch b.Event {
		case EventAnchor:
			if c, err := parsePEM(b.CA); err == nil {
				known[Fingerprint(c.Raw)] = c
			}
		case EventRotation:
			next, err := parsePEM(b.CA)
			if err != nil || Fingerprint(next.Raw) != b.Next {
				h.Problems = append(h.Problems, tr.Record.ID+": a rotation whose next CA is not the one it names")
				continue
			}
			known[b.Next] = next
			r := Rotation{Previous: b.Previous, Next: b.Next}
			if chain, err := chainOf(tr.Record); err == nil && len(chain) > 0 && Fingerprint(chain[len(chain)-1]) == b.Previous {
				if prev, err := x509.ParseCertificate(chain[len(chain)-1]); err == nil {
					known[b.Previous] = prev
					r.Signed = verifySignature(tr.Record, prev) == nil
				}
			}
			r.Accepted = verifyAcceptance(tr.Record, b, next) == nil
			if !r.Signed || !r.Accepted {
				h.Problems = append(h.Problems, tr.Record.ID+": a rotation without both signatures")
			}
			rotations = append(rotations, r)
		case EventRevoke:
			revokes = append(revokes, revoke{fp: b.Fingerprint, rec: tr})
		}
	}
	for fp := range TrustedCAs(configured, rotations) {
		if c := known[fp]; c != nil {
			h.trusted[fp] = c
		}
	}
	for _, rv := range revokes { // a revocation counts when its signer chains to a trusted CA when it was received
		chain, err := chainOf(rv.rec.Record)
		if err == nil && h.chainHolds(chain, rv.rec.Received) == nil && verifySignature(rv.rec.Record, mustLeaf(chain)) == nil {
			h.revocations = append(h.revocations, Revocation{Fingerprint: rv.fp, At: rv.rec.Received})
			continue
		}
		h.Problems = append(h.Problems, rv.rec.Record.ID+": a revocation whose signer is not trusted")
	}
	return h
}

// Holds says whether a signer's chain (DER, leaf first) vouched for a record received at t (P12.26).
func (h *TrustHistory) Holds(chain [][]byte, t time.Time) error {
	if err := h.chainHolds(chain, t); err != nil {
		return err
	}
	var fps []string
	for _, c := range chain {
		fps = append(fps, Fingerprint(c))
	}
	if ca := h.anchorOf(chain, t); ca != "" {
		fps = append(fps, ca)
	}
	if revokedAt(fps, t, h.revocations, h.Skew) {
		return errors.New("the signer's credential was revoked before this record was received")
	}
	return nil
}

// chainHolds: the chain verifies to one of the trusted CAs, validity judged at t.
func (h *TrustHistory) chainHolds(chain [][]byte, t time.Time) error {
	if h.anchorOf(chain, t) == "" {
		return errors.New("the signer's chain ends at no trusted CA")
	}
	return nil
}

// anchorOf is the trusted CA the chain verifies to at t (its fingerprint), "" when none.
func (h *TrustHistory) anchorOf(chain [][]byte, t time.Time) string {
	if len(chain) == 0 {
		return ""
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return ""
	}
	inter := x509.NewCertPool()
	for _, d := range chain[1:] {
		if c, err := x509.ParseCertificate(d); err == nil {
			inter.AddCert(c)
		}
	}
	for fp, ca := range h.trusted {
		if Fingerprint(leaf.Raw) == fp { // the record is signed by a CA itself (a trust record)
			return fp
		}
		pool := x509.NewCertPool()
		pool.AddCert(ca)
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter, CurrentTime: t,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err == nil {
			return fp
		}
	}
	return ""
}

// Trusted is whether a CA (by fingerprint) is trusted in this history.
func (h *TrustHistory) Trusted(fp string) bool { return h.trusted[fp] != nil }

func mustLeaf(chain [][]byte) *x509.Certificate {
	if len(chain) == 0 {
		return nil
	}
	c, _ := x509.ParseCertificate(chain[0])
	return c
}

func parsePEM(s string) (*x509.Certificate, error) {
	b, _ := pem.Decode([]byte(s))
	if b == nil {
		return nil, errors.New("no PEM certificate")
	}
	return x509.ParseCertificate(b.Bytes)
}

// chainOf is a record's signer chain (DER, leaf first).
func chainOf(r Record) ([][]byte, error) {
	if r.Signature == nil {
		return nil, errors.New("no signature")
	}
	var chain [][]byte
	rest := []byte(r.Signature.CertChain)
	for {
		var b *pem.Block
		if b, rest = pem.Decode(rest); b == nil {
			break
		}
		chain = append(chain, b.Bytes)
	}
	if len(chain) == 0 {
		return nil, errors.New("no certificate chain")
	}
	return chain, nil
}

// verifySignature checks a record's signature with a certificate's public key (P12.2).
func verifySignature(r Record, signer *x509.Certificate) error {
	if r.Signature == nil || signer == nil {
		return errors.New("no signature")
	}
	msg, err := SignedBytes(r)
	if err != nil {
		return err
	}
	return verifyBytes(msg, r.Signature, signer.PublicKey)
}

func verifyBytes(msg []byte, s *Signature, pub crypto.PublicKey) error {
	sig, err := base64.StdEncoding.DecodeString(s.Value)
	if err != nil {
		return err
	}
	switch k := pub.(type) {
	case *ecdsa.PublicKey:
		d := sha256.Sum256(msg)
		if s.Alg == "ES256" && ecdsa.VerifyASN1(k, d[:], sig) {
			return nil
		}
	case ed25519.PublicKey:
		if s.Alg == "EdDSA" && ed25519.Verify(k, msg, sig) {
			return nil
		}
	}
	return errors.New("the signature does not hold")
}

func signBytes(msg []byte, key crypto.Signer) (*Signature, error) {
	var sig []byte
	var alg string
	var err error
	switch key.Public().(type) {
	case *ecdsa.PublicKey: // ASN.1 DER over SHA-256
		d := sha256.Sum256(msg)
		alg = "ES256"
		sig, err = key.Sign(rand.Reader, d[:], crypto.SHA256)
	case ed25519.PublicKey:
		alg = "EdDSA"
		sig, err = key.Sign(rand.Reader, msg, crypto.Hash(0))
	default:
		return nil, fmt.Errorf("run profile: a %T key cannot sign records", key.Public())
	}
	if err != nil {
		return nil, err
	}
	return &Signature{Alg: alg, Value: base64.StdEncoding.EncodeToString(sig)}, nil
}

// acceptanceBytes is what the next CA signs in a rotation: the body without its acceptance, canonical.
func acceptanceBytes(b TrustBody) ([]byte, error) {
	b.Acceptance = nil
	raw, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return canon.Encode(v)
}

// Accept signs a rotation's body with the next CA's key (P12.24), for the issuer.
func Accept(b TrustBody, next crypto.Signer) (TrustBody, error) {
	msg, err := acceptanceBytes(b)
	if err != nil {
		return b, err
	}
	sig, err := signBytes(msg, next)
	if err != nil {
		return b, err
	}
	b.Acceptance = sig
	return b, nil
}

func verifyAcceptance(r Record, b TrustBody, next *x509.Certificate) error {
	if b.Acceptance == nil {
		return errors.New("no acceptance")
	}
	msg, err := acceptanceBytes(b)
	if err != nil {
		return err
	}
	return verifyBytes(msg, b.Acceptance, next.PublicKey)
}

// pemChain encodes a chain (DER) as PEM, leaf first.
func pemChain(chain [][]byte) string {
	var out bytes.Buffer
	for _, c := range chain {
		_ = pem.Encode(&out, &pem.Block{Type: "CERTIFICATE", Bytes: c})
	}
	return out.String()
}
