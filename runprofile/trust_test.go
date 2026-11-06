package runprofile

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type issued struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

var serial int64

// mint makes a CA (parent nil: self-signed) or a client certificate under parent, valid from..until.
func mint(t *testing.T, name string, parent *issued, ca bool, from, until time.Time) issued {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial++
	tpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, NotBefore: from,
		NotAfter: until, BasicConstraintsValid: true, IsCA: ca}
	if ca {
		tpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	} else {
		tpl.KeyUsage = x509.KeyUsageDigitalSignature
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	signer, signerKey := tpl, key
	if parent != nil {
		signer, signerKey = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, signer, &key.PublicKey, signerKey)
	require.NoError(t, err)
	c, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return issued{cert: c, key: key}
}

func pemOf(c *x509.Certificate) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Raw}))
}

// trustRecord is a trust record signed by a key with a chain, received at at.
func trustRecord(t *testing.T, kind string, body TrustBody, by issued, at time.Time) TrustRecord {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	r := Record{RecordType: RecordType, Schema: "1.0", Deployment: "d", Kind: kind, ID: "r-" + body.Event + "-" + at.Format("150405"),
		WrittenAt: at.Format(AtLayoutForTest), Writer: Writer{Component: "ops/trust"}, Body: raw}
	r.Signature, err = signWith(r, by.key, [][]byte{by.cert.Raw})
	require.NoError(t, err)
	return TrustRecord{Record: r, Received: at}
}

const AtLayoutForTest = "2006-01-02T15:04:05.000000Z"

// The trust material's history on real certificates (P12.24-P12.26): a rotation signed by the old CA and accepted by
// the new one keeps the old CA's records valid when only the new one is configured; without the acceptance, or forged
// by a CA nobody vouches for, it does not; a revocation by a trusted CA ends its certificate's records received from
// it on (less the skew); one by an untrusted signer counts for nothing; validity is judged at the receipt.
func TestTheTrustHistoryOnCertificates(t *testing.T) {
	t0 := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	a := mint(t, "ca-a", nil, true, t0.Add(-time.Hour), t0.Add(48*time.Hour))
	b := mint(t, "ca-b", nil, true, t0.Add(-time.Hour), t0.Add(48*time.Hour))
	f := mint(t, "ca-fake", nil, true, t0.Add(-time.Hour), t0.Add(48*time.Hour))
	leaf := mint(t, "infra/authz", &a, false, t0.Add(-time.Hour), t0.Add(24*time.Hour))
	forged := mint(t, "infra/authz", &f, false, t0.Add(-time.Hour), t0.Add(24*time.Hour))
	fpA, fpB, fpF := Fingerprint(a.cert.Raw), Fingerprint(b.cert.Raw), Fingerprint(f.cert.Raw)

	rotation := TrustBody{Event: EventRotation, Previous: fpA, Next: fpB, CA: pemOf(b.cert)}
	accepted, err := Accept(rotation, b.key)
	require.NoError(t, err)
	rot := trustRecord(t, KindTrustCA, accepted, a, t0.Add(time.Minute))
	revoke := trustRecord(t, KindTrustCredential, TrustBody{Event: EventRevoke, Fingerprint: Fingerprint(leaf.cert.Raw)}, b,
		t0.Add(10*time.Minute))
	chain := [][]byte{leaf.cert.Raw, a.cert.Raw}

	h := BuildTrust([]*x509.Certificate{b.cert}, []TrustRecord{rot, revoke}, DefaultSkew)
	assert.True(t, h.Trusted(fpA), "A rotated to B with both signatures: A still vouches")
	assert.NoError(t, h.Holds(chain, t0.Add(5*time.Minute)), "before the revocation")
	assert.NoError(t, h.Holds(chain, t0.Add(10*time.Minute-3*time.Second)), "more than the skew before it")
	assert.ErrorContains(t, h.Holds(chain, t0.Add(10*time.Minute-time.Second)), "revoked", "within the skew")
	assert.ErrorContains(t, h.Holds(chain, t0.Add(time.Hour)), "revoked")
	plain := BuildTrust([]*x509.Certificate{a.cert}, nil, DefaultSkew)
	assert.NoError(t, plain.Holds(chain, t0.Add(23*time.Hour)), "within the certificate's validity at the receipt")
	assert.ErrorContains(t, plain.Holds(chain, t0.Add(25*time.Hour)), "no trusted CA",
		"past the certificate's validity at the receipt (whatever the clock says now)")
	assert.ErrorContains(t, plain.Holds(chain, t0.Add(-2*time.Hour)), "no trusted CA", "before its validity")

	// accepted by B but not signed by A (the record signed with another key, A's certificate claimed as its chain): A
	// does not become trusted on B's word alone
	notSigned := trustRecord(t, KindTrustCA, accepted, f, t0.Add(time.Minute))
	notSigned.Record.Signature.CertChain = pemOf(a.cert)
	h = BuildTrust([]*x509.Certificate{b.cert}, []TrustRecord{notSigned}, DefaultSkew)
	assert.False(t, h.Trusted(fpA))

	// without the next CA's acceptance: A is not trusted
	h = BuildTrust([]*x509.Certificate{b.cert}, []TrustRecord{trustRecord(t, KindTrustCA, rotation, a, t0)}, DefaultSkew)
	assert.False(t, h.Trusted(fpA))
	assert.ErrorContains(t, h.Holds(chain, t0.Add(5*time.Minute)), "no trusted CA")
	assert.NotEmpty(t, h.Problems)

	// forged: a rotation from a fake CA to B, signed by the fake and "accepted" by the fake's own key
	fake := TrustBody{Event: EventRotation, Previous: fpF, Next: fpB, CA: pemOf(b.cert)}
	fakeAccepted, err := Accept(fake, f.key)
	require.NoError(t, err)
	h = BuildTrust([]*x509.Certificate{b.cert}, []TrustRecord{trustRecord(t, KindTrustCA, fakeAccepted, f, t0)}, DefaultSkew)
	assert.False(t, h.Trusted(fpF))
	assert.Error(t, h.Holds([][]byte{forged.cert.Raw, f.cert.Raw}, t0.Add(time.Minute)))

	// a revocation signed by an untrusted CA counts for nothing
	h = BuildTrust([]*x509.Certificate{a.cert}, []TrustRecord{trustRecord(t, KindTrustCredential,
		TrustBody{Event: EventRevoke, Fingerprint: Fingerprint(leaf.cert.Raw)}, f, t0)}, DefaultSkew)
	assert.NoError(t, h.Holds(chain, t0.Add(time.Hour)))
	assert.NotEmpty(t, h.Problems)

	// the CA itself revoked: every chain under it from then on
	h = BuildTrust([]*x509.Certificate{b.cert}, []TrustRecord{rot, trustRecord(t, KindTrustCA,
		TrustBody{Event: EventRevoke, Fingerprint: fpA}, b, t0.Add(20*time.Minute))}, DefaultSkew)
	assert.NoError(t, h.Holds(chain, t0.Add(15*time.Minute)))
	assert.ErrorContains(t, h.Holds(chain, t0.Add(30*time.Minute)), "revoked")
}
