// Package sectest is a test deployment's security: a token issuer with its published keys, a trust root issuing
// component certificates, and a fake authorization member answering view verifications over mTLS. Tests only.
package sectest

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/internal/records"
	authzv3 "github.com/brickKit/contract-infra-authz/v3/gen/go/be/authz/v3"
	auditv3 "github.com/brickKit/infra-audit/contract/v3/gen/go/be/audit/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Principals used by tests.
const (
	Alice = "0190f3a1-0000-7000-8000-0000000000a1"
	Bob   = "0190f3a1-0000-7000-8000-0000000000b2"
)

// World is one test deployment.
type World struct {
	Deployment string
	Issuer     string
	JWKSURL    string
	AuthzAddr  string
	AuditAddr  string
	dir        string
	signer     ed25519.PrivateKey
	caCert     *x509.Certificate
	caKey      *ecdsa.PrivateKey
	caFile     string

	mu       sync.Mutex
	subjects map[string]authz.SubjectEntry
	types    map[string]bool
	gen      int
	audited  map[string]json.RawMessage // record ID -> the record as received
	// auditDigests and submitted are the fake members' memory; RefuseDefinitions makes Submit refuse.
	auditDigests      map[string]string
	submitted         map[string]string
	RefuseDefinitions string
}

// New starts the issuer and the fake authorization member.
func New(t testing.TB) *World {
	t.Helper()
	w := &World{Deployment: "0190f3a1-0000-7000-8000-00000000d001", dir: t.TempDir(), subjects: map[string]authz.SubjectEntry{},
		types: map[string]bool{}, gen: 1, audited: map[string]json.RawMessage{}, auditDigests: map[string]string{},
		submitted: map[string]string{}}
	_, w.signer, _ = ed25519.GenerateKey(rand.Reader)
	jwks := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		pub := w.signer.Public().(ed25519.PublicKey)
		_ = json.NewEncoder(rw).Encode(map[string]any{"keys": []any{map[string]string{"kty": "OKP", "crv": "Ed25519",
			"kid": "test-1", "alg": "EdDSA", "use": "sig", "x": base64.RawURLEncoding.EncodeToString(pub)}}})
	}))
	t.Cleanup(jwks.Close)
	w.JWKSURL, w.Issuer = jwks.URL, "https://iam.test/"
	w.trustRoot(t)
	w.startAuthz(t)
	w.startAudit(t)
	return w
}

// Token issues a valid access token for a principal.
func (w *World) Token(subject string) string {
	now := time.Now().Unix()
	h, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "at+jwt", "kid": "test-1"})
	c, _ := json.Marshal(map[string]any{"iss": w.Issuer, "aud": "be:deployment:" + w.Deployment, "sub": subject,
		"iat": now, "exp": now + 600})
	signing := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(c)
	return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(w.signer, []byte(signing)))
}

// Bearer is the Authorization header value for a principal.
func (w *World) Bearer(subject string) string { return "Bearer " + w.Token(subject) }

func (w *World) trustRoot(t testing.TB) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test deployment root"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true,
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	w.caCert, _ = x509.ParseCertificate(der)
	w.caKey = key
	w.caFile = filepath.Join(w.dir, "ca.pem")
	write(t, w.caFile, "CERTIFICATE", der)
}

func write(t testing.TB, path, kind string, der []byte) {
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

var serial int64 = 100

// Certs issues a component's certificate in this deployment and returns the three file paths.
func (w *World) Certs(t testing.TB, component string) (cert, key, ca string) {
	return w.CertsIn(t, w.Deployment, component)
}

// CertsIn issues a certificate naming any deployment (for refusals).
func (w *World) CertsIn(t testing.TB, deployment, component string) (cert, key, ca string) {
	return w.issue(t, deployment, component, []string{identity.DNSName(deployment, component)})
}

// CertsWithoutDNS issues a certificate with only the URI SAN (for refusals: P8.5 needs both).
func (w *World) CertsWithoutDNS(t testing.TB, component string) (cert, key, ca string) {
	return w.issue(t, w.Deployment, component, nil)
}

func (w *World) issue(t testing.TB, deployment, component string, dns []string) (cert, key, ca string) {
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	u, _ := url.Parse(identity.SPIFFE(deployment, component))
	w.mu.Lock()
	serial++
	n := serial
	w.mu.Unlock()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(n), Subject: pkix.Name{CommonName: component}, URIs: []*url.URL{u},
		DNSNames:  dns,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, w.caCert, &k.PublicKey, w.caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalPKCS8PrivateKey(k)
	base := filepath.Join(w.dir, strconv.FormatInt(n, 10))
	cert, key = base+"-cert.pem", base+"-key.pem"
	write(t, cert, "CERTIFICATE", der)
	write(t, key, "PRIVATE KEY", keyDER)
	return cert, key, w.caFile
}

// TLS loads a component's service identity.
func (w *World) TLS(t testing.TB, component string) *identity.ServiceTLS {
	cert, key, ca := w.Certs(t, component)
	s, err := identity.LoadServiceTLS(w.Deployment, component, cert, key, ca, os.ReadFile)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Env is a component's security configuration in this deployment.
func (w *World) Env(t testing.TB, component string) map[string]string {
	cert, key, ca := w.Certs(t, component)
	return map[string]string{"DEPLOYMENT_ID": w.Deployment, "IAM_ISSUER": w.Issuer, "IAM_JWKS_URL": w.JWKSURL,
		"AUTHZ_GRPC_URL": "http://" + w.AuthzAddr, "AUDIT_GRPC_URL": "http://" + w.AuditAddr, "SERVICE_TLS_CERT_FILE": cert, "SERVICE_TLS_KEY_FILE": key,
		"SERVICE_TLS_CA_FILE": ca}
}

// Grant gives a principal segments (their resource types become applicable).
func (w *World) Grant(principal string, segs ...authz.Segment) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e := w.subjects[principal]
	e.Segments = append(e.Segments, segs...)
	w.subjects[principal] = e
	for _, s := range segs {
		w.types[s.ResourceType] = true
	}
	w.gen++
}

// Revoke removes every segment a principal holds; the next view the component verifies no longer has them.
func (w *World) Revoke(principal string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	e := w.subjects[principal]
	e.Segments = nil
	w.subjects[principal] = e
	w.gen++
}

// GrantAll gives a principal every declared operation on every record (no dimension restricts it), reading all
// non-secret fields.
func (w *World) GrantAll(principal string, defs *authz.Definitions) {
	var segs []authz.Segment
	for _, op := range defs.Operations {
		var res authz.Resource
		for _, r := range defs.Resources {
			if r.Type == op.ResourceType {
				res = r
			}
		}
		atoms := map[string]authz.Atom{}
		for d := range res.Dimensions {
			atoms[d] = authz.Atom{Kind: "all"}
		}
		var fields []string
		for f, fd := range res.Fields {
			if fd.Class != authz.Secret {
				fields = append(fields, f)
			}
		}
		segs = append(segs, authz.Segment{ResourceType: res.Type, Operation: op.Key, Atoms: atoms, ReadFields: fields,
			Valid: authz.Window{From: "2026-01-01T00:00:00Z"}})
	}
	w.Grant(principal, segs...)
}

// Defs declares one resource without dimensions; the first operation is its view operation.
func Defs(component, resource string, ops ...string) *authz.Definitions {
	d := &authz.Definitions{Component: component, Resources: []authz.Resource{{Type: resource, ViewOperation: ops[0],
		Dimensions: map[string]authz.Dimension{}, Fields: map[string]authz.Field{"id": {Class: authz.Ordinary}}}}}
	for _, op := range ops {
		d.Operations = append(d.Operations, authz.Operation{Key: op, ResourceType: resource, Purpose: "read", RiskClass: "low"})
	}
	return d
}

// The fake authorization member's identity and the defaults it fills into granted segments.
const (
	Instance      = "authz-test-1"
	segmentSource = "0190f3a1-0000-7000-8000-00000000500e"
	viewID        = "0190f3a1-0000-7000-8000-0000000000a0"
)

// view is the one shard the fake member issues, as canonical bytes and their digest (D76).
func (w *World) view() ([]byte, string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	gen := strconv.Itoa(w.gen)
	types := make([]any, 0, len(w.types))
	for typ := range w.types {
		types = append(types, typ)
	}
	applicability, err := canon.SortedSet(types)
	if err != nil {
		return nil, "", err
	}
	subjects := map[string]any{}
	for p, e := range w.subjects {
		segs := make([]any, 0, len(e.Segments))
		for _, seg := range e.Segments {
			if seg.Source == "" {
				seg.Source = segmentSource
			}
			if len(seg.Definition) == 0 {
				seg.Definition, _ = json.Marshal(map[string]any{"owner_component": "test/fake", "kind": "resource",
					"key": seg.ResourceType, "version": "3.0.0", "digest": "sha256:" + strings.Repeat("0", 64)})
			}
			c, err := seg.Canonical(seg.ResourceType)
			if err != nil {
				return nil, "", err
			}
			segs = append(segs, c)
		}
		set, err := canon.SortedSet(segs)
		if err != nil {
			return nil, "", err
		}
		subjects[p] = map[string]any{"segments": set, "accounts_disabled": e.AccountsDisabled}
	}
	rev := func(domain string) map[string]any {
		return map[string]any{"domain": domain, "value": gen, "epoch": "1.1", "instance": Instance}
	}
	return records.Seal("be.security.view", map[string]any{"record_type": "be.security.view", "schema": "1.0",
		"deployment": w.Deployment, "authority": "infra/authz", "id": viewID, "revision": rev("authz.view.all"),
		"shard": "all", "recovery_generation": map[string]any{"domain": "authz.recovery", "value": "1"},
		"security_revisions": []any{rev("authz.security.all")}, "applicability": applicability,
		"subjects": subjects, "capabilities": []any{}})
}

type authzMember struct {
	authzv3.UnimplementedViewServiceServer
	authzv3.UnimplementedDefinitionServiceServer
	w *World
}

func (m authzMember) Verify(_ context.Context, in *authzv3.VerifyRequest) (*authzv3.VerifyResponse, error) {
	raw, digest, err := m.w.view()
	if err != nil {
		return nil, err
	}
	out := &authzv3.VerifyResponse{Nonce: in.GetNonce(), ComputedAt: canon.Time(time.Now()), Instance: Instance,
		RecoveryGeneration: &authzv3.RevisionRef{Domain: "authz.recovery", Value: "1"}}
	for _, h := range in.GetHeld() {
		if h.GetShard() == "all" && h.GetDigest() == digest {
			out.Shards = append(out.Shards, &authzv3.ShardAnswer{Shard: "all", Status: authzv3.ShardStatus_SHARD_STATUS_UNCHANGED})
			return out, nil
		}
	}
	out.Shards = append(out.Shards, &authzv3.ShardAnswer{Shard: "all", Status: authzv3.ShardStatus_SHARD_STATUS_CHANGED,
		View: raw, Digest: digest})
	return out, nil
}

// Submit checks every definition as the member must (the contract's README) except the registry, and remembers
// the accepted set per component and version.
func (m authzMember) Submit(_ context.Context, in *authzv3.SubmitRequest) (*authzv3.SubmitResponse, error) {
	w := m.w
	reject := func(reason, digest, detail string) *authzv3.SubmitResponse {
		return &authzv3.SubmitResponse{Status: authzv3.SubmitStatus_SUBMIT_STATUS_REJECTED,
			Rejections: []*authzv3.Rejection{{Reason: reason, Digest: digest, Detail: detail}}}
	}
	w.mu.Lock()
	refuse := w.RefuseDefinitions
	w.mu.Unlock()
	if refuse != "" {
		return reject(refuse, "", "refused by the test"), nil
	}
	var sealed []authz.Sealed
	for _, d := range in.GetDefinitions() {
		var head struct {
			RecordType string `json:"record_type"`
			Authority  string `json:"authority"`
		}
		if err := json.Unmarshal(d.GetRecord(), &head); err != nil {
			return reject("DEFINITION_INVALID", d.GetDigest(), err.Error()), nil
		}
		if head.RecordType != "be.security.resource_definition" && head.RecordType != "be.security.operation_definition" {
			return reject("DEFINITION_INVALID", d.GetDigest(), "record type "+head.RecordType), nil
		}
		if _, err := records.Open(head.RecordType, d.GetRecord(), d.GetDigest()); err != nil {
			return reject("DEFINITION_INVALID", d.GetDigest(), err.Error()), nil
		}
		if head.Authority != in.GetComponent() {
			return reject("DEFINITION_OWNER_MISMATCH", d.GetDigest(), "authority "+head.Authority), nil
		}
		sealed = append(sealed, authz.Sealed{Digest: d.GetDigest()})
	}
	set, err := authz.SetDigest(in.GetComponent(), in.GetVersion(), sealed)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	key := in.GetComponent() + "@" + in.GetVersion()
	if prev, ok := w.submitted[key]; ok && prev != set {
		return reject("DEFINITION_VERSION_CONFLICT", "", "another set for this version"), nil
	}
	w.submitted[key] = set
	return &authzv3.SubmitResponse{Status: authzv3.SubmitStatus_SUBMIT_STATUS_ACCEPTED, SetDigest: set}, nil
}

// Submitted returns the accepted definition set's digest for a component's version, if any.
func (w *World) Submitted(component, version string) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	d, ok := w.submitted[component+"@"+version]
	return d, ok
}

// Refuse makes the fake member refuse every submission with a reason ("" accepts again).
func (w *World) Refuse(reason string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.RefuseDefinitions = reason
}

func (w *World) startAuthz(t testing.TB) {
	tls := w.TLS(t, "infra/authz")
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tls.ServerConfig())))
	authzv3.RegisterViewServiceServer(srv, authzMember{w: w})
	authzv3.RegisterDefinitionServiceServer(srv, authzMember{w: w})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	w.AuthzAddr = ln.Addr().String()
}

// Audited returns the records the fake audit component received, by ID.
func (w *World) Audited() map[string]json.RawMessage {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := map[string]json.RawMessage{}
	for k, v := range w.audited {
		out[k] = v
	}
	return out
}

type auditMember struct {
	auditv3.UnimplementedIntakeServiceServer
	w *World
}

// Submit stores records idempotently by ID and checks them as the audit component must (contract/README.md).
func (m auditMember) Submit(_ context.Context, in *auditv3.SubmitRequest) (*auditv3.SubmitResponse, error) {
	w := m.w
	w.mu.Lock()
	defer w.mu.Unlock()
	out := &auditv3.SubmitResponse{}
	for _, it := range in.GetItems() {
		rec, err := records.Open("be.audit.record", it.GetRecord(), it.GetDigest())
		switch {
		case err != nil || rec["id"] != it.GetId():
			out.Receipts = append(out.Receipts, &auditv3.Receipt{Id: it.GetId(), Status: auditv3.ReceiptStatus_RECEIPT_STATUS_INVALID})
			continue
		case w.auditDigests[it.GetId()] != "" && w.auditDigests[it.GetId()] != it.GetDigest():
			out.Receipts = append(out.Receipts, &auditv3.Receipt{Id: it.GetId(), Status: auditv3.ReceiptStatus_RECEIPT_STATUS_CONFLICT})
			continue
		}
		if _, seen := w.audited[it.GetId()]; !seen {
			w.audited[it.GetId()] = json.RawMessage(it.GetRecord())
			w.auditDigests[it.GetId()] = it.GetDigest()
		}
		out.Receipts = append(out.Receipts, &auditv3.Receipt{Id: it.GetId(), Status: auditv3.ReceiptStatus_RECEIPT_STATUS_RECEIVED,
			Epoch: "1.1", Seq: int64(len(w.audited)), Chain: "sha256:test"})
	}
	return out, nil
}

// Watermark: nothing is backed up, so nothing is deleted.
func (m auditMember) Watermark(context.Context, *auditv3.WatermarkRequest) (*auditv3.WatermarkResponse, error) {
	return &auditv3.WatermarkResponse{Epoch: "1.1"}, nil
}

func (w *World) startAudit(t testing.TB) {
	tls := w.TLS(t, "infra/audit")
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tls.ServerConfig())))
	auditv3.RegisterIntakeServiceServer(srv, auditMember{w: w})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	w.AuditAddr = ln.Addr().String()
}
