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
	"sync"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
}

// New starts the issuer and the fake authorization member.
func New(t testing.TB) *World {
	t.Helper()
	w := &World{Deployment: "0190f3a1-0000-7000-8000-00000000d001", dir: t.TempDir(), subjects: map[string]authz.SubjectEntry{},
		types: map[string]bool{}, gen: 1, audited: map[string]json.RawMessage{}}
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
	t.Helper()
	k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	u, _ := url.Parse(identity.SPIFFE(deployment, component))
	w.mu.Lock()
	serial++
	n := serial
	w.mu.Unlock()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(n), Subject: pkix.Name{CommonName: component}, URIs: []*url.URL{u},
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

func (w *World) answer(req authz.VerifyRequest) authz.VerifyAnswer {
	w.mu.Lock()
	defer w.mu.Unlock()
	shard := authz.Shard{Shard: "all", RecoveryGeneration: authz.Revision{Domain: "authz.recovery", Value: "1"},
		Subjects: map[string]authz.SubjectEntry{}, Digest: "sha256:" + strconv.Itoa(w.gen)}
	for typ := range w.types {
		shard.Applicability = append(shard.Applicability, typ)
	}
	for p, e := range w.subjects {
		shard.Subjects[p] = e
	}
	return authz.VerifyAnswer{Nonce: req.Nonce, ComputedAt: time.Now().UTC().Format(time.RFC3339Nano),
		RecoveryGeneration: shard.RecoveryGeneration, Shards: []authz.ShardAnswer{{Shard: "all", Status: "changed", View: &shard}}}
}

type viewServer interface{}

func (w *World) startAuthz(t testing.TB) {
	tls := w.TLS(t, "infra/authz")
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tls.ServerConfig())))
	srv.RegisterService(&grpc.ServiceDesc{ServiceName: "be.authz.v3.ViewService", HandlerType: (*viewServer)(nil),
		Methods: []grpc.MethodDesc{{MethodName: "Verify", Handler: func(_ any, ctx context.Context, dec func(any) error,
			_ grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.BytesValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			var req authz.VerifyRequest
			if err := json.Unmarshal(in.Value, &req); err != nil {
				return nil, err
			}
			b, _ := json.Marshal(w.answer(req))
			return wrapperspb.Bytes(b), nil
		}}}}, struct{}{})
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

func (w *World) startAudit(t testing.TB) {
	tls := w.TLS(t, "infra/audit")
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tls.ServerConfig())))
	handle := func(fn func([]byte) any) func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
		return func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.BytesValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			b, _ := json.Marshal(fn(in.Value))
			return wrapperspb.Bytes(b), nil
		}
	}
	srv.RegisterService(&grpc.ServiceDesc{ServiceName: "be.audit.v3.Intake", HandlerType: (*viewServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Submit", Handler: handle(func(body []byte) any {
				var recs []struct {
					ID     string          `json:"id"`
					Record json.RawMessage `json:"record"`
				}
				_ = json.Unmarshal(body, &recs)
				w.mu.Lock()
				defer w.mu.Unlock()
				var out []map[string]any
				for _, r := range recs {
					w.audited[r.ID] = r.Record
					out = append(out, map[string]any{"id": r.ID, "status": "received", "epoch": "1.1",
						"seq": len(w.audited), "chain": "sha256:test"})
				}
				return out
			})},
			{MethodName: "Watermark", Handler: handle(func([]byte) any { return map[string]any{"Epoch": "1.1", "Seq": 0} })},
		}}, struct{}{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	w.AuditAddr = ln.Addr().String()
}
