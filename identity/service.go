package identity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// SPIFFE is the service identity of a component in a deployment (P8.5).
func SPIFFE(deployment, component string) string {
	return "spiffe://" + deployment + "/component/" + component
}

// DNSName is the DNS SAN derived from the SPIFFE ID (P8.5), for clients that can only check a host name.
func DNSName(deployment, component string) string {
	return strings.ReplaceAll(component, "/", ".") + ".component." + deployment
}

// ServiceTLS is a member's mTLS identity: its certificate and the deployment's trust root, re-read from their
// files so a rotation applies to new connections.
type ServiceTLS struct {
	Deployment string
	Component  string
	certFile   string
	keyFile    string
	caFile     string
	read       func(string) ([]byte, error)

	mu   sync.RWMutex
	cert *tls.Certificate
	pool *x509.CertPool
}

// LoadServiceTLS reads the certificate, key and trust root; the certificate must carry this member's identity.
func LoadServiceTLS(deployment, component, certFile, keyFile, caFile string, read func(string) ([]byte, error)) (*ServiceTLS, error) {
	s := &ServiceTLS{Deployment: deployment, Component: component, certFile: certFile, keyFile: keyFile, caFile: caFile, read: read}
	if err := s.Reload(); err != nil {
		return nil, err
	}
	return s, nil
}

// Reload re-reads the files.
func (s *ServiceTLS) Reload() error {
	certPEM, err := s.read(s.certFile)
	if err != nil {
		return fmt.Errorf("service certificate: %w", err)
	}
	keyPEM, err := s.read(s.keyFile)
	if err != nil {
		return fmt.Errorf("service key: %w", err)
	}
	caPEM, err := s.read(s.caFile)
	if err != nil {
		return fmt.Errorf("service trust root: %w", err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("service certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return errors.New("service trust root: no certificate")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	if id := identityOf(leaf); id != SPIFFE(s.Deployment, s.Component) {
		return fmt.Errorf("service certificate: it names %q, this member is %q", id, SPIFFE(s.Deployment, s.Component))
	}
	if want := DNSName(s.Deployment, s.Component); !slices.Contains(leaf.DNSNames, want) {
		return fmt.Errorf("service certificate: no DNS name %q (P8.5)", want)
	}
	s.mu.Lock()
	s.cert, s.pool = &cert, pool
	s.mu.Unlock()
	return nil
}

// Watch re-reads the files every period until ctx ends.
func (s *ServiceTLS) Watch(ctx context.Context, period time.Duration, onError func(error)) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.Reload(); err != nil && onError != nil {
				onError(err)
			}
		}
	}
}

func identityOf(c *x509.Certificate) string {
	for _, u := range c.URIs {
		if u.Scheme == "spiffe" {
			return u.String()
		}
	}
	return ""
}

func (s *ServiceTLS) current() (*tls.Certificate, *x509.CertPool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cert, s.pool
}

// verify checks a peer chain against the current trust root and returns the peer's identity.
func (s *ServiceTLS) verify(raw [][]byte, usage x509.ExtKeyUsage) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("no certificate")
	}
	certs := make([]*x509.Certificate, len(raw))
	for i, r := range raw {
		c, err := x509.ParseCertificate(r)
		if err != nil {
			return "", err
		}
		certs[i] = c
	}
	_, pool := s.current()
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	if _, err := certs[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
		return "", err
	}
	id := identityOf(certs[0])
	if !strings.HasPrefix(id, "spiffe://"+s.Deployment+"/component/") {
		return "", fmt.Errorf("the peer %q is not a component of this deployment", id)
	}
	return id, nil
}

// ServerConfig requires and verifies a client certificate of this deployment.
func (s *ServiceTLS) ServerConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			c, _ := s.current()
			return c, nil
		},
		ClientAuth: tls.RequireAnyClientCert, // verified below against the current trust root
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			_, err := s.verify(raw, x509.ExtKeyUsageClientAuth)
			return err
		},
	}
}

// ClientConfig presents this member's certificate and accepts only the target component's server certificate.
func (s *ServiceTLS) ClientConfig(target string) *tls.Config {
	want := SPIFFE(s.Deployment, target)
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			c, _ := s.current()
			return c, nil
		},
		InsecureSkipVerify: true, // replaced by VerifyPeerCertificate: the identity is a URI SAN, not a host name
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			id, err := s.verify(raw, x509.ExtKeyUsageServerAuth)
			if err != nil {
				return err
			}
			if id != want {
				return fmt.Errorf("the server is %q, not %q", id, want)
			}
			return nil
		},
	}
}

// Caller returns the component ID a system-surface call came from, as its verified certificate says.
func Caller(ctx context.Context, deployment string) (string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", false
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.PeerCertificates) == 0 {
		return "", false
	}
	id := identityOf(info.State.PeerCertificates[0])
	prefix := "spiffe://" + deployment + "/component/"
	if !strings.HasPrefix(id, prefix) {
		return "", false
	}
	u, err := url.Parse(id)
	if err != nil {
		return "", false
	}
	return strings.TrimPrefix(u.Path, "/component/"), true
}
