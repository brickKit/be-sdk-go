// Package runprofile writes and reads the run profile, the deployment's append-only records outside the deployment,
// and sends to the alert channel (be-protocol chapter 12): an object-locked bucket, one signed object per record,
// kinds with their retention, the recovery generations read as the largest valid value and advanced to the largest
// seen anywhere plus one.
package runprofile

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/s3sig"
)

// RecordType is every run profile record's type (P12.2).
const RecordType = "be.ops.run_profile_record"

// The kinds this SDK writes (P12.3).
const (
	KindDeploymentGeneration = "generation.deployment"
	KindAuthzGeneration      = "generation.authz"
	KindAlert                = "alert"
	KindRecoveryStep         = "recovery.step"
)

// Retention is each kind's starting retention (P12.3): a deployment may lengthen it, never shorten it.
func Retention(kind string) time.Duration {
	switch {
	case strings.HasPrefix(kind, "generation."), strings.HasPrefix(kind, "trust."):
		return 3650 * 24 * time.Hour
	default: // alert, recovery.step: no shorter than the full physical backup's retention
		return 30 * 24 * time.Hour
	}
}

// AlertTimeout is each alert copy's time limit (assembly protocol 15's starting value, P12.8).
const AlertTimeout = 2 * time.Second

// Writer is who wrote a record.
type Writer struct {
	Component string `json:"component"`
	Instance  string `json:"instance"`
}

// Signature is a record's signature (P12.2).
type Signature struct {
	Alg       string `json:"alg"`
	Value     string `json:"value"`      // base64
	CertChain string `json:"cert_chain"` // PEM, leaf first
}

// Record is one run profile record.
type Record struct {
	RecordType string          `json:"record_type"`
	Schema     string          `json:"schema"`
	Deployment string          `json:"deployment"`
	Kind       string          `json:"kind"`
	ID         string          `json:"id"`
	WrittenAt  string          `json:"written_at"`
	Writer     Writer          `json:"writer"`
	Body       json.RawMessage `json:"body"`
	Signature  *Signature      `json:"signature,omitempty"`
}

// Identity signs with the member's service credential and checks other signers against the trust root (identity's
// ServiceTLS).
type Identity interface {
	SigningKey() (crypto.Signer, [][]byte, error)
	VerifySigner(chain [][]byte) (string, error)
}

// Options build a Store.
type Options struct {
	Endpoint, Region, Bucket, AccessKey string
	Secret                              func() string // read at every request: a rotated secret applies from the next
	Deployment                          string
	Writer                              Writer
	Identity                            Identity
	Webhook                             string // the alert channel's notification (optional)
	Client                              *http.Client
	Now                                 func() time.Time
	Log                                 func(msg string, args ...any) // severe: the double failure (P12.8)

	retention func(kind string) time.Duration // the package's own tests only: objects that expire in minutes
	newID     func() string                   // the package's own tests only: a fixed record identity
}

// Store is one member's run profile.
type Store struct {
	o      Options
	signer *s3sig.Signer
}

// New builds a store.
func New(o Options) (*Store, error) {
	if o.Bucket == "" || o.Deployment == "" || o.Identity == nil || o.Secret == nil {
		return nil, errors.New("run profile: the bucket, deployment, identity and secret are required")
	}
	if o.Region == "" {
		o.Region = "us-east-1"
	}
	if o.Client == nil {
		o.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Log == nil {
		o.Log = func(string, ...any) {}
	}
	s, err := s3sig.New(o.Endpoint, o.Region, o.AccessKey)
	if err != nil {
		return nil, err
	}
	return &Store{o: o, signer: s}, nil
}

// Keys are the run profile's and the alert channel's configuration keys, for the components that write evidence
// intents (component.Definition.Evidence). All optional: without RUN_PROFILE_URL there is no alert channel.
func Keys() []config.Key {
	return []config.Key{
		{Name: "RUN_PROFILE_URL", Type: config.String},    // the object store, http(s)://host[:port]
		{Name: "RUN_PROFILE_BUCKET", Type: config.String}, // the object-locked bucket
		{Name: "RUN_PROFILE_REGION", Type: config.String, Default: "us-east-1"},
		{Name: "RUN_PROFILE_ACCESS_KEY", Type: config.String},
		{Name: "RUN_PROFILE_SECRET_KEY", Type: config.String, Secret: true},
		{Name: "ALERT_WEBHOOK_URL", Type: config.String}, // the alert channel's notification
	}
}

// Key is a record's object key (P12.1).
func Key(deployment, kind, writtenAt, id string) string {
	return deployment + "/" + kind + "/" + writtenAt[0:4] + "/" + writtenAt[5:7] + "/" + writtenAt[8:10] + "/" + writtenAt + "-" + id + ".json"
}

// SignedBytes are a record's bytes to sign: the canonical encoding without the signature (P12.2).
func SignedBytes(r Record) ([]byte, error) {
	r.Signature = nil
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber() // integers only: the canonical encoding refuses floating point
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	return canon.Encode(v)
}

// Append signs and writes one record of a kind: a conditional create with the kind's retention under compliance-mode
// object lock; done when the store answers success (P12.1-P12.3).
func (s *Store) Append(ctx context.Context, kind string, body any) (Record, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return Record{}, err
	}
	id := ids.New().String()
	if s.o.newID != nil {
		id = s.o.newID()
	}
	r := Record{RecordType: RecordType, Schema: "1.0", Deployment: s.o.Deployment, Kind: kind, ID: id,
		WrittenAt: canon.Time(s.o.Now()), Writer: s.o.Writer, Body: b}
	if r.Signature, err = s.sign(r); err != nil {
		return Record{}, err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return Record{}, err
	}
	sum, md := sha256.Sum256(data), md5.Sum(data)
	keep := Retention(kind)
	if s.o.retention != nil {
		keep = s.o.retention(kind)
	}
	until := s.o.Now().Add(keep).UTC().Format(time.RFC3339)
	extra := map[string]string{"content-type": "application/json", "content-md5": base64.StdEncoding.EncodeToString(md[:]),
		"if-none-match": "*", "x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": until}
	_, err = s.do(ctx, http.MethodPut, Key(r.Deployment, kind, r.WrittenAt, r.ID), "", extra, data, hex.EncodeToString(sum[:]))
	return r, err
}

func (s *Store) sign(r Record) (*Signature, error) {
	key, chain, err := s.o.Identity.SigningKey()
	if err != nil {
		return nil, err
	}
	msg, err := SignedBytes(r)
	if err != nil {
		return nil, err
	}
	var sig []byte
	var alg string
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
	var pemChain bytes.Buffer
	for _, c := range chain {
		_ = pem.Encode(&pemChain, &pem.Block{Type: "CERTIFICATE", Bytes: c})
	}
	return &Signature{Alg: alg, Value: base64.StdEncoding.EncodeToString(sig), CertChain: pemChain.String()}, nil
}

// do sends one signed request; any answer but 2xx is an error.
func (s *Store) do(ctx context.Context, method, key, query string, extra map[string]string, body []byte,
	payload string) ([]byte, error) {
	u, headers := s.signer.Request(s.o.Secret(), s.o.Now(), method, s.o.Bucket, key, query, extra, payload)
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for n, v := range headers {
		req.Header.Set(n, v)
	}
	resp, err := s.o.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode/100 != 2 {
		return nil, &StatusError{Status: resp.StatusCode, Body: string(out)}
	}
	return out, nil
}

// StatusError is an answer of the object store other than success (412: the key exists).
type StatusError struct {
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("run profile: the object store answered %d: %.200s", e.Status, e.Body)
}

// unsigned is the payload hash of the bodyless requests.
const unsigned = "UNSIGNED-PAYLOAD"

// List reads every record of a kind and keeps those whose signature holds against the trust root; the rest are
// returned as problems for the caller to alert on (P12.4). err: the listing itself failed (nothing is known).
func (s *Store) List(ctx context.Context, kind string) (recs []Record, problems []error, err error) {
	var keys []string
	token := ""
	for {
		q := "list-type=2&prefix=" + s3sig.Escape(s.o.Deployment+"/"+kind+"/", false)
		if token != "" {
			q = "continuation-token=" + s3sig.Escape(token, false) + "&" + q
		}
		raw, err := s.do(ctx, http.MethodGet, "", q, nil, nil, unsigned)
		if err != nil {
			return nil, nil, err
		}
		var page struct {
			Contents []struct{ Key string } `xml:"Contents"`
			Next     string                 `xml:"NextContinuationToken"`
			More     bool                   `xml:"IsTruncated"`
		}
		if err := xml.Unmarshal(raw, &page); err != nil {
			return nil, nil, err
		}
		for _, c := range page.Contents {
			keys = append(keys, c.Key)
		}
		if !page.More {
			break
		}
		token = page.Next
	}
	for _, k := range keys {
		raw, err := s.do(ctx, http.MethodGet, k, "", nil, nil, unsigned)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		var r Record
		if err := json.Unmarshal(raw, &r); err != nil {
			problems = append(problems, fmt.Errorf("run profile: %s: %w", k, err))
			continue
		}
		if err := s.Verify(r); err != nil {
			problems = append(problems, fmt.Errorf("run profile: %s: %w", k, err))
			continue
		}
		recs = append(recs, r)
	}
	return recs, problems, nil
}

// Verify checks a record's signature and its signer's chain against the trust root (P12.2, P12.4).
func (s *Store) Verify(r Record) error {
	if r.Signature == nil {
		return errors.New("no signature")
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
		return errors.New("no certificate chain")
	}
	if _, err := s.o.Identity.VerifySigner(chain); err != nil {
		return fmt.Errorf("the signer's chain does not hold: %w", err)
	}
	leaf, err := x509.ParseCertificate(chain[0])
	if err != nil {
		return err
	}
	msg, err := SignedBytes(r)
	if err != nil {
		return err
	}
	sig, err := base64.StdEncoding.DecodeString(r.Signature.Value)
	if err != nil {
		return err
	}
	switch pub := leaf.PublicKey.(type) {
	case *ecdsa.PublicKey:
		d := sha256.Sum256(msg)
		if r.Signature.Alg == "ES256" && ecdsa.VerifyASN1(pub, d[:], sig) {
			return nil
		}
	case ed25519.PublicKey:
		if r.Signature.Alg == "EdDSA" && ed25519.Verify(pub, msg, sig) {
			return nil
		}
	}
	return errors.New("the signature does not hold")
}

// Current is a recovery generation's current value: the largest body.value among the valid records, 0 for none
// (P12.4).
func (s *Store) Current(ctx context.Context, kind string) (int64, []error, error) {
	recs, problems, err := s.List(ctx, kind)
	if err != nil {
		return 0, nil, err // the store did not answer: no value is known
	}
	return currentOf(recs), problems, nil
}

func currentOf(recs []Record) int64 {
	var max int64
	for _, r := range recs {
		var b struct{ Value int64 }
		if json.Unmarshal(r.Body, &b) == nil && b.Value > max {
			max = b.Value
		}
	}
	return max
}

// Advance moves a recovery generation on: the largest value seen in the run profile and elsewhere (the evidence
// store, the databases), plus one, written to the run profile before it is used; no write, no advance (P12.5).
func (s *Store) Advance(ctx context.Context, kind string, seen ...int64) (int64, error) {
	cur, _, err := s.Current(ctx, kind)
	if err != nil {
		return 0, err
	}
	next := NextGeneration(cur, seen...)
	if _, err := s.Append(ctx, kind, map[string]int64{"value": next}); err != nil {
		return 0, err
	}
	return next, nil
}

// NextGeneration is the largest value plus one (P12.5).
func NextGeneration(runProfile int64, seen ...int64) int64 {
	m := runProfile
	for _, v := range seen {
		m = max(m, v)
	}
	return m + 1
}

// Alert sends a record to the alert channel: its machine-readable copy in the run profile and a notification to the
// webhook, each within AlertTimeout, neither waiting for the other; when the run profile copy fails it logs the
// double failure and the caller goes on (P12.7, P12.8). It returns whether the run profile copy was written.
func (s *Store) Alert(ctx context.Context, body any) bool {
	done := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(context.WithoutCancel(ctx), AlertTimeout)
		defer cancel()
		_, err := s.Append(c, KindAlert, body)
		done <- err
	}()
	if s.o.Webhook != "" {
		go func() {
			c, cancel := context.WithTimeout(context.WithoutCancel(ctx), AlertTimeout)
			defer cancel()
			b, _ := json.Marshal(map[string]any{"deployment": s.o.Deployment, "writer": s.o.Writer, "alert": body})
			if req, err := http.NewRequestWithContext(c, http.MethodPost, s.o.Webhook, bytes.NewReader(b)); err == nil {
				req.Header.Set("Content-Type", "application/json")
				if resp, err := s.o.Client.Do(req); err == nil {
					resp.Body.Close()
				}
			}
		}()
	}
	if err := <-done; err != nil {
		s.o.Log("the alert channel could not be written either", "event", "evidence_double_failure", "error", err.Error())
		return false
	}
	return true
}
