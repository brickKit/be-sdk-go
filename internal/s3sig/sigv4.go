package s3sig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Package s3sig is AWS Signature Version 4 for the SDK's own S3 requests: the claim check's object PUT and presigned
// GET (P6.14), and the run profile's conditional, object-locked PUT, signed GET and listing (chapter 12). Path-style
// addressing (<endpoint>/<bucket>/<key>), as every S3-compatible store accepts. Only for the SDK: through the AWS SDK
// an upload with presigning cost about 1.2 ms of CPU against 0.7 ms (events/objectstore.go).

const (
	sigAlgorithm = "AWS4-HMAC-SHA256"
	unsignedBody = "UNSIGNED-PAYLOAD"
)

// Signer signs for one endpoint, region and access key; the secret is passed at every call, so a rotated secret is
// used from the next request.
type Signer struct {
	scheme, host, base, region, access string

	mu      sync.Mutex
	keyFor  [2]string // the secret and date the cached key was derived for
	keyHMAC []byte
}

// New is a signer for an endpoint (http(s)://host[:port][/base]), region and access key.
func New(endpoint, region, access string) (*Signer, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("object storage: %q is not an http(s) address", endpoint)
	}
	return &Signer{scheme: u.Scheme, host: u.Host, base: strings.TrimRight(u.Path, "/"), region: region, access: access}, nil
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

// signingKey is derived once a day per secret.
func (s *Signer) signingKey(secret, date string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keyHMAC == nil || s.keyFor != [2]string{secret, date} {
		k := hmacSHA256([]byte("AWS4"+secret), date)
		k = hmacSHA256(k, s.region)
		k = hmacSHA256(k, "s3")
		s.keyHMAC, s.keyFor = hmacSHA256(k, "aws4_request"), [2]string{secret, date}
	}
	return s.keyHMAC
}

// Escape percent-encodes everything but the unreserved characters (RFC 3986), and "/" too unless keepSlash.
func Escape(s string, keepSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'A' <= c && c <= 'Z', 'a' <= c && c <= 'z', '0' <= c && c <= '9', c == '-', c == '_', c == '.', c == '~',
			c == '/' && keepSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// path is the canonical path: each segment percent-encoded once (S3 does not encode twice); "/" stays.
func (s *Signer) path(bucket, key string) string {
	return s.base + "/" + Escape(bucket, false) + "/" + Escape(key, true)
}

func (s *Signer) sign(secret string, when time.Time, method, path, query string, headers map[string]string,
	payloadHash string) (signature, scope, signed string) {
	amz, date := when.UTC().Format("20060102T150405Z"), when.UTC().Format("20060102")
	names := make([]string, 0, len(headers))
	for n := range headers {
		names = append(names, n)
	}
	sort.Strings(names)
	var canon strings.Builder
	canon.WriteString(method + "\n" + path + "\n" + query + "\n")
	for _, n := range names {
		canon.WriteString(n + ":" + headers[n] + "\n")
	}
	signed = strings.Join(names, ";")
	canon.WriteString("\n" + signed + "\n" + payloadHash)
	sum := sha256.Sum256([]byte(canon.String()))
	scope = date + "/" + s.region + "/s3/aws4_request"
	toSign := sigAlgorithm + "\n" + amz + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	return hex.EncodeToString(hmacSHA256(s.signingKey(secret, date), toSign)), scope, signed
}

// PutHeaders are the URL and headers of a signed PUT of an object of size bytes whose body has the given SHA-256
// (hex); the length is signed too, as the AWS SDK's signer does.
func (s *Signer) PutHeaders(secret, bucket, key, payloadSHA256, contentType string, size int, when time.Time) (string, map[string]string) {
	amz := when.UTC().Format("20060102T150405Z")
	path := s.path(bucket, key)
	h := map[string]string{"host": s.host, "x-amz-content-sha256": payloadSHA256, "x-amz-date": amz,
		"content-type": contentType, "content-length": fmt.Sprint(size)}
	sig, scope, signed := s.sign(secret, when, "PUT", path, "", h, payloadSHA256)
	return s.scheme + "://" + s.host + path, map[string]string{
		"X-Amz-Content-Sha256": payloadSHA256, "X-Amz-Date": amz, "Content-Type": contentType,
		"Authorization": sigAlgorithm + " Credential=" + s.access + "/" + scope + ", SignedHeaders=" + signed +
			", Signature=" + sig}
}

// PresignGet is a read-only GET of the object, valid for expires, that needs no credentials to use.
func (s *Signer) PresignGet(secret, bucket, key string, expires time.Duration, when time.Time) string {
	amz, date := when.UTC().Format("20060102T150405Z"), when.UTC().Format("20060102")
	path := s.path(bucket, key)
	params := [][2]string{{"X-Amz-Algorithm", sigAlgorithm},
		{"X-Amz-Credential", s.access + "/" + date + "/" + s.region + "/s3/aws4_request"},
		{"X-Amz-Date", amz}, {"X-Amz-Expires", fmt.Sprint(int64(expires / time.Second))},
		{"X-Amz-SignedHeaders", "host"}}
	parts := make([]string, len(params))
	for i, p := range params { // already in sorted order
		parts[i] = Escape(p[0], false) + "=" + Escape(p[1], false)
	}
	query := strings.Join(parts, "&")
	sig, _, _ := s.sign(secret, when, "GET", path, query, map[string]string{"host": s.host}, unsignedBody)
	return s.scheme + "://" + s.host + path + "?" + query + "&X-Amz-Signature=" + sig
}

// Request signs any request in its headers: the method, the object (key "" for the bucket itself), the canonical query
// (already sorted and escaped, "" for none), the extra headers to sign (lowercase names; x-amz-* headers must be among
// them) and the body's SHA-256 (hex). It returns the URL and every header to set.
func (s *Signer) Request(secret string, when time.Time, method, bucket, key, query string, extra map[string]string,
	payloadSHA256 string) (string, map[string]string) {
	amz := when.UTC().Format("20060102T150405Z")
	path := s.base + "/" + Escape(bucket, false)
	if key != "" {
		path += "/" + Escape(key, true)
	}
	h := map[string]string{"host": s.host, "x-amz-content-sha256": payloadSHA256, "x-amz-date": amz}
	for n, v := range extra {
		h[n] = v
	}
	sig, scope, signed := s.sign(secret, when, method, path, query, h, payloadSHA256)
	out := map[string]string{}
	for n, v := range h {
		if n != "host" {
			out[n] = v
		}
	}
	out["authorization"] = sigAlgorithm + " Credential=" + s.access + "/" + scope + ", SignedHeaders=" + signed +
		", Signature=" + sig
	u := s.scheme + "://" + s.host + path
	if query != "" {
		u += "?" + query
	}
	return u, out
}

// Presign is a request that needs no credentials to use, valid for expires: the method on the object, the headers
// the user must send exactly as given (lowercase names; host is always signed), and extra query parameters (the
// response-* overrides of a GET). The storage port's presigned upload signs content-length and content-type; its
// presigned download sets response-content-disposition (be-protocol P19.6, P19.7).
func (s *Signer) Presign(secret, method, bucket, key string, expires time.Duration, when time.Time,
	headers map[string]string, query map[string]string) string {
	amz, date := when.UTC().Format("20060102T150405Z"), when.UTC().Format("20060102")
	path := s.path(bucket, key)
	signed := map[string]string{"host": s.host}
	for n, v := range headers {
		signed[n] = v
	}
	names := make([]string, 0, len(signed))
	for n := range signed {
		names = append(names, n)
	}
	sort.Strings(names)
	params := map[string]string{"X-Amz-Algorithm": sigAlgorithm,
		"X-Amz-Credential": s.access + "/" + date + "/" + s.region + "/s3/aws4_request", "X-Amz-Date": amz,
		"X-Amz-Expires": fmt.Sprint(int64(expires / time.Second)), "X-Amz-SignedHeaders": strings.Join(names, ";")}
	for n, v := range query {
		params[n] = v
	}
	keys := make([]string, 0, len(params))
	for n := range params {
		keys = append(keys, n)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, n := range keys {
		parts[i] = Escape(n, false) + "=" + Escape(params[n], false)
	}
	q := strings.Join(parts, "&")
	sig, _, _ := s.sign(secret, when, method, path, q, signed, unsignedBody)
	return s.scheme + "://" + s.host + path + "?" + q + "&X-Amz-Signature=" + sig
}
