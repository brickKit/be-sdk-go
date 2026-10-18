package events

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

// AWS Signature Version 4 for the two S3 requests the claim check makes (P6.14): an object PUT signed in its headers,
// and a presigned GET. Path-style addressing (<endpoint>/<bucket>/<key>), as every S3-compatible store accepts.

const (
	sigAlgorithm = "AWS4-HMAC-SHA256"
	unsignedBody = "UNSIGNED-PAYLOAD"
)

// signer signs for one endpoint, region and access key; the secret is passed at every call, so a rotated secret is
// used from the next request.
type signer struct {
	scheme, host, base, region, access string

	mu      sync.Mutex
	keyFor  [2]string // the secret and date the cached key was derived for
	keyHMAC []byte
}

func newSigner(endpoint, region, access string) (*signer, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("object storage: OBJECT_STORE_URL %q is not an http(s) address", endpoint)
	}
	return &signer{scheme: u.Scheme, host: u.Host, base: strings.TrimRight(u.Path, "/"), region: region, access: access}, nil
}

func hmacSHA256(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

// signingKey is derived once a day per secret.
func (s *signer) signingKey(secret, date string) []byte {
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

// escape percent-encodes everything but the unreserved characters (RFC 3986), and "/" too unless keepSlash.
func escape(s string, keepSlash bool) string {
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
func (s *signer) path(bucket, key string) string {
	return s.base + "/" + escape(bucket, false) + "/" + escape(key, true)
}

func (s *signer) sign(secret string, when time.Time, method, path, query string, headers map[string]string,
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

// putHeaders are the URL and headers of a signed PUT of an object of size bytes whose body has the given SHA-256
// (hex); the length is signed too, as the AWS SDK's signer does.
func (s *signer) putHeaders(secret, bucket, key, payloadSHA256, contentType string, size int, when time.Time) (string, map[string]string) {
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

// presignGet is a read-only GET of the object, valid for expires, that needs no credentials to use.
func (s *signer) presignGet(secret, bucket, key string, expires time.Duration, when time.Time) string {
	amz, date := when.UTC().Format("20060102T150405Z"), when.UTC().Format("20060102")
	path := s.path(bucket, key)
	params := [][2]string{{"X-Amz-Algorithm", sigAlgorithm},
		{"X-Amz-Credential", s.access + "/" + date + "/" + s.region + "/s3/aws4_request"},
		{"X-Amz-Date", amz}, {"X-Amz-Expires", fmt.Sprint(int64(expires / time.Second))},
		{"X-Amz-SignedHeaders", "host"}}
	parts := make([]string, len(params))
	for i, p := range params { // already in sorted order
		parts[i] = escape(p[0], false) + "=" + escape(p[1], false)
	}
	query := strings.Join(parts, "&")
	sig, _, _ := s.sign(secret, when, "GET", path, query, map[string]string{"host": s.host}, unsignedBody)
	return s.scheme + "://" + s.host + path + "?" + query + "&X-Amz-Signature=" + sig
}
