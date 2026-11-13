// Package storage is the SDK's storage port (be-protocol chapter 19): an owner component's objects in its own bucket
// and prefix (OBJECT_STORE_*, chapter 02), with the conditional writes, the server-side copy, the presigned upload and
// download and the error classes the attachments need (assembly protocol 12). Requests are signed with the SDK's own
// SigV4 (internal/s3sig, as the large events and the run profile).
package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/internal/s3sig"
)

// The error classes (P19.9).
var (
	ErrNotFound    = errors.New("storage: not found")
	ErrExists      = errors.New("storage: exists")
	ErrTooLarge    = errors.New("storage: too large")
	ErrUnavailable = errors.New("storage: unavailable")
	ErrDenied      = errors.New("storage: denied")
)

// MaxPresign is the longest a presigned address may live (P19.6, P19.7).
const MaxPresign = time.Hour

const unsignedPayload = "UNSIGNED-PAYLOAD"

var keyForm = regexp.MustCompile(`^[a-z0-9_.-]+(/[a-z0-9_.-]+)*$`)

// Meta is an object's metadata (P19.2); SHA256 is empty for an object written without the port's digest (an upload
// through a presigned address, a copy of one).
type Meta struct {
	Size        int64
	ContentType string
	SHA256      string
	Modified    time.Time
}

// KeyPublicURL is the object store's address as browsers reach it (chapter 02): the presigned addresses are signed
// for it. A component that presigns declares it with Keys.
const KeyPublicURL = "OBJECT_STORE_PUBLIC_URL"

// Keys are the storage port's own keys a presigning component adds to its configuration keys.
func Keys() []config.Key { return []config.Key{{Name: KeyPublicURL, Type: config.String}} }

// Store is one member's view of its own bucket and prefix.
type Store struct {
	Bucket, Prefix string
	signer         *s3sig.Signer
	public         *s3sig.Signer // signs the presigned addresses: the browsers' address of the store
	secret         func() string
	client         *http.Client
	now            func() time.Time
	log            *slog.Logger
	region, access string

	copyOnce sync.Once
	copyErr  error
	native   bool // the store copies on its side with If-None-Match (P19.4)

	condMu    sync.Mutex
	condKnown bool  // the probe got an answer
	condErr   error // the store does not honour If-None-Match on a put: no conditional write at all (P19.3)
}

// From builds the member's store from OBJECT_STORE_*; nil when OBJECT_STORE_URL is not set.
func From(values *config.Values, log *slog.Logger) (*Store, error) {
	url := values.String("OBJECT_STORE_URL")
	if url == "" {
		return nil, nil
	}
	secret := values.Secret("OBJECT_STORE_SECRET_KEY")
	if values.String("OBJECT_STORE_ACCESS_KEY") == "" || secret == nil || values.String("OBJECT_STORE_BUCKET") == "" {
		return nil, errors.New("storage: OBJECT_STORE_BUCKET, OBJECT_STORE_ACCESS_KEY and OBJECT_STORE_SECRET_KEY are " +
			"required with OBJECT_STORE_URL")
	}
	region := values.String("OBJECT_STORE_REGION")
	if region == "" {
		region = "us-east-1"
	}
	s, err := New(url, region, values.String("OBJECT_STORE_BUCKET"), values.String("OBJECT_STORE_PREFIX"),
		values.String("OBJECT_STORE_ACCESS_KEY"), secret.Value, log)
	if err != nil {
		return nil, err
	}
	if pub := values.String(KeyPublicURL); pub != "" {
		if err := s.UsePublic(pub); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// UsePublic signs the presigned addresses for another address of the same store: the one browsers reach (P19.6).
func (s *Store) UsePublic(endpoint string) error {
	p, err := s3sig.New(endpoint, s.region, s.access)
	if err != nil {
		return err
	}
	s.public = p
	return nil
}

// New is a store over an endpoint, a region, a bucket and the owner's prefix, signing with the access key and the
// secret (read at every request, so a rotated secret applies from the next one).
func New(endpoint, region, bucket, prefix, access string, secret func() string, log *slog.Logger) (*Store, error) {
	signer, err := s3sig.New(endpoint, region, access)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Store{Bucket: bucket, Prefix: prefix, signer: signer, public: signer, secret: secret,
		client: &http.Client{Timeout: 5 * time.Minute}, now: time.Now, log: log, region: region, access: access}, nil
}

// full is the store key of a port key (P19.1).
func (s *Store) full(key string) (string, error) {
	if !keyForm.MatchString(key) || strings.Contains(key, "..") {
		return "", fmt.Errorf("storage: %q is not a key (P19.1)", key)
	}
	return s.Prefix + key, nil
}

// do sends one signed request; the body is streamed (unsigned payload) when size >= 0.
func (s *Store) do(ctx context.Context, method, key, query string, extra map[string]string, body io.Reader,
	size int64) (*http.Response, error) {
	u, headers := s.signer.Request(s.secret(), s.now(), method, s.Bucket, key, query, extra, unsignedPayload)
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if size >= 0 {
		req.ContentLength = size
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s: %v", ErrUnavailable, method, key, redact(err.Error()))
	}
	return resp, nil
}

// classify turns an answer into the error classes (P19.9); the store's own words go to the log only.
func (s *Store) classify(op, key string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	resp.Body.Close()
	code := resp.StatusCode
	var cls error
	switch {
	case code == http.StatusNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, key)
	case code == http.StatusPreconditionFailed || code == http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrExists, key)
	case code == http.StatusRequestEntityTooLarge || strings.Contains(string(body), "EntityTooLarge"):
		cls = ErrTooLarge
	case code == http.StatusForbidden || code == http.StatusUnauthorized:
		cls = ErrDenied
	default:
		cls = ErrUnavailable
	}
	s.log.Warn("the object store refused a request", "event", "storage_refused", "op", op, "key", key, "status", code,
		"answer", redact(string(body)))
	return fmt.Errorf("%w: %s %s: %d", cls, op, key, code)
}

// redact cuts a presigned address's signature out of a text (P19.10).
func redact(s string) string {
	if i := strings.Index(s, "X-Amz-Signature="); i >= 0 {
		end := strings.IndexAny(s[i:], "& \"'")
		if end < 0 {
			return s[:i] + "X-Amz-Signature=…"
		}
		return s[:i] + "X-Amz-Signature=…" + s[i+end:]
	}
	return s
}

// Put writes an object from a stream of size bytes (P19.3): ifAbsent refuses an existing key with ErrExists. The
// port's digest of what it wrote is kept as the object's metadata; the digest is answered.
func (s *Store) Put(ctx context.Context, key string, body io.Reader, size int64, contentType string, ifAbsent bool) (string, error) {
	full, err := s.full(key)
	if err != nil {
		return "", err
	}
	// the digest goes in the metadata, so it must be known before the request: read the content once (the port's
	// writes are small; large ones come through presigned uploads)
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return "", err
	}
	if int64(len(data)) != size {
		return "", fmt.Errorf("storage: put %s: %d bytes given for %d", key, len(data), size)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	extra := map[string]string{"content-type": contentType, "x-amz-meta-sha256": digest}
	if ifAbsent {
		if err := s.conditional(ctx); err != nil {
			return "", err
		}
		extra["if-none-match"] = "*"
	}
	resp, err := s.do(ctx, http.MethodPut, full, "", extra, bytes.NewReader(data), size)
	if err != nil {
		return "", err
	}
	if resp.StatusCode/100 != 2 {
		return "", s.classify("put", key, resp)
	}
	resp.Body.Close()
	return digest, nil
}

// conditional finds out once whether the store honours If-None-Match on a put; one that does not gets no conditional
// write at all (P19.3: never an unconditional one in its place).
func (s *Store) conditional(ctx context.Context) error {
	s.condMu.Lock()
	defer s.condMu.Unlock()
	if s.condKnown {
		return s.condErr
	}
	func() { // an unreachable store leaves it unknown: probed again next time
		key := fmt.Sprintf("be-storage-probe/put-%d", s.now().UnixNano())
		full, _ := s.full(key)
		put := func() (*http.Response, error) {
			return s.do(ctx, http.MethodPut, full, "", map[string]string{"content-type": "text/plain", "if-none-match": "*"},
				strings.NewReader("probe"), 5)
		}
		first, err := put()
		if err != nil {
			return
		}
		first.Body.Close()
		second, err := put()
		if err != nil {
			return
		}
		second.Body.Close()
		_ = s.Delete(ctx, key)
		s.condKnown = true
		if second.StatusCode != http.StatusPreconditionFailed && second.StatusCode != http.StatusConflict {
			s.condErr = fmt.Errorf("%w: the object store does not honour If-None-Match on a put", ErrUnavailable)
			s.log.Error("the object store does not honour conditional writes: the storage port refuses them",
				"event", "storage_no_conditional_write")
		}
	}()
	if !s.condKnown {
		return fmt.Errorf("%w: the conditional write could not be probed", ErrUnavailable)
	}
	return s.condErr
}

// Copy writes the source's content to the destination, only when the destination does not exist (P19.4): on the
// store's side when it can; otherwise by reading the source and putting it with the same condition. Either way the
// destination holds what the source held at that moment.
func (s *Store) Copy(ctx context.Context, src, dst string) error {
	fsrc, err := s.full(src)
	if err != nil {
		return err
	}
	fdst, err := s.full(dst)
	if err != nil {
		return err
	}
	s.copyOnce.Do(func() { s.native, s.copyErr = s.probeCopy(ctx) })
	if s.copyErr != nil {
		return s.copyErr
	}
	if s.native {
		return s.copyNative(ctx, fsrc, fdst, dst)
	}
	return s.copyThrough(ctx, src, dst)
}

func (s *Store) copyNative(ctx context.Context, fsrc, fdst, dst string) error {
	// the destination's metadata is the port's, never what a client sent with the source (P19.4)
	extra := map[string]string{"x-amz-copy-source": "/" + s3sig.Escape(s.Bucket, false) + "/" + s3sig.Escape(fsrc, true),
		"if-none-match": "*", "x-amz-metadata-directive": "REPLACE", "content-type": "application/octet-stream"}
	resp, err := s.do(ctx, http.MethodPut, fdst, "", extra, nil, 0)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusPreconditionFailed || resp.StatusCode == http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrExists, dst)
	case resp.StatusCode == http.StatusNotFound:
		return fmt.Errorf("%w: the copy's source", ErrNotFound)
	case resp.StatusCode/100 != 2 || strings.Contains(string(body), "<Error>"): // a copy may fail in a 200's body
		s.log.Warn("the object store refused a copy", "event", "storage_refused", "op", "copy", "key", dst,
			"status", resp.StatusCode, "answer", redact(string(body)))
		return fmt.Errorf("%w: copy %s: %d", ErrUnavailable, dst, resp.StatusCode)
	}
	return nil
}

func (s *Store) copyThrough(ctx context.Context, src, dst string) error {
	r, meta, err := s.Get(ctx, src)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = s.Put(ctx, dst, r, meta.Size, "application/octet-stream", true)
	return err
}

// probeCopy finds out once whether the store honours If-None-Match on a server-side copy: it copies a probe object
// onto itself's twin twice; the second must be refused. The answer is logged (P19.4).
func (s *Store) probeCopy(ctx context.Context) (bool, error) {
	src, dst := "be-storage-probe/src", fmt.Sprintf("be-storage-probe/dst-%d", s.now().UnixNano())
	if _, err := s.Put(ctx, src, strings.NewReader("probe"), 5, "text/plain", false); err != nil {
		return false, err
	}
	fsrc, _ := s.full(src)
	fdst, _ := s.full(dst)
	first := s.copyNative(ctx, fsrc, fdst, dst)
	second := s.copyNative(ctx, fsrc, fdst, dst)
	_ = s.Delete(ctx, dst)
	native := first == nil && errors.Is(second, ErrExists)
	mode := "server-side copy with If-None-Match"
	if !native {
		mode = "read and conditional put (the store's copy does not honour If-None-Match)"
	}
	s.log.Info("the storage port's copy", "event", "storage_copy_mode", "mode", mode)
	return native, nil
}

// Get reads an object as a stream with its metadata (P19.5); ErrNotFound when it is not there.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, Meta, error) {
	full, err := s.full(key)
	if err != nil {
		return nil, Meta{}, err
	}
	resp, err := s.do(ctx, http.MethodGet, full, "", nil, nil, -1)
	if err != nil {
		return nil, Meta{}, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, Meta{}, s.classify("get", key, resp)
	}
	return resp.Body, metaOf(resp), nil
}

// Head answers an object's metadata (P19.5).
func (s *Store) Head(ctx context.Context, key string) (Meta, error) {
	full, err := s.full(key)
	if err != nil {
		return Meta{}, err
	}
	resp, err := s.do(ctx, http.MethodHead, full, "", nil, nil, -1)
	if err != nil {
		return Meta{}, err
	}
	resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return Meta{}, fmt.Errorf("%w: %s", ErrNotFound, key)
	case resp.StatusCode/100 != 2:
		return Meta{}, fmt.Errorf("%w: head %s: %d", ErrUnavailable, key, resp.StatusCode)
	}
	return metaOf(resp), nil
}

func metaOf(resp *http.Response) Meta {
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	mod, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	return Meta{Size: size, ContentType: resp.Header.Get("Content-Type"), SHA256: resp.Header.Get("X-Amz-Meta-Sha256"),
		Modified: mod}
}

// PresignPut is an address that PUTs this key with exactly this length and content type, for expires (P19.6).
func (s *Store) PresignPut(key string, size int64, contentType string, expires time.Duration) (string, error) {
	full, err := s.full(key)
	if err != nil {
		return "", err
	}
	if expires <= 0 || expires > MaxPresign {
		return "", fmt.Errorf("storage: a presigned address lives 1 hour at most")
	}
	return s.public.Presign(s.secret(), http.MethodPut, s.Bucket, full, expires, s.now(),
		map[string]string{"content-length": strconv.FormatInt(size, 10), "content-type": contentType}, nil), nil
}

// PresignGet is an address that GETs this key for expires (P19.7): saved as a file named filename, unless inline
// gives the (allow-listed) type to show it in the browser with.
func (s *Store) PresignGet(key string, expires time.Duration, filename, inline string) (string, error) {
	full, err := s.full(key)
	if err != nil {
		return "", err
	}
	if expires <= 0 || expires > MaxPresign {
		return "", fmt.Errorf("storage: a presigned address lives 1 hour at most")
	}
	query := map[string]string{}
	if inline != "" {
		query["response-content-type"] = inline
		query["response-content-disposition"] = "inline"
	} else {
		query["response-content-disposition"] = mime.FormatMediaType("attachment", map[string]string{"filename": filename})
		query["response-content-type"] = "application/octet-stream"
	}
	return s.public.Presign(s.secret(), http.MethodGet, s.Bucket, full, expires, s.now(), nil, query), nil
}

// Delete removes an object; a missing one is not an error (P19.8).
func (s *Store) Delete(ctx context.Context, key string) error {
	full, err := s.full(key)
	if err != nil {
		return err
	}
	resp, err := s.do(ctx, http.MethodDelete, full, "", nil, nil, -1)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return s.classify("delete", key, resp)
	}
	resp.Body.Close()
	return nil
}

// Object is one listed object.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

// List answers the objects under a key prefix after the given key, in key order, a page at a time; next is "" on the
// last page (P19.8).
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]Object, string, error) {
	q := map[string]string{"list-type": "2", "max-keys": strconv.Itoa(limit), "prefix": s.Prefix + prefix}
	if after != "" {
		q["start-after"] = s.Prefix + after
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = s3sig.Escape(k, false) + "=" + s3sig.Escape(q[k], false)
	}
	resp, err := s.do(ctx, http.MethodGet, "", strings.Join(parts, "&"), nil, nil, -1)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode/100 != 2 {
		return nil, "", s.classify("list", prefix, resp)
	}
	defer resp.Body.Close()
	var out struct {
		Contents []struct {
			Key          string
			Size         int64
			LastModified time.Time
		}
		IsTruncated bool
	}
	if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, "", fmt.Errorf("%w: list %s: %v", ErrUnavailable, prefix, err)
	}
	objs := make([]Object, 0, len(out.Contents))
	for _, c := range out.Contents {
		objs = append(objs, Object{Key: strings.TrimPrefix(c.Key, s.Prefix), Size: c.Size, Modified: c.LastModified})
	}
	next := ""
	if out.IsTruncated && len(objs) > 0 {
		next = objs[len(objs)-1].Key
	}
	return objs, next, nil
}
