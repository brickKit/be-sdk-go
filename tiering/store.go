package tiering

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/internal/s3sig"
)

// The cold route's object store (A153): one bucket, the component's prefix, and three credentials: the transfer
// writes, reads read, and destruction and the orphan cleanup delete. Objects are created, never overwritten
// (P15.15). The SDK signs its own requests (internal/s3sig, as the run profile and the large events).

// Store configuration keys.
const (
	KeyStoreURL = "TIERING_STORE_URL"
	KeyBucket   = "TIERING_BUCKET"
)

// credentialKeys are the access and secret keys of one role: WRITE, READ or DELETE.
func credentialKeys(role string) (string, string) {
	return "TIERING_" + role + "_ACCESS_KEY", "TIERING_" + role + "_SECRET_KEY"
}

// StoreKeys are the object store's keys; all optional (without TIERING_STORE_URL the cold route is unavailable).
func StoreKeys() []config.Key {
	keys := []config.Key{{Name: KeyStoreURL, Type: config.String}, {Name: KeyBucket, Type: config.String, Default: "be-tiering"}}
	for _, role := range []string{"WRITE", "READ", "DELETE"} {
		a, s := credentialKeys(role)
		keys = append(keys, config.Key{Name: a, Type: config.String}, config.Key{Name: s, Type: config.String, Secret: true})
	}
	return keys
}

// ErrMissing is an object that is not there.
var ErrMissing = errors.New("tiering: the object is missing")

// ErrExists is a create of a key that is already there.
var ErrExists = errors.New("tiering: the object exists")

type role struct {
	signer *s3sig.Signer
	secret func() string
}

// Store is one member's view of the cold bucket.
type Store struct {
	Bucket, Prefix     string
	write, read, erase role
	client             *http.Client
	now                func() time.Time
}

// StoreFrom builds the store from the configuration; nil when TIERING_STORE_URL is not set.
func StoreFrom(values *config.Values, prefix string) (*Store, error) {
	url := values.String(KeyStoreURL)
	if url == "" {
		return nil, nil
	}
	s := &Store{Bucket: values.String(KeyBucket), Prefix: prefix, client: &http.Client{Timeout: time.Minute}, now: time.Now}
	for _, r := range []struct {
		name string
		to   *role
	}{{"WRITE", &s.write}, {"READ", &s.read}, {"DELETE", &s.erase}} {
		a, sk := credentialKeys(r.name)
		if values.String(a) == "" || values.Secret(sk) == nil {
			return nil, fmt.Errorf("tiering: %s and %s are required with %s", a, sk, KeyStoreURL)
		}
		signer, err := s3sig.New(url, "us-east-1", values.String(a))
		if err != nil {
			return nil, err
		}
		secret := values.Secret(sk)
		*r.to = role{signer: signer, secret: secret.Value}
	}
	return s, nil
}

func (s *Store) do(ctx context.Context, r role, method, key, query string, extra map[string]string, body []byte) (*http.Response, []byte, error) {
	payload := "UNSIGNED-PAYLOAD"
	if body != nil {
		sum := sha256.Sum256(body)
		payload = hex.EncodeToString(sum[:])
	}
	u, headers := r.signer.Request(r.secret(), s.now(), method, s.Bucket, key, query, extra, payload)
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	return resp, out, err
}

func statusErr(op, key string, resp *http.Response, body []byte) error {
	return fmt.Errorf("tiering: %s %s: the object store answered %d: %.200s", op, key, resp.StatusCode, body)
}

// Put creates an object (the writer's credential); ErrExists when the key is taken (P15.15).
func (s *Store) Put(ctx context.Context, key string, body []byte) error {
	md := md5.Sum(body)
	resp, out, err := s.do(ctx, s.write, http.MethodPut, key, "", map[string]string{"if-none-match": "*",
		"content-md5": base64.StdEncoding.EncodeToString(md[:]), "content-type": "application/octet-stream"}, body)
	switch {
	case err != nil:
		return err
	case resp.StatusCode == http.StatusPreconditionFailed:
		return fmt.Errorf("%w: %s", ErrExists, key)
	case resp.StatusCode/100 != 2:
		return statusErr("put", key, resp, out)
	}
	return nil
}

// Get reads an object (the reader's credential); ErrMissing when it is not there.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	resp, out, err := s.do(ctx, s.read, http.MethodGet, key, "", nil, nil)
	switch {
	case err != nil:
		return nil, err
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("%w: %s", ErrMissing, key)
	case resp.StatusCode/100 != 2:
		return nil, statusErr("get", key, resp, out)
	}
	return out, nil
}

// PresignGet is an address that reads one object for expires without credentials (the reader's credential signs it).
func (s *Store) PresignGet(key string, expires time.Duration) string {
	return s.read.signer.PresignGet(s.read.secret(), s.Bucket, key, expires, s.now())
}

// Delete removes an object (the deleter's credential); a missing object is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	resp, out, err := s.do(ctx, s.erase, http.MethodDelete, key, "", nil, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
		return statusErr("delete", key, resp, out)
	}
	return nil
}

// Object is one listed object.
type Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// List lists the objects under a prefix (the reader's credential).
func (s *Store) List(ctx context.Context, prefix string) ([]Object, error) {
	var out []Object
	token := ""
	for {
		q := "list-type=2&prefix=" + s3sig.Escape(prefix, false)
		if token != "" {
			q = "continuation-token=" + s3sig.Escape(token, false) + "&" + q
		}
		resp, body, err := s.do(ctx, s.read, http.MethodGet, "", q, nil, nil)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode/100 != 2 {
			return nil, statusErr("list", prefix, resp, body)
		}
		var page struct {
			Contents []struct {
				Key          string
				Size         int64
				LastModified string
			} `xml:"Contents"`
			Next string `xml:"NextContinuationToken"`
			More bool   `xml:"IsTruncated"`
		}
		if err := xml.Unmarshal(body, &page); err != nil {
			return nil, err
		}
		for _, c := range page.Contents {
			at, _ := time.Parse(time.RFC3339Nano, c.LastModified)
			out = append(out, Object{Key: c.Key, Size: c.Size, LastModified: at})
		}
		if !page.More {
			return out, nil
		}
		token = page.Next
	}
}

// Locked says whether the bucket has object lock enabled: its objects cannot be erased in place (P15.32).
func (s *Store) Locked(ctx context.Context) (bool, error) {
	resp, body, err := s.do(ctx, s.read, http.MethodGet, "", "object-lock=", nil, nil)
	if err != nil {
		return false, err
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return false, nil
	case resp.StatusCode/100 != 2:
		if strings.Contains(string(body), "ObjectLockConfigurationNotFound") {
			return false, nil
		}
		return false, statusErr("object lock of", s.Bucket, resp, body)
	}
	return strings.Contains(string(body), "<ObjectLockEnabled>Enabled</ObjectLockEnabled>"), nil
}
