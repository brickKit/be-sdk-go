package events

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
)

// ClaimTTL is how long a claim's presigned read stays valid: the transport retention, also SigV4's maximum (P6.14).
const ClaimTTL = TransportRetention

// StoreKeys are the member-level keys of the SDK's storage port (chapter 02); without OBJECT_STORE_URL there is no
// object storage and payloads over 64 KiB cannot be emitted.
func StoreKeys() []config.Key {
	return []config.Key{
		{Name: "OBJECT_STORE_URL", Type: config.String},
		{Name: "OBJECT_STORE_REGION", Type: config.String, Default: "us-east-1"},
		{Name: "OBJECT_STORE_BUCKET", Type: config.String},
		{Name: "OBJECT_STORE_PREFIX", Type: config.String},
		{Name: "OBJECT_STORE_ACCESS_KEY", Type: config.String},
		{Name: "OBJECT_STORE_SECRET_KEY", Type: config.String, Secret: true},
	}
}

// S3Store is the producer side of the claim check: the payload goes to the owner's bucket and prefix, and the
// reference is a presigned read-only GET (P6.14). Both requests are signed with the SDK's own SigV4 (sigv4.go) over the
// member's own transport: through the AWS SDK an upload with presigning cost about 1.2 ms of CPU (2.2 ms with 8 at
// once), against 0.7 ms (1.2 ms). The secret is read at every upload, so a rotated file applies from the next one.
type S3Store struct {
	signer *signer
	client *http.Client
	bucket string
	prefix string
	secret *config.Secret
}

// NewS3Store builds the store from a member's values; nil without OBJECT_STORE_URL.
func NewS3Store(v *config.Values) (*S3Store, error) {
	url := v.String("OBJECT_STORE_URL")
	if url == "" {
		return nil, nil
	}
	secret := v.Secret("OBJECT_STORE_SECRET_KEY")
	if v.String("OBJECT_STORE_BUCKET") == "" || v.String("OBJECT_STORE_ACCESS_KEY") == "" || secret == nil {
		return nil, fmt.Errorf("object storage: OBJECT_STORE_BUCKET, OBJECT_STORE_ACCESS_KEY and OBJECT_STORE_SECRET_KEY go with OBJECT_STORE_URL")
	}
	s, err := newSigner(url, v.String("OBJECT_STORE_REGION"), v.String("OBJECT_STORE_ACCESS_KEY"))
	if err != nil {
		return nil, err
	}
	return &S3Store{signer: s, client: memberHTTPClient(), bucket: v.String("OBJECT_STORE_BUCKET"),
		prefix: v.String("OBJECT_STORE_PREFIX"), secret: secret}, nil
}

// Put implements ClaimStore.
func (s *S3Store) Put(ctx context.Context, eventID string, data []byte) (string, error) {
	key := s.prefix + "events/" + eventID
	secret := s.secret.Value()
	u, headers := s.signer.putHeaders(secret, s.bucket, key, sha256Hex(data), "application/json", len(data), time.Now())
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("object storage: putting %s: %w", key, err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("object storage: putting %s: %w", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// the answer says why (a wrong secret, a missing bucket); it never carries the secret
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return "", fmt.Errorf("object storage: putting %s: answered %d: %s", key, resp.StatusCode, b)
	}
	_, _ = io.Copy(io.Discard, resp.Body) // the connection goes back to the pool
	return s.signer.presignGet(secret, s.bucket, key, ClaimTTL, time.Now()), nil
}

// memberHTTPClient is one member's own client for object storage (never the process-wide default transport).
func memberHTTPClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{Proxy: nil, MaxIdleConnsPerHost: ClaimConcurrency * 2,
		IdleConnTimeout: 90 * time.Second}} // calls.ClientIdleTimeout: below the servers' (P1.20)
}

// HTTPClaims is the subscriber side: a plain GET of the presigned address, no storage credentials (P6.14).
type HTTPClaims struct{ Client *http.Client }

// NewHTTPClaims returns the member's own reader (never the process-wide default client).
func NewHTTPClaims() HTTPClaims { return HTTPClaims{Client: memberHTTPClient()} }

// Get implements ClaimReader.
func (h HTTPClaims) Get(ctx context.Context, uri string) ([]byte, error) {
	if !strings.HasPrefix(uri, "http://") && !strings.HasPrefix(uri, "https://") {
		return nil, fmt.Errorf("a claim reference is not an http(s) address")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the claimed object answered %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 64<<20))
}
