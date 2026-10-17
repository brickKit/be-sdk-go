package events

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
// reference is a presigned read-only GET (P6.14). Credentials are read at every request, so a rotated file applies.
type S3Store struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
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
	access := v.String("OBJECT_STORE_ACCESS_KEY")
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return credentials.NewStaticCredentialsProvider(access, secret.Value(), "").Retrieve(context.Background())
	})
	client := s3.New(s3.Options{Region: v.String("OBJECT_STORE_REGION"), BaseEndpoint: aws.String(url),
		Credentials: creds, UsePathStyle: true, HTTPClient: &http.Client{Timeout: 30 * time.Second}})
	return &S3Store{client: client, presign: s3.NewPresignClient(client), bucket: v.String("OBJECT_STORE_BUCKET"),
		prefix: v.String("OBJECT_STORE_PREFIX")}, nil
}

// Put implements ClaimStore.
func (s *S3Store) Put(ctx context.Context, eventID string, data []byte) (string, error) {
	key := s.prefix + "events/" + eventID
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key),
		Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
		ContentType: aws.String("application/json")}); err != nil {
		return "", fmt.Errorf("object storage: putting %s: %w", key, err)
	}
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)},
		s3.WithPresignExpires(ClaimTTL))
	if err != nil {
		return "", fmt.Errorf("object storage: presigning %s: %w", key, err)
	}
	return req.URL, nil
}

// HTTPClaims is the subscriber side: a plain GET of the presigned address, no storage credentials (P6.14).
type HTTPClaims struct{ Client *http.Client }

// NewHTTPClaims returns the member's own reader (never the process-wide default client).
func NewHTTPClaims() HTTPClaims { return HTTPClaims{Client: &http.Client{Timeout: 30 * time.Second}} }

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
