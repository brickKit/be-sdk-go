package s3sig

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The claim check's SigV4 against the AWS SDK's signer at a fixed time (P6.14): a presigned GET and a signed PUT,
// for an ordinary event key and for one that needs every kind of encoding.

var (
	sigWhen  = time.Date(2026, 10, 18, 14, 30, 5, 0, time.UTC)
	sigCreds = aws.Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}
	sigKeys  = []string{"test_probe_go/events/0190f3a1-0000-7000-8000-0000000000a1", "a b/ü+x=y&z~%"}
)

func awsSigner() *v4.Signer {
	return v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
}

func TestAPresignedGetEqualsTheAWSSigners(t *testing.T) {
	s, err := New("http://127.0.0.1:9000", "us-east-1", sigCreds.AccessKeyID)
	require.NoError(t, err)
	for _, key := range sigKeys {
		mine, err := url.Parse(s.PresignGet(sigCreds.SecretAccessKey, "be-claims", key, 7*24*time.Hour, sigWhen))
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:9000"+s.path("be-claims", key)+"?X-Amz-Expires=604800", nil)
		require.NoError(t, err)
		theirs, _, err := awsSigner().PresignHTTP(context.Background(), sigCreds, req, unsignedBody, "s3", "us-east-1", sigWhen)
		require.NoError(t, err)
		want, err := url.Parse(theirs)
		require.NoError(t, err)
		assert.Equal(t, want.EscapedPath(), mine.EscapedPath(), key)
		assert.Equal(t, want.Query(), mine.Query(), key)
	}
}

func TestASignedPutEqualsTheAWSSigners(t *testing.T) {
	s, err := New("https://store.example:9443/base", "eu-west-1", sigCreds.AccessKeyID)
	require.NoError(t, err)
	const digest = "1b4f0e9851971998e732078544c96b36c3d01cedf7caa332359d6f1d83567014" // sha256("test")
	for _, key := range sigKeys {
		u, headers := s.PutHeaders(sigCreds.SecretAccessKey, "be-claims", key, digest, "application/json", 4, sigWhen)
		require.True(t, strings.HasPrefix(u, "https://store.example:9443/base/be-claims/"))
		req, err := http.NewRequest(http.MethodPut, u, strings.NewReader("test"))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Amz-Content-Sha256", digest)
		require.NoError(t, awsSigner().SignHTTP(context.Background(), sigCreds, req, digest, "s3", "eu-west-1", sigWhen))
		assert.Equal(t, req.Header.Get("Authorization"), headers["Authorization"], key)
		assert.Equal(t, req.Header.Get("X-Amz-Date"), headers["X-Amz-Date"], key)
	}
}

func TestTheSigningKeyFollowsARotatedSecret(t *testing.T) {
	s, err := New("http://127.0.0.1:9000", "us-east-1", sigCreds.AccessKeyID)
	require.NoError(t, err)
	a := s.PresignGet("secret-one", "b", "k", time.Minute, sigWhen)
	b := s.PresignGet("secret-two", "b", "k", time.Minute, sigWhen)
	assert.NotEqual(t, a, b)
	assert.Equal(t, a, s.PresignGet("secret-one", "b", "k", time.Minute, sigWhen))
}

// The run profile's requests (chapter 12): a conditional, object-locked PUT with every header signed, and a listing
// GET with a query, both against the AWS SDK's signer.
func TestASignedRequestEqualsTheAWSSigners(t *testing.T) {
	s, err := New("https://store.example:9443/base", "eu-west-1", sigCreds.AccessKeyID)
	require.NoError(t, err)
	const digest = "1b4f0e9851971998e732078544c96b36c3d01cedf7caa332359d6f1d83567014" // sha256("test")
	extra := map[string]string{"content-type": "application/json", "content-length": "4", "content-md5": "CY9rzUYh03PK3k6DJie09g==",
		"if-none-match": "*", "x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": "2036-10-18T14:30:05Z"}
	u, headers := s.Request(sigCreds.SecretAccessKey, sigWhen, http.MethodPut, "be-run-profile",
		"d1/alert/2026/10/18/2026-10-18T14:30:05.000000Z-x.json", "", extra, digest)
	req, err := http.NewRequest(http.MethodPut, u, strings.NewReader("test"))
	require.NoError(t, err)
	for n, v := range headers {
		if n != "authorization" && n != "content-length" {
			req.Header.Set(n, v)
		}
	}
	require.NoError(t, awsSigner().SignHTTP(context.Background(), sigCreds, req, digest, "s3", "eu-west-1", sigWhen))
	assert.Equal(t, req.Header.Get("Authorization"), headers["authorization"])

	q := "list-type=2&prefix=" + Escape("d1/generation.authz/", false)
	u, headers = s.Request(sigCreds.SecretAccessKey, sigWhen, http.MethodGet, "be-run-profile", "", q, nil, unsignedBody)
	assert.Equal(t, "https://store.example:9443/base/be-run-profile?"+q, u)
	req, err = http.NewRequest(http.MethodGet, u, nil)
	require.NoError(t, err)
	req.Header.Set("X-Amz-Content-Sha256", unsignedBody)
	require.NoError(t, awsSigner().SignHTTP(context.Background(), sigCreds, req, unsignedBody, "s3", "eu-west-1", sigWhen))
	assert.Equal(t, req.Header.Get("Authorization"), headers["authorization"])
}
