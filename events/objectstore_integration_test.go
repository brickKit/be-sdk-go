//go:build integration

package events

import (
	"bytes"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/s3test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3ClaimsRoundTripWithoutStorageCredentials(t *testing.T) { // P6.14, CK-EV-06
	env := s3test.Env(t, "demo-claims/")
	values, problems := config.Parse(StoreKeys(), config.Map(env))
	require.Empty(t, problems)
	store, err := NewS3Store(values)
	require.NoError(t, err)
	id := ids.New().String()
	payload := []byte(`{"blob":"` + strings.Repeat("y", MaxPayload) + `"}`)
	uri, err := store.Put(context.Background(), id, payload)
	require.NoError(t, err)
	assert.Contains(t, uri, "/"+s3test.Bucket+"/demo-claims/events/"+id+"?", "under the owner's prefix")
	assert.Contains(t, uri, "X-Amz-Expires=604800", "valid for the transport retention")

	m := Message{Headers: map[string]string{HeaderID: id, HeaderClaim: "true"},
		Data: []byte(`{"claim":{"uri":"` + uri + `","size":` + strconv.Itoa(len(payload)) + `,"sha256":"` + sha256Hex(payload) + `"}}`)}
	got, err := ResolveClaim(context.Background(), m, NewHTTPClaims())
	require.NoError(t, err, "a plain GET of the presigned address, no credentials")
	assert.Equal(t, payload, got)

	admin := s3test.Client(env["OBJECT_STORE_URL"], env["OBJECT_STORE_ACCESS_KEY"], os.Getenv("BE_SDK_TEST_S3_SECRET_KEY"))
	_, err = admin.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(s3test.Bucket),
		Key: aws.String("demo-claims/events/" + id), Body: bytes.NewReader([]byte("tampered"))})
	require.NoError(t, err)
	_, err = ResolveClaim(context.Background(), m, NewHTTPClaims())
	assert.ErrorContains(t, err, "does not match its size and digest")
}

func TestARotatedSecretIsUsedFromTheNextUpload(t *testing.T) { // P6.14, P2.8
	env := s3test.Env(t, "demo-rotate/")
	secretFile := t.TempDir() + "/secret"
	right := env["OBJECT_STORE_SECRET_KEY"]
	require.NoError(t, os.WriteFile(secretFile, []byte(right), 0o600))
	delete(env, "OBJECT_STORE_SECRET_KEY")
	env["OBJECT_STORE_SECRET_KEY_FILE"] = secretFile
	src := config.Env{LookupEnv: func(k string) (string, bool) { v, ok := env[k]; return v, ok }, Read: os.ReadFile}
	values, problems := config.Parse(StoreKeys(), src)
	require.Empty(t, problems)
	store, err := NewS3Store(values)
	require.NoError(t, err)
	secret := values.Secret("OBJECT_STORE_SECRET_KEY")
	put := func() (string, error) { return store.Put(context.Background(), ids.New().String(), []byte(`{"n":1}`)) }

	uri, err := put()
	require.NoError(t, err)
	got, err := NewHTTPClaims().Get(context.Background(), uri)
	require.NoError(t, err)
	assert.Equal(t, `{"n":1}`, string(got), "the presigned address reads the object back")

	require.NoError(t, os.WriteFile(secretFile, []byte(right+"-rotated-away"), 0o600))
	_, err = secret.Refresh(os.ReadFile)
	require.NoError(t, err)
	_, err = put()
	assert.ErrorContains(t, err, "answered 403", "the next upload signs with the file's new secret")

	require.NoError(t, os.WriteFile(secretFile, []byte(right), 0o600))
	_, err = secret.Refresh(os.ReadFile)
	require.NoError(t, err)
	_, err = put()
	assert.NoError(t, err)
}
