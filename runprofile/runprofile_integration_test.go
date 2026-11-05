//go:build integration

package runprofile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/s3test"
	"github.com/brickKit/be-sdk-go/v3/sectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lockBucket is the object-locked test bucket (P12.1), created once; each run writes under its own deployment, and
// the records expire in minutes (the package's retention override), so the bucket does not keep them for years.
const lockBucket = "be-sdk-test-run-profile"

type env struct {
	url, access, secret string
	admin               *s3.Client
	w                   *sectest.World
	deployment          string
}

func setup(t *testing.T) env {
	e := env{url: os.Getenv("BE_SDK_TEST_S3_URL"), access: os.Getenv("BE_SDK_TEST_S3_ACCESS_KEY"),
		secret: os.Getenv("BE_SDK_TEST_S3_SECRET_KEY")}
	if e.url == "" || e.access == "" || e.secret == "" {
		t.Fatal("BE_SDK_TEST_S3_URL, BE_SDK_TEST_S3_ACCESS_KEY and BE_SDK_TEST_S3_SECRET_KEY must name object storage")
	}
	e.admin = s3test.Client(e.url, e.access, e.secret)
	_, err := e.admin.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(lockBucket),
		ObjectLockEnabledForBucket: aws.Bool(true)})
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if err != nil && !errors.As(err, &owned) && !errors.As(err, &exists) {
		t.Fatalf("creating the object-locked bucket: %v", err)
	}
	e.w = sectest.New(t)
	e.deployment = ids.New().String() // this run's own records
	return e
}

func (e env) store(t *testing.T, component string, edit ...func(*Options)) *Store {
	o := Options{Endpoint: e.url, Bucket: lockBucket, AccessKey: e.access, Secret: func() string { return e.secret },
		Deployment: e.deployment, Writer: Writer{Component: component, Instance: component + "-1"},
		Identity: e.w.TLS(t, component), retention: func(string) time.Duration { return 2 * time.Minute }}
	for _, f := range edit {
		f(&o)
	}
	s, err := New(o)
	require.NoError(t, err)
	return s
}

// A record is signed, written once under compliance-mode object lock with its kind's retention, and read back.
func TestARecordIsSignedLockedAndReadBack(t *testing.T) { // P12.1, P12.2, P12.3
	e := setup(t)
	s := e.store(t, "infra/authz")
	ctx := context.Background()
	r, err := s.Append(ctx, KindAuthzGeneration, map[string]int64{"value": 3})
	require.NoError(t, err)
	key := Key(e.deployment, KindAuthzGeneration, r.WrittenAt, r.ID)
	ret, err := e.admin.GetObjectRetention(ctx, &s3.GetObjectRetentionInput{Bucket: aws.String(lockBucket), Key: aws.String(key)})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockRetentionModeCompliance, ret.Retention.Mode)
	assert.WithinDuration(t, time.Now().Add(2*time.Minute), *ret.Retention.RetainUntilDate, 30*time.Second)
	recs, problems, err := s.List(ctx, KindAuthzGeneration)
	require.NoError(t, err)
	assert.Empty(t, problems)
	require.Len(t, recs, 1)
	assert.Equal(t, r.ID, recs[0].ID)
	assert.Equal(t, "infra/authz", recs[0].Writer.Component)
	assert.Equal(t, "ES256", recs[0].Signature.Alg)
	// the same key is never written twice: a writer that produced the same identity and instant is refused
	at := time.Now()
	same := e.store(t, "infra/authz", func(o *Options) {
		o.Now = func() time.Time { return at }
		o.newID = func() string { return "01a1ab2d-0000-7000-8000-00000000c0de" }
	})
	_, err = same.Append(ctx, KindAuthzGeneration, map[string]int64{"value": 1})
	require.NoError(t, err)
	_, err = same.Append(ctx, KindAuthzGeneration, map[string]int64{"value": 2})
	var se *StatusError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, http.StatusPreconditionFailed, se.Status)
	// no secret in the record
	raw, err := e.admin.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(lockBucket), Key: aws.String(key)})
	require.NoError(t, err)
	b, _ := io.ReadAll(raw.Body)
	assert.NotContains(t, string(b), e.secret) // P12.6
	assert.NotContains(t, string(b), "PRIVATE KEY")
}

// A record whose body was changed, or signed under another trust root, is not used and is reported (P12.4).
func TestATamperedOrForeignRecordIsNotUsed(t *testing.T) {
	e := setup(t)
	s := e.store(t, "infra/authz")
	ctx := context.Background()
	r, err := s.Append(ctx, KindAuthzGeneration, map[string]int64{"value": 4})
	require.NoError(t, err)
	tampered := r
	tampered.ID, tampered.Body = ids.New().String(), json.RawMessage(`{"value":99}`)
	raw, _ := json.Marshal(tampered)
	_, err = e.admin.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(lockBucket),
		Key: aws.String(Key(e.deployment, KindAuthzGeneration, tampered.WrittenAt, tampered.ID)), Body: bytes.NewReader(raw)})
	require.NoError(t, err)
	foreign := setup(t) // another deployment's trust root signs a record under this deployment's prefix
	fs := foreign.store(t, "infra/authz", func(o *Options) { o.Deployment = e.deployment })
	_, err = fs.Append(ctx, KindAuthzGeneration, map[string]int64{"value": 50})
	require.NoError(t, err)

	recs, problems, err := s.List(ctx, KindAuthzGeneration)
	require.NoError(t, err)
	require.Len(t, recs, 1, "only the genuine record")
	assert.Equal(t, r.ID, recs[0].ID)
	assert.Len(t, problems, 2)
	cur, _, err := s.Current(ctx, KindAuthzGeneration)
	require.NoError(t, err)
	assert.EqualValues(t, 4, cur, "the tampered 99 and the foreign 50 are not the generation")

	// ListAll: every record with its verification; an untrusted one comes with its content (6.2 reviews it)
	all, err := s.ListAll(ctx, KindAuthzGeneration)
	require.NoError(t, err)
	require.Len(t, all, 3)
	bodies := map[string]bool{}
	for _, l := range all {
		require.True(t, l.Read, l.Key)
		bodies[string(l.Record.Body)] = l.Err == nil
	}
	assert.Equal(t, map[string]bool{`{"value":4}`: true, `{"value":99}`: false, `{"value":50}`: false}, bodies)
}

// A generation advances to the largest value seen anywhere plus one, written to the run profile first; without the
// run profile it does not advance (P12.4, P12.5).
func TestAGenerationAdvancesOnlyThroughTheRunProfile(t *testing.T) {
	e := setup(t)
	s := e.store(t, "infra/authz")
	ctx := context.Background()
	cur, _, err := s.Current(ctx, KindDeploymentGeneration)
	require.NoError(t, err)
	assert.Zero(t, cur, "no record: 0")
	next, err := s.Advance(ctx, KindDeploymentGeneration, 4, 2) // the evidence store saw 4, a database 2
	require.NoError(t, err)
	assert.EqualValues(t, 5, next)
	next, err = s.Advance(ctx, KindDeploymentGeneration, 3) // the evidence store restored from an older backup
	require.NoError(t, err)
	assert.EqualValues(t, 6, next, "the run profile's 5 counts")
	cur, _, err = s.Current(ctx, KindDeploymentGeneration)
	require.NoError(t, err)
	assert.EqualValues(t, 6, cur)

	down := e.store(t, "infra/authz", func(o *Options) { o.Endpoint = closedPort(t); o.Client = &http.Client{Timeout: time.Second} })
	_, err = down.Advance(ctx, KindDeploymentGeneration, 6)
	assert.Error(t, err, "no run profile: no advance (AT62)")
}

// An alert goes to the run profile's alert kind and to the webhook; when the run profile copy cannot be written the
// double failure is logged and the caller goes on within the time limit (P12.7, P12.8).
func TestAnAlertIsWrittenAndADoubleFailureIsLogged(t *testing.T) {
	e := setup(t)
	var mu sync.Mutex
	var got []string
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, string(b))
		mu.Unlock()
	}))
	defer hook.Close()
	s := e.store(t, "infra/iam-casdoor", func(o *Options) { o.Webhook = hook.URL })
	ctx := context.Background()
	assert.True(t, s.Alert(ctx, map[string]string{"intent": "disable", "subject": "p1"}))
	recs, _, err := s.List(ctx, KindAlert)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.JSONEq(t, `{"intent":"disable","subject":"p1"}`, string(recs[0].Body))
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 1 }, 3*time.Second, 20*time.Millisecond)
	assert.Contains(t, got[0], `"subject":"p1"`)

	var logged []string
	down := e.store(t, "infra/iam-casdoor", func(o *Options) {
		o.Endpoint = closedPort(t)
		o.Log = func(msg string, args ...any) { logged = append(logged, msg+" "+fmt.Sprint(args...)) }
	})
	start := time.Now()
	assert.False(t, down.Alert(ctx, map[string]string{"intent": "disable"}))
	assert.Less(t, time.Since(start), AlertTimeout+time.Second, "the caller goes on within the time limit")
	require.Len(t, logged, 1)
	assert.Contains(t, logged[0], "evidence_double_failure")
}

// closedPort is an address nothing listens on.
func closedPort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return "http://" + addr
}
