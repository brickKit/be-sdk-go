//go:build integration

package events

// The claim check's upload (PUT 100 KiB, then presign a GET) through the AWS SDK (the reference) against the SDK's
// S3Store (its own SigV4 over the member's net/http transport), against real object storage: process CPU per upload
// (cpu-ms/op) and wall time, one at a time and 8 at once.
// Run: go test -tags integration -run '^$' -bench ClaimUpload -benchtime 500x ./events/

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/s3test"
	"github.com/stretchr/testify/require"
)

type benchStore interface {
	Put(ctx context.Context, eventID string, data []byte) (string, error)
}

// awsStore is the reference: the claim check's upload as it was written with the AWS SDK.
type awsStore struct {
	client  *s3.Client
	presign *s3.PresignClient
	bucket  string
	prefix  string
}

func newAWSStore(env map[string]string) awsStore {
	creds := credentials.NewStaticCredentialsProvider(env["OBJECT_STORE_ACCESS_KEY"], env["OBJECT_STORE_SECRET_KEY"], "")
	c := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(env["OBJECT_STORE_URL"]), Credentials: creds,
		UsePathStyle: true, HTTPClient: memberHTTPClient()})
	return awsStore{client: c, presign: s3.NewPresignClient(c), bucket: env["OBJECT_STORE_BUCKET"], prefix: "bench-claims/"}
}

func (a awsStore) Put(ctx context.Context, eventID string, data []byte) (string, error) {
	key := a.prefix + "events/" + eventID
	if _, err := a.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(key),
		Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))), ContentType: aws.String("application/json")}); err != nil {
		return "", err
	}
	req, err := a.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(key)},
		s3.WithPresignExpires(ClaimTTL))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

func cpuNow() time.Duration {
	var r syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return time.Duration(r.Utime.Nano() + r.Stime.Nano())
}

func BenchmarkClaimUpload(b *testing.B) {
	env := s3test.Env(b, "bench-claims/")
	values, problems := config.Parse(StoreKeys(), config.Map(env))
	require.Empty(b, problems)
	own, err := NewS3Store(values)
	require.NoError(b, err)
	body := []byte(`{"blob":"` + strings.Repeat("x", 100<<10) + `"}`)
	for _, c := range []struct {
		name  string
		store benchStore
	}{{"aws-sdk-go-v2", newAWSStore(env)}, {"sigv4-net-http", own}} {
		for _, conc := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/concurrency-%d", c.name, conc), func(b *testing.B) {
				for range 10 { // warm the connections and the signing key
					_, err := c.store.Put(context.Background(), ids.New().String(), body)
					require.NoError(b, err)
				}
				work := make(chan struct{}, b.N)
				for range b.N {
					work <- struct{}{}
				}
				close(work)
				b.ResetTimer()
				c0 := cpuNow()
				var wg sync.WaitGroup
				for range conc {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range work {
							if _, err := c.store.Put(context.Background(), ids.New().String(), body); err != nil {
								b.Error(err)
								return
							}
						}
					}()
				}
				wg.Wait()
				b.ReportMetric(float64(cpuNow()-c0)/float64(time.Millisecond)/float64(b.N), "cpu-ms/op")
			})
		}
	}
}
