// Package s3test gives integration tests an object storage bucket: BE_SDK_TEST_S3_URL, BE_SDK_TEST_S3_ACCESS_KEY and
// BE_SDK_TEST_S3_SECRET_KEY name an S3 interface (the assembly project's be-rustfs). Without them the tests fail.
package s3test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Bucket is the test bucket; it is created on first use and never dropped.
const Bucket = "be-sdk-test"

// Env returns the OBJECT_STORE_* values of a member with its own prefix in the test bucket, creating the bucket.
func Env(t testing.TB, prefix string) map[string]string {
	t.Helper()
	url, access, secret := os.Getenv("BE_SDK_TEST_S3_URL"), os.Getenv("BE_SDK_TEST_S3_ACCESS_KEY"), os.Getenv("BE_SDK_TEST_S3_SECRET_KEY")
	if url == "" || access == "" || secret == "" {
		t.Fatal("BE_SDK_TEST_S3_URL, BE_SDK_TEST_S3_ACCESS_KEY and BE_SDK_TEST_S3_SECRET_KEY must name object storage")
	}
	c := Client(url, access, secret)
	_, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(Bucket)})
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if err != nil && !errors.As(err, &owned) && !errors.As(err, &exists) {
		t.Fatalf("creating the test bucket: %v", err)
	}
	return map[string]string{"OBJECT_STORE_URL": url, "OBJECT_STORE_BUCKET": Bucket, "OBJECT_STORE_PREFIX": prefix,
		"OBJECT_STORE_ACCESS_KEY": access, "OBJECT_STORE_SECRET_KEY": secret}
}

// Client is an admin client for checks and tampering.
func Client(url, access, secret string) *s3.Client {
	return s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(url), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(access, secret, "")})
}
