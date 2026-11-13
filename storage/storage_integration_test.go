//go:build integration

package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brickKit/be-sdk-go/v3/internal/s3test"
	"github.com/brickKit/be-sdk-go/v3/storage"
)

func store(t *testing.T) (*storage.Store, *bytes.Buffer) {
	t.Helper()
	env := s3test.Env(t, fmt.Sprintf("storage-it-%d/", time.Now().UnixNano()))
	var logs bytes.Buffer
	s, err := storage.New(env["OBJECT_STORE_URL"], "us-east-1", env["OBJECT_STORE_BUCKET"], env["OBJECT_STORE_PREFIX"],
		env["OBJECT_STORE_ACCESS_KEY"], func() string { return os.Getenv("BE_SDK_TEST_S3_SECRET_KEY") },
		slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)
	return s, &logs
}

func read(t *testing.T, s *storage.Store, key string) string {
	t.Helper()
	r, _, err := s.Get(context.Background(), key)
	require.NoError(t, err)
	defer r.Close()
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(b)
}

// P19.1-P19.3, P19.5, P19.8: put only when absent, the metadata with the port's digest, a missing object, delete and
// list.
func TestPutHeadGetDeleteList(t *testing.T) {
	s, _ := store(t)
	ctx := context.Background()
	digest, err := s.Put(ctx, "a/one", strings.NewReader("hello"), 5, "text/plain", true)
	require.NoError(t, err)
	assert.Equal(t, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", digest)
	_, err = s.Put(ctx, "a/one", strings.NewReader("other"), 5, "text/plain", true)
	assert.ErrorIs(t, err, storage.ErrExists, "only when absent")
	assert.Equal(t, "hello", read(t, s, "a/one"), "unchanged")
	m, err := s.Head(ctx, "a/one")
	require.NoError(t, err)
	assert.Equal(t, int64(5), m.Size)
	assert.Equal(t, "text/plain", m.ContentType)
	assert.Equal(t, digest, m.SHA256)
	_, _, err = s.Get(ctx, "a/none")
	assert.ErrorIs(t, err, storage.ErrNotFound)
	_, err = s.Head(ctx, "a/none")
	assert.ErrorIs(t, err, storage.ErrNotFound)
	for i := range 3 {
		_, err := s.Put(ctx, fmt.Sprintf("list/%d", i), strings.NewReader("x"), 1, "text/plain", false)
		require.NoError(t, err)
	}
	page, next, err := s.List(ctx, "list/", "", 2)
	require.NoError(t, err)
	require.Len(t, page, 2)
	assert.Equal(t, "list/0", page[0].Key, "keys relative to the prefix")
	rest, last, err := s.List(ctx, "list/", next, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"list/2"}, []string{rest[0].Key})
	assert.Empty(t, last)
	require.NoError(t, s.Delete(ctx, "a/one"))
	require.NoError(t, s.Delete(ctx, "a/one"), "a missing object is no error")
	for _, bad := range []string{"/abs", "a//b", "a/../b", "Upper", "a/ b"} {
		_, err := s.Put(ctx, bad, strings.NewReader("x"), 1, "text/plain", false)
		assert.Error(t, err, bad)
	}
}

// P19.4: the copy writes what the source held, only when the destination is absent; the mode it found is logged.
func TestCopyOnlyWhenAbsent(t *testing.T) {
	s, logs := store(t)
	ctx := context.Background()
	_, err := s.Put(ctx, "staging/x", strings.NewReader("first"), 5, "application/pdf", false)
	require.NoError(t, err)
	require.NoError(t, s.Copy(ctx, "staging/x", "objects/x/1"))
	_, err = s.Put(ctx, "staging/x", strings.NewReader("later"), 5, "application/pdf", false) // the client overwrites
	require.NoError(t, err)
	assert.ErrorIs(t, s.Copy(ctx, "staging/x", "objects/x/1"), storage.ErrExists)
	assert.Equal(t, "first", read(t, s, "objects/x/1"), "the final object is what was copied")
	m, err := s.Head(ctx, "objects/x/1")
	require.NoError(t, err)
	assert.Empty(t, m.SHA256, "the source's metadata is not carried over")
	assert.Equal(t, "application/octet-stream", m.ContentType)
	assert.ErrorIs(t, s.Copy(ctx, "staging/none", "objects/none/1"), storage.ErrNotFound)
	assert.Contains(t, logs.String(), "storage_copy_mode")
	t.Logf("copy mode: %s", logs.String()[strings.Index(logs.String(), "mode="):])
}

// P19.6, P19.7: the presigned addresses are signed for the browsers' address of the store when one is given.
func TestPresignedForThePublicAddress(t *testing.T) {
	s, _ := store(t)
	require.NoError(t, s.UsePublic("https://files.example.com"))
	u, err := s.PresignPut("staging/p", 1, "image/png", time.Minute)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(u, "https://files.example.com/"), u)
	g, err := s.PresignGet("objects/p/1", time.Minute, "p.png", "")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(g, "https://files.example.com/"), g)
}

// P19.6: the presigned upload takes exactly the signed length and type; anything else is refused by the store.
func TestPresignedUpload(t *testing.T) {
	s, _ := store(t)
	u, err := s.PresignPut("staging/up", 5, "application/pdf", time.Minute)
	require.NoError(t, err)
	put := func(body, typ string) int {
		req, _ := http.NewRequest(http.MethodPut, u, strings.NewReader(body))
		req.Header.Set("Content-Type", typ)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}
	assert.NotEqual(t, 200, put("toolong", "application/pdf"), "another length")
	assert.NotEqual(t, 200, put("hello", "text/html"), "another type")
	assert.Equal(t, 200, put("hello", "application/pdf"))
	assert.Equal(t, "hello", read(t, s, "staging/up"))
	_, err = s.PresignPut("staging/up", 5, "application/pdf", 2*time.Hour)
	assert.Error(t, err, "an hour at most")
}

// P19.7: the presigned download reads the object saved as a named file, and stops working when it expires.
func TestPresignedDownload(t *testing.T) {
	s, _ := store(t)
	_, err := s.Put(context.Background(), "objects/d/1", strings.NewReader("<svg/>"), 6, "image/svg+xml", false)
	require.NoError(t, err)
	u, err := s.PresignGet("objects/d/1", 2*time.Second, "合同 1.svg", "")
	require.NoError(t, err)
	resp, err := http.Get(u)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, string(body))
	assert.Equal(t, "<svg/>", string(body))
	assert.Contains(t, resp.Header.Get("Content-Disposition"), "attachment", "saved, never shown")
	assert.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"), "an uploaded SVG does not run")
	time.Sleep(3 * time.Second)
	resp, err = http.Get(u)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, 403, resp.StatusCode, "expired")
}

// P19.9, P19.10: a wrong credential is denied, not unavailable; the answer logged has no signature.
func TestErrorClasses(t *testing.T) {
	env := s3test.Env(t, "storage-it-denied/")
	var logs bytes.Buffer
	s, err := storage.New(env["OBJECT_STORE_URL"], "us-east-1", env["OBJECT_STORE_BUCKET"], env["OBJECT_STORE_PREFIX"],
		env["OBJECT_STORE_ACCESS_KEY"], func() string { return "not-the-secret" }, slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)
	_, err = s.Put(context.Background(), "x", strings.NewReader("x"), 1, "text/plain", false)
	assert.True(t, errors.Is(err, storage.ErrDenied), "%v", err)
	_, err = s.Head(context.Background(), "x")
	assert.True(t, errors.Is(err, storage.ErrDenied), "a refused head is denied, not unavailable (P19.9): %v", err)
	// the redaction of the store's words is storage_test.go's (a fake store that answers with a signature); here the
	// refusal reached the log
	assert.Contains(t, logs.String(), "storage_refused")
}
