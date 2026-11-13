package storage

// Unit tests against a fake object store (httptest): the probes keep only a definite answer (P19.3, P19.4), the
// fallback copy, the error classes (P19.9) and the redaction of presigned addresses in the log (P19.10). The real
// store's behaviour is storage_integration_test.go's (RustFS).

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeStore keeps objects by path; fail answers the next requests of a method with a status; ignoreNoneMatch and
// copyIgnoresNoneMatch make it a store that does not honour the condition (on a put, on a copy); noCopy refuses
// server-side copies (501).
type fakeStore struct {
	mu                                    sync.Mutex
	objects                               map[string][]byte
	fail                                  map[string][]int
	ignoreNoneMatch, copyIgnoresNoneMatch bool
	noCopy                                bool
	copies, puts                          int
}

func newFake() *fakeStore { return &fakeStore{objects: map[string][]byte{}, fail: map[string][]int{}} }

func (f *fakeStore) failNext(method string, codes ...int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[method] = append(f.fail[method], codes...)
}

func (f *fakeStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if q := f.fail[r.Method]; len(q) > 0 {
		f.fail[r.Method] = q[1:]
		w.WriteHeader(q[0])
		_, _ = w.Write([]byte("<Error><Code>Boom</Code><Message>see https://store/x?X-Amz-Signature=deadbeef&a=1</Message></Error>"))
		return
	}
	key := r.URL.Path
	switch r.Method {
	case http.MethodPut:
		_, exists := f.objects[key]
		if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
			f.copies++
			if f.noCopy {
				w.WriteHeader(http.StatusNotImplemented)
				return
			}
			if exists && r.Header.Get("If-None-Match") == "*" && !f.copyIgnoresNoneMatch {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			data, ok := f.objects[src]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			f.objects[key] = append([]byte(nil), data...)
			_, _ = w.Write([]byte("<CopyObjectResult/>"))
			return
		}
		f.puts++
		if exists && r.Header.Get("If-None-Match") == "*" && !f.ignoreNoneMatch {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		data, _ := io.ReadAll(r.Body)
		f.objects[key] = data
	case http.MethodGet, http.MethodHead:
		data, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", itoa(len(data)))
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	case http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	}
}

func itoa(n int) string {
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			return string(b[i:])
		}
	}
}

func storeOver(t *testing.T, f *fakeStore) (*Store, *bytes.Buffer) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	var logs bytes.Buffer
	s, err := New(srv.URL, "us-east-1", "b", "owner/", "AK", func() string { return "SK" },
		slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return s, &logs
}

func put(t *testing.T, s *Store, key, body string, ifAbsent bool) error {
	t.Helper()
	_, err := s.Put(context.Background(), key, strings.NewReader(body), int64(len(body)), "text/plain", ifAbsent)
	return err
}

// A probe that meets an unreachable store or a 5xx decides nothing: the next write probes again and succeeds (P19.3).
func TestTheConditionalWriteProbeKeepsOnlyADefiniteAnswer(t *testing.T) {
	f := newFake()
	s, _ := storeOver(t, f)
	f.failNext(http.MethodPut, http.StatusServiceUnavailable)
	if err := put(t, s, "a", "x", true); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("the first probe write failed: want unavailable, got %v", err)
	}
	f.failNext(http.MethodPut, 200, http.StatusServiceUnavailable) // the second probe write fails
	if err := put(t, s, "a", "x", true); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("the second probe write failed: want unavailable, got %v", err)
	}
	if err := put(t, s, "a", "x", true); err != nil {
		t.Fatalf("the store is back: the probe answers and the write goes through, got %v", err)
	}
	if err := put(t, s, "a", "y", true); !errors.Is(err, ErrExists) {
		t.Fatalf("the condition holds: want exists, got %v", err)
	}
}

// A store that ignores If-None-Match on a put gets no conditional write at all (P19.3).
func TestAStoreThatIgnoresTheConditionGetsNoConditionalWrite(t *testing.T) {
	f := newFake()
	f.ignoreNoneMatch = true
	s, logs := storeOver(t, f)
	if err := put(t, s, "a", "x", true); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want unavailable, got %v", err)
	}
	if _, ok := f.objects["/b/owner/a"]; ok {
		t.Fatal("an unconditional write went through in the conditional one's place")
	}
	if !strings.Contains(logs.String(), "storage_no_conditional_write") {
		t.Fatal("the refusal is not logged")
	}
	if err := put(t, s, "b", "x", false); err != nil {
		t.Fatalf("an unconditional write is still fine: %v", err)
	}
}

// The copy probe that meets a 5xx decides nothing: the next copy probes again (P19.4).
func TestTheCopyProbeKeepsOnlyADefiniteAnswer(t *testing.T) {
	f := newFake()
	s, _ := storeOver(t, f)
	ctx := context.Background()
	if err := put(t, s, "src", "content", false); err != nil {
		t.Fatal(err)
	}
	f.failNext(http.MethodPut, http.StatusServiceUnavailable) // the probe object's write
	if err := s.Copy(ctx, "src", "dst"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want unavailable, got %v", err)
	}
	if err := s.Copy(ctx, "src", "dst"); err != nil {
		t.Fatalf("the store is back: the copy goes through, got %v", err)
	}
	if !s.native {
		t.Fatal("the store honours the condition on a copy: the copy is the store's")
	}
	if err := s.Copy(ctx, "src", "dst"); !errors.Is(err, ErrExists) {
		t.Fatalf("want exists, got %v", err)
	}
}

// A store whose copy ignores the condition, or refuses copies, gets the read and the conditional put; the destination
// holds what the source held, and an existing destination is refused (P19.4).
func TestTheFallbackCopy(t *testing.T) {
	for name, set := range map[string]func(*fakeStore){
		"copy ignores the condition": func(f *fakeStore) { f.copyIgnoresNoneMatch = true },
		"no server-side copy":        func(f *fakeStore) { f.noCopy = true },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake()
			set(f)
			s, _ := storeOver(t, f)
			ctx := context.Background()
			if err := put(t, s, "src", "content", false); err != nil {
				t.Fatal(err)
			}
			if err := s.Copy(ctx, "src", "dst"); err != nil {
				t.Fatal(err)
			}
			if s.native {
				t.Fatal("the copy should be the port's read and put")
			}
			copies := f.copies
			if got := string(f.objects["/b/owner/dst"]); got != "content" {
				t.Fatalf("the destination holds %q", got)
			}
			if err := s.Copy(ctx, "src", "dst"); !errors.Is(err, ErrExists) {
				t.Fatalf("want exists, got %v", err)
			}
			if f.copies != copies {
				t.Fatal("the fallback used the store's copy")
			}
		})
	}
}

// The answers become the error classes (P19.9), and the store's words reach the log without a presigned address's
// signature (P19.10).
func TestTheErrorClassesAndTheRedaction(t *testing.T) {
	f := newFake()
	s, logs := storeOver(t, f)
	ctx := context.Background()
	for code, want := range map[int]error{403: ErrDenied, 401: ErrDenied, 413: ErrTooLarge, 500: ErrUnavailable,
		503: ErrUnavailable} {
		f.failNext(http.MethodGet, code)
		if _, _, err := s.Get(ctx, "a"); !errors.Is(err, want) {
			t.Errorf("get %d: want %v, got %v", code, want, err)
		}
		f.failNext(http.MethodHead, code)
		if _, err := s.Head(ctx, "a"); !errors.Is(err, want) {
			t.Errorf("head %d: want %v, got %v", code, want, err)
		}
	}
	if _, err := s.Head(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("head of a missing object: %v", err)
	}
	if !strings.Contains(logs.String(), "X-Amz-Signature=…") {
		t.Fatal("the store's answers were not logged: the redaction is not tested")
	}
	if strings.Contains(logs.String(), "deadbeef") {
		t.Fatal("a signature reached the log")
	}
}
