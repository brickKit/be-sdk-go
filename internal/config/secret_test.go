package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recorder collects OnChange / OnError calls.
type recorder struct {
	mu      sync.Mutex
	changes []string
	errs    []error
}

func (r *recorder) options() SecretOptions {
	return SecretOptions{
		OnChange: func(key string) { r.mu.Lock(); r.changes = append(r.changes, key); r.mu.Unlock() },
		OnError:  func(key string, err error) { r.mu.Lock(); r.errs = append(r.errs, err); r.mu.Unlock() },
	}
}

func (r *recorder) counts() (changes, errs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.changes), len(r.errs)
}

// writeSecret writes content and moves the mtime forward by step so a change is always visible.
func writeSecret(t *testing.T, path, content string, step int) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	mt := time.Now().Add(time.Duration(step) * time.Second)
	if err := os.Chtimes(path, mt, mt); err != nil {
		t.Fatal(err)
	}
}

func newSecretFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "APP_TOKEN_SIGNING_KEY_FILE")
	writeSecret(t, p, content, 0)
	return p
}

func TestOpenSecretReadsAtOnce(t *testing.T) {
	p := newSecretFile(t, "s3cr3t\r\n")
	s, err := OpenSecret("APP_TOKEN_SIGNING_KEY_FILE", p, SecretOptions{Required: true})
	if err != nil {
		t.Fatal(err)
	}
	if s.Current() != "s3cr3t" || string(s.Bytes()) != "s3cr3t\r\n" || s.Key() != "APP_TOKEN_SIGNING_KEY_FILE" {
		t.Fatalf("Current %q Bytes %q", s.Current(), s.Bytes())
	}
	b := s.Bytes()
	b[0] = 'X'
	if s.Current() != "s3cr3t" || s.Bytes()[0] != 's' {
		t.Fatal("Bytes must return a copy")
	}
}

func TestOpenSecretErrors(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	writeSecret(t, empty, "\n", 0)
	cases := map[string]struct {
		path string
		o    SecretOptions
		want string
	}{
		"relative":        {"secrets/x", SecretOptions{}, ReasonInvalid},
		"missing":         {filepath.Join(dir, "nope"), SecretOptions{}, ReasonInvalid},
		"directory":       {dir, SecretOptions{}, ReasonInvalid},
		"empty required":  {empty, SecretOptions{Required: true}, ReasonMissing},
		"interval > 30 s": {empty, SecretOptions{Interval: 31 * time.Second}, ReasonInvalid},
	}
	for name, c := range cases {
		_, err := OpenSecret("K_FILE", c.path, c.o)
		if err == nil {
			t.Errorf("%s: no error", name)
			continue
		}
		if got := reasonOf(t, err); got != c.want {
			t.Errorf("%s: want %s got %s", name, c.want, got)
		}
	}
	if s, err := OpenSecret("K_FILE", empty, SecretOptions{}); err != nil || s.Current() != "" {
		t.Errorf("an optional secret may be empty: %v", err)
	}
}

func TestSecretPollSeesRotation(t *testing.T) {
	p := newSecretFile(t, "old\n")
	var r recorder
	s, err := OpenSecret("APP_TOKEN_SIGNING_KEY_FILE", p, r.options())
	if err != nil {
		t.Fatal(err)
	}
	if s.Poll() {
		t.Fatal("no change yet")
	}
	ch := s.Changed()
	writeSecret(t, p, "new\n", 1)
	if !s.Poll() || s.Current() != "new" {
		t.Fatalf("after rotation Current = %q", s.Current())
	}
	select {
	case <-ch:
	default:
		t.Fatal("Changed channel not closed on change")
	}
	select {
	case <-s.Changed():
		t.Fatal("a fresh Changed channel must be open")
	default:
	}
	writeSecret(t, p, "new\n", 2) // touched, same content
	if s.Poll() {
		t.Fatal("same content is not a change")
	}
	if c, e := r.counts(); c != 1 || e != 0 || r.changes[0] != "APP_TOKEN_SIGNING_KEY_FILE" {
		t.Fatalf("OnChange %d OnError %d", c, e)
	}
}

func TestSecretKeepsLastGoodWhenDeleted(t *testing.T) {
	p := newSecretFile(t, "good\n")
	var r recorder
	s, err := OpenSecret("K_FILE", p, r.options())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if s.Poll() || s.Current() != "good" {
		t.Fatalf("Current = %q", s.Current())
	}
	s.Poll() // same broken state: reported once
	if _, e := r.counts(); e != 1 {
		t.Fatalf("OnError called %d times, want 1", e)
	}
	writeSecret(t, p, "back\n", 3)
	if !s.Poll() || s.Current() != "back" {
		t.Fatalf("after recreate Current = %q", s.Current())
	}
}

func TestSecretKeepsLastGoodWhenUnreadableOrEmptied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 file")
	}
	p := newSecretFile(t, "good\n")
	var r recorder
	o := r.options()
	o.Required = true
	s, err := OpenSecret("K_FILE", p, o)
	if err != nil {
		t.Fatal(err)
	}
	writeSecret(t, p, "leaked-if-logged\n", 1)
	if err := os.Chmod(p, 0); err != nil {
		t.Fatal(err)
	}
	if s.Poll() || s.Current() != "good" {
		t.Fatalf("unreadable: Current = %q", s.Current())
	}
	if err := os.Chmod(p, 0o600); err != nil {
		t.Fatal(err)
	}
	writeSecret(t, p, "", 2)
	if s.Poll() || s.Current() != "good" {
		t.Fatalf("emptied required: Current = %q", s.Current())
	}
	if _, e := r.counts(); e != 2 {
		t.Fatalf("OnError %d, want 2", e)
	}
	for _, err := range r.errs {
		if strings.Contains(err.Error(), "leaked") || strings.Contains(err.Error(), "good") {
			t.Errorf("error carries the secret: %v", err)
		}
	}
}

func TestSecretRunPollsUntilCancelled(t *testing.T) {
	p := newSecretFile(t, "v1\n")
	s, err := OpenSecret("K_FILE", p, SecretOptions{Interval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	ch := s.Changed()
	writeSecret(t, p, "v2\n", 1)
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not notice the change")
	}
	if s.Current() != "v2" {
		t.Fatalf("Current = %q", s.Current())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestSecretConcurrentReadsDuringRotation(t *testing.T) {
	p := newSecretFile(t, "v0\n")
	s, err := OpenSecret("K_FILE", p, SecretOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if c := s.Current(); !strings.HasPrefix(c, "v") {
					t.Errorf("torn read %q", c)
					return
				}
				_ = s.Bytes()
				_ = s.Changed()
			}
		}()
	}
	for i := 1; i <= 50; i++ {
		writeSecret(t, p, "v"+strings.Repeat("x", i)+"\n", i)
		s.Poll()
	}
	stop.Store(true)
	wg.Wait()
}
