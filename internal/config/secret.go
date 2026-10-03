package config

import (
	"bytes"
	"context"
	"os"
	"sync"
	"time"
)

// Secret polling bounds (P2.9): the file is compared at most 30 s apart.
const (
	DefaultSecretInterval = 10 * time.Second
	MaxSecretInterval     = 30 * time.Second
)

// SecretOptions configures OpenSecret.
type SecretOptions struct {
	Interval time.Duration // between two checks in Run; 0 = DefaultSecretInterval; at most MaxSecretInterval
	Required bool          // an empty file is CONFIG_MISSING at open, and a failed re-read later
	// OnChange is called after each successful change; the caller logs one INFO line naming the key.
	OnChange func(key string)
	// OnError is called once per failed re-read; the caller logs ERROR naming the key and counts
	// be_secret_reload_failures_total{key}. err never contains the secret.
	OnError func(key string, err error)
}

// fileSig is what Poll compares: modification time and size, or the class of a stat failure.
type fileSig struct {
	modNs  int64
	size   int64
	failed string
}

// Secret is a file-delivered secret (P2.7, P2.9): read when opened, re-read when the file's
// modification time or size changes, keeping the last good value when a re-read fails. It is safe for
// concurrent use; Run (or the caller calling Poll) drives the re-reads.
type Secret struct {
	key, path string
	o         SecretOptions

	pollMu sync.Mutex // serialises Poll; guards seen
	seen   fileSig

	mu      sync.RWMutex // guards raw, text, changed
	raw     []byte
	text    string
	changed chan struct{}
}

// OpenSecret reads the secret file at path at once (P2.9). An error is a configuration error at start
// (exit 78): a path that is not absolute, a file that is not a readable regular file, an empty file
// when o.Required, or an interval above MaxSecretInterval.
func OpenSecret(key, path string, o SecretOptions) (*Secret, error) {
	if e := checkSecretPath(key, path); e != nil {
		return nil, e
	}
	if o.Interval == 0 {
		o.Interval = DefaultSecretInterval
	}
	if o.Interval < 0 || o.Interval > MaxSecretInterval {
		return nil, newErr(ReasonInvalid, key, "secret re-read interval must be within 30s (P2.9)")
	}
	s := &Secret{key: key, path: path, o: o, changed: make(chan struct{})}
	s.seen = statSig(path)
	b, e := readSecretFile(key, path)
	if e != nil {
		return nil, e
	}
	text, _, err := SecretText(key, b, o.Required)
	if err != nil {
		return nil, err
	}
	s.raw, s.text = b, text
	return s, nil
}

// Key returns the secret's configuration key.
func (s *Secret) Key() string { return s.key }

// Current returns the text value: the file's content with one trailing LF or CRLF removed (P2.9).
func (s *Secret) Current() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.text
}

// Bytes returns a copy of the file's content byte for byte (a component's own binary secret).
func (s *Secret) Bytes() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return bytes.Clone(s.raw)
}

// Changed returns a channel closed at the next successful change; after that, call it again for the
// following one.
func (s *Secret) Changed() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.changed
}

// Poll compares the file's modification time and size with the last check and, when they differ,
// re-reads it. It reports whether the value changed. A failed re-read keeps the last good value and
// calls OnError once for that file state; identical content after a touch is not a change.
func (s *Secret) Poll() bool {
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	sig := statSig(s.path)
	if sig == s.seen {
		return false
	}
	s.seen = sig
	b, e := readSecretFile(s.key, s.path)
	if e != nil {
		s.fail(e)
		return false
	}
	text, _, err := SecretText(s.key, b, s.o.Required)
	if err != nil {
		s.fail(err)
		return false
	}
	s.mu.Lock()
	if bytes.Equal(b, s.raw) {
		s.mu.Unlock()
		return false
	}
	s.raw, s.text = b, text
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
	if s.o.OnChange != nil {
		s.o.OnChange(s.key)
	}
	return true
}

// Run polls every Interval until ctx is done. The caller supervises it (one goroutine per secret).
func (s *Secret) Run(ctx context.Context) {
	t := time.NewTicker(s.o.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Poll()
		}
	}
}

func (s *Secret) fail(err error) {
	if s.o.OnError != nil {
		s.o.OnError(s.key, err)
	}
}

// statSig stats path; a failure is a state of its own so the same failure is reported once.
func statSig(path string) fileSig {
	fi, err := os.Stat(path)
	if err != nil {
		return fileSig{failed: errClass(err)}
	}
	return fileSig{modNs: fi.ModTime().UnixNano(), size: fi.Size()}
}
