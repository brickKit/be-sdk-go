package config

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// FileSource is a Source that can also read files, for <KEY>_FILE secrets.
type FileSource interface {
	Source
	ReadFile(path string) ([]byte, error)
}

// Env is the single-run Source: the process environment and the files it points at. Only the launcher builds it.
type Env struct {
	LookupEnv func(string) (string, bool)
	Read      func(string) ([]byte, error)
}

// ProcessEnv reads the real process environment. Called by the launcher, never by module code.
func ProcessEnv() Env { return Env{LookupEnv: os.LookupEnv, Read: os.ReadFile} }

// Lookup implements Source.
func (e Env) Lookup(name string) (string, bool) { return e.LookupEnv(name) }

// ReadFile implements FileSource.
func (e Env) ReadFile(path string) ([]byte, error) { return e.Read(path) }

// Secret is a secret value; when it came from a file, Refresh re-reads that file (P2.8).
type Secret struct {
	path  string
	value atomic.Value // string
}

// Value returns the current value. Never log it.
func (s *Secret) Value() string { return s.value.Load().(string) }

// FromFile reports whether the secret is re-readable.
func (s *Secret) FromFile() bool { return s.path != "" }

// Refresh re-reads the file; changed reports whether the value differs. A failed read keeps the old value.
func (s *Secret) Refresh(read func(string) ([]byte, error)) (changed bool, err error) {
	if s.path == "" {
		return false, nil
	}
	b, err := read(s.path)
	if err != nil {
		return false, err
	}
	nv := trimNewline(b)
	if nv == s.Value() {
		return false, nil
	}
	s.value.Store(nv)
	return true, nil
}

// Values are a component's parsed configuration. Asking for an undeclared key, or with the wrong type, panics:
// that is a programming error the SDK's recovery turns into INTERNAL, not something to handle at run time.
type Values struct {
	keys    map[string]Key
	vals    map[string]any
	secrets map[string]*Secret
}

func (v *Values) get(name string, t Type) (any, bool) {
	k, ok := v.keys[name]
	if !ok {
		panic(fmt.Sprintf("config: key %s is not declared", name))
	}
	if k.Type != t && !(t == String && k.Type == Enum) {
		panic(fmt.Sprintf("config: key %s is %s, read as %s", name, k.Type, t))
	}
	val, set := v.vals[name]
	return val, set
}

// Has reports whether an optional key without a default is set.
func (v *Values) Has(name string) bool {
	if _, ok := v.keys[name]; !ok {
		panic(fmt.Sprintf("config: key %s is not declared", name))
	}
	_, set := v.vals[name]
	return set
}

// String returns a string or enum key ("" when an optional key is unset and has no default).
func (v *Values) String(name string) string {
	if s, ok := v.secrets[name]; ok {
		return s.Value()
	}
	val, ok := v.get(name, String)
	if !ok {
		return ""
	}
	return val.(string)
}

// Int returns an integer key (0 when unset without a default).
func (v *Values) Int(name string) int64 {
	val, ok := v.get(name, Integer)
	if !ok {
		return 0
	}
	return val.(int64)
}

// Bool returns a boolean key (false when unset without a default).
func (v *Values) Bool(name string) bool {
	val, ok := v.get(name, Boolean)
	if !ok {
		return false
	}
	return val.(bool)
}

// Duration returns a duration key (0 when unset without a default).
func (v *Values) Duration(name string) time.Duration {
	val, ok := v.get(name, Duration)
	if !ok {
		return 0
	}
	return val.(time.Duration)
}

// Secret returns a secret key's handle (nil when an optional secret is unset).
func (v *Values) Secret(name string) *Secret {
	if k, ok := v.keys[name]; !ok || !k.Secret {
		panic(fmt.Sprintf("config: key %s is not a declared secret", name))
	}
	return v.secrets[name]
}

// Secrets returns every secret read from a file, for the runtime's re-read loop.
func (v *Values) Secrets() []*Secret {
	var out []*Secret
	for _, s := range v.secrets {
		if s.FromFile() {
			out = append(out, s)
		}
	}
	return out
}

// Raw returns the parsed value of a key for comparisons across shell members (P2.11); secrets are not exposed.
func (v *Values) Raw(name string) (any, bool) {
	if k, ok := v.keys[name]; !ok || k.Secret {
		return nil, false
	}
	val, ok := v.vals[name]
	return val, ok
}
