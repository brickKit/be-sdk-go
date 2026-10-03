package besdk

import (
	"fmt"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/config"
)

// Config holds the component's configuration: only the keys its configSchema declares, every one
// parsed and checked at start (P2.2, P2.3). The getters return no error: a wrong getter or an
// undeclared key is a programming error that panics with a configuration error, which the start
// phase turns into exit 78.
type Config struct {
	vals    *config.Values
	secrets map[string]*config.Secret
}

// Require returns a key that has a value; the schema should declare it required (or with a default).
func (c *Config) Require(key string) string {
	s, ok := c.vals.String(key)
	if !ok {
		panic(&config.Error{Reason: config.ReasonMissing, Key: key, Detail: "required by the component but not set"})
	}
	return s
}

// String returns a text value, or def when the key is not set.
func (c *Config) String(key, def string) string {
	if s, ok := c.vals.String(key); ok {
		return s
	}
	return def
}

// Int returns an integer value, or def.
func (c *Config) Int(key string, def int) int {
	if v, ok := c.vals.Int(key); ok {
		return int(v)
	}
	return def
}

// Bool returns a boolean value, or def.
func (c *Config) Bool(key string, def bool) bool {
	if v, ok := c.vals.Bool(key); ok {
		return v
	}
	return def
}

// Duration returns a duration value (Go syntax), or def.
func (c *Config) Duration(key string, def time.Duration) time.Duration {
	if v, ok := c.vals.Duration(key); ok {
		return v
	}
	return def
}

// JSON decodes a JSON value into into; a key that is not set leaves into unchanged.
func (c *Config) JSON(key string, into any) error {
	_, err := c.vals.JSON(key, into)
	return err
}

// Has reports whether a declared key has a value.
func (c *Config) Has(key string) bool { return c.vals.Has(key) }

// Secret is a file-delivered secret (P2.7, P2.9): read when used, re-read when the file changes.
type Secret interface {
	Current() string          // the text value, one trailing newline removed
	Bytes() []byte            // the file's bytes, for a component's own binary secret
	Changed() <-chan struct{} // closed at the next successful change
}

// Secret returns a declared secret key (`…_FILE`, P2.12); String on such a key returns its path.
func (c *Config) Secret(key string) Secret {
	if !strings.HasSuffix(key, "_FILE") {
		panic(&config.Error{Reason: config.ReasonInvalid, Key: key, Detail: "only a …_FILE key is a secret (P2.12)"})
	}
	s, ok := c.secrets[key]
	if !ok {
		if _, declared := c.vals.Decl(key); !declared {
			panic(&config.Error{Reason: config.ReasonUndeclared, Key: key, Detail: "not declared in configSchema"})
		}
		panic(&config.Error{Reason: config.ReasonMissing, Key: key, Detail: "secret not set"})
	}
	return s
}

// openSecrets opens every set secret key at start (P2.9: an unreadable required secret is a
// configuration error). The serving runtime never opens PG_OWNER_PASSWORD_FILE (P10.12): only the
// migrate entry point reads the owner's file.
func openSecrets(vals *config.Values, opts func(key string) config.SecretOptions, skip ...string) (map[string]*config.Secret, []*config.Error) {
	out := map[string]*config.Secret{}
	var errs []*config.Error
	for _, d := range vals.Schema().Decls {
		if !d.Secret || contains(skip, d.Name) {
			continue
		}
		path, ok := vals.String(d.Name)
		if !ok {
			continue
		}
		o := opts(d.Name)
		o.Required = d.Required
		s, err := config.OpenSecret(d.Name, path, o)
		if err != nil {
			ce, isCfg := config.AsError(err)
			if !isCfg {
				ce = &config.Error{Reason: config.ReasonInvalid, Key: d.Name, Detail: fmt.Sprint(err)}
			}
			errs = append(errs, ce)
			continue
		}
		out[d.Name] = s
	}
	return out, errs
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
