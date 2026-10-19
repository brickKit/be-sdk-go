// Package records seals and opens security records as they travel (D76; protocol P8.8): the canonical JSON bytes of
// a record without its own digest, with the digest beside them. Opening checks, in order, that the bytes are
// canonical, that the digest is theirs, and that the record passes its JSON Schema (be-protocol schemas/security,
// generated from the field tables); the conditional rules of the tables stay with the callers.
package records

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"

	protocol "github.com/brickKit/be-protocol/v3"
	"github.com/brickKit/be-sdk-go/v3/canon"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Major is the major version of every record type this SDK reads and writes.
const Major = 1

// ErrInvalid wraps every reason a received record is refused.
var ErrInvalid = errors.New("record refused")

var (
	once    sync.Once
	schemas map[string]*jsonschema.Schema
	loadErr error
)

func load() {
	c := jsonschema.NewCompiler()
	names, err := fs.Glob(protocol.Schemas, "schemas/security/*.schema.json")
	if err != nil {
		loadErr = err
		return
	}
	schemas = map[string]*jsonschema.Schema{}
	for _, n := range names {
		raw, err := fs.ReadFile(protocol.Schemas, n)
		if err != nil {
			loadErr = err
			return
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			loadErr = err
			return
		}
		if err := c.AddResource(n, doc); err != nil {
			loadErr = err
			return
		}
		s, err := c.Compile(n)
		if err != nil {
			loadErr = fmt.Errorf("%s: %w", n, err)
			return
		}
		schemas[strings.TrimSuffix(strings.TrimPrefix(n, "schemas/security/"), ".schema.json")] = s
	}
}

// Validate checks a decoded record (numbers as json.Number) against its record type's schema.
func Validate(recordType string, v any) error {
	once.Do(load)
	if loadErr != nil {
		return fmt.Errorf("the record schemas could not be loaded: %w", loadErr)
	}
	s := schemas[recordType]
	if s == nil {
		return fmt.Errorf("%w: no schema for record type %s", ErrInvalid, recordType)
	}
	if err := s.Validate(v); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrInvalid, recordType, err)
	}
	return nil
}

// Seal encodes a record for the wire: its canonical bytes and their digest. The record must not hold a digest.
func Seal(recordType string, v map[string]any) (raw []byte, digest string, err error) {
	raw, err = canon.Encode(v)
	if err != nil {
		return nil, "", err
	}
	return raw, canon.DigestBytes(recordType, Major, raw), nil
}

// Open checks a received record and returns it decoded.
func Open(recordType string, raw []byte, digest string) (map[string]any, error) {
	v, err := canon.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if got := canon.DigestBytes(recordType, Major, raw); got != digest {
		return nil, fmt.Errorf("%w: the digest does not match the bytes", ErrInvalid)
	}
	if err := Validate(recordType, v); err != nil {
		return nil, err
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: not an object", ErrInvalid)
	}
	return m, nil
}
