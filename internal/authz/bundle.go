// Package authz holds the component side of the authorization bundle: fetching and accepting the
// bundle (be-protocol P6.1, contract-infra-authz EVALUATION.md E1) and the keys-only route decision
// (P6.2; E2–E5). Levels, dimensions, subject sets and the projection are a later wave; the bundle keeps
// the fields and the raw JSON they will need. The package does not import internal/authn: the root
// package maps verified claims to a Token.
package authz

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

// ErrBundleRefused marks a bundle that is not used: its contract is missing or not authz/2.x, or it is
// not well-formed JSON of the bundle's shape (P6.1, E1). A refused bundle never replaces an accepted one.
var ErrBundleRefused = errors.New("authz: bundle refused")

// contractPattern is E1's acceptance pattern, anchored to the whole string.
var contractPattern = regexp.MustCompile(`^authz/2\.(0|[1-9][0-9]*)$`)

// Bundle is an accepted authz/2.x bundle (contract-infra-authz schemas/bundle.schema.json). Unknown
// members are ignored (E1); Raw keeps the whole document for later waves. A Bundle is immutable once
// parsed and safe to share between goroutines.
type Bundle struct {
	Contract      string              `json:"contract"`
	Revision      string              `json:"revision"`
	Capabilities  Capabilities        `json:"capabilities"`
	Roles         map[string][]string `json:"roles"`
	Grants        map[string]Grant    `json:"grants"`
	Profiles      map[string]Profile  `json:"profiles"`
	Delegations   []Delegation        `json:"delegations"`
	StaleSince    map[string]int64    `json:"stale_since"`
	RevokedGrants map[string]int64    `json:"revoked_grants"`
	CatalogDigest string              `json:"catalog_digest"`
	Raw           json.RawMessage     `json:"-"`
}

// Grant is a role's data-scope assignment and validity window (E3, E6, E7). FromTS is inclusive,
// Until exclusive, both unix seconds; nil is an open bound.
type Grant struct {
	Levels       map[string]string   `json:"levels"`
	DefaultLevel string              `json:"default_level"`
	Values       map[string][]string `json:"values"`
	FromTS       *int64              `json:"from_ts"`
	Until        *int64              `json:"until"`
}

// Profile is a ceiling profile named by a token's ceil claim (E4).
type Profile struct {
	Keys      []string `json:"keys"`
	Fields    []string `json:"fields"`
	MaxLevel  string   `json:"max_level"`
	Relations []string `json:"relations"`
}

// Delegation is one entry of the bundle's delegations (E3, E5). FromTS and Until as in Grant.
type Delegation struct {
	ID       string   `json:"id"`
	Mode     string   `json:"mode"`
	From     string   `json:"from"`
	To       string   `json:"to"`
	Keys     []string `json:"keys"`
	Profiles []string `json:"profiles,omitempty"`
	FromTS   *int64   `json:"from_ts,omitempty"`
	Until    *int64   `json:"until,omitempty"`
}

// ParseBundle accepts or refuses a bundle document (P6.1, E1). The error wraps ErrBundleRefused.
func ParseBundle(raw []byte) (*Bundle, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil || probe == nil {
		return nil, fmt.Errorf("%w: not a JSON object", ErrBundleRefused)
	}
	var contract string
	if err := json.Unmarshal(probe["contract"], &contract); err != nil || !contractPattern.MatchString(contract) {
		return nil, fmt.Errorf("%w: contract %s does not match authz/2.x", ErrBundleRefused, truncate(probe["contract"]))
	}
	var b Bundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBundleRefused, err)
	}
	b.Raw = bytes.Clone(raw)
	return &b, nil
}

// truncate renders a raw member for an error message without letting a huge value into the log.
func truncate(v json.RawMessage) string {
	if v == nil {
		return "(missing)"
	}
	const max = 64
	if len(v) > max {
		return string(v[:max]) + "…"
	}
	return string(v)
}
