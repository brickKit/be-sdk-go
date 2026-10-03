package config

import (
	"regexp"
	"strings"
)

// Platform names (P2.2, P2.4): brickKit injects them; a component reads them without declaring them
// (except BRICKKIT_SERVED_MEMBERS*, which only the shell launcher reads) and never declares any.
const (
	KeyComponentID      = "COMPONENT_ID"
	KeyComponentVersion = "COMPONENT_VERSION"
	KeyPort             = "PORT"
	endpointSuffix      = "_ENDPOINT"
	servedMembersPrefix = "BRICKKIT_SERVED_MEMBERS"
	secretSuffix        = "_FILE"
	mountFile           = "file"
)

var keyNameRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// IsPlatformName reports whether key is a platform name a component reads without declaring it:
// COMPONENT_ID, COMPONENT_VERSION, PORT or any *_ENDPOINT (P2.2, vectors read_undeclared).
func IsPlatformName(key string) bool {
	return key == KeyComponentID || key == KeyComponentVersion || key == KeyPort ||
		strings.HasSuffix(key, endpointSuffix)
}

// CheckKeyName checks that a component may declare key (P2.4, vectors key_name): an upper-snake
// environment variable name that is not a platform name.
func CheckKeyName(key string) error {
	if !keyNameRe.MatchString(key) {
		return newErr(ReasonKeyInvalid, key, "not an upper-snake environment variable name")
	}
	if IsPlatformName(key) || strings.HasPrefix(key, servedMembersPrefix) {
		return newErr(ReasonKeyReserved, key, "a platform name, never declared")
	}
	return nil
}

// KeyDeclaration is the part of a configSchema item that P2.12 constrains.
type KeyDeclaration struct {
	Key    string `json:"key"`
	Secret bool   `json:"secret"`
	Mount  string `json:"mount"` // "" = not declared
	Type   string `json:"type"`  // brickKit type; "" = string
}

// CheckKeyDeclaration checks a configSchema item against P2.4 and P2.12 (vectors key_declaration):
// secret: true <=> mount: file <=> the name ends in _FILE; brickKit's own rules first (mount is only
// file, only with secret: true, only on a string).
func CheckKeyDeclaration(d KeyDeclaration) error {
	if err := CheckKeyName(d.Key); err != nil {
		return err
	}
	typ := d.Type
	if typ == "" {
		typ = "string"
	}
	hasMount := d.Mount != ""
	fileName := strings.HasSuffix(d.Key, secretSuffix)
	switch {
	case hasMount && d.Mount != mountFile:
		return newErr(ReasonMountInvalid, d.Key, "mount is only file")
	case hasMount && !d.Secret:
		return newErr(ReasonMountNeedsSecret, d.Key, "mount: file only with secret: true")
	case hasMount && typ != "string":
		return newErr(ReasonMountNeedsString, d.Key, "mount: file only on a string")
	case d.Secret && !hasMount:
		return newErr(ReasonSecretNotFile, d.Key, "a secret is declared mount: file")
	case d.Secret && !fileName:
		return newErr(ReasonFileSuffixReq, d.Key, "a secret's name ends in _FILE")
	case !d.Secret && fileName:
		return newErr(ReasonFileSuffixRsv, d.Key, "_FILE is reserved for file-delivered secrets")
	}
	return nil
}
