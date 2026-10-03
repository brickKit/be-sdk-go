// Package config implements be-protocol P2 (configuration) for be-sdk-go: the protocol key catalogue
// (schemas/config-keys.yaml), a component's configSchema read from its component.yaml, strict typed
// parsing of every declared key with all errors collected (P2.3), dependency and slot-family address
// reading (P2.5, P2.6, P2.10), key-name and secret-declaration rules (P2.4, P2.12) and file-delivered
// secrets that are re-read when they change (P2.7, P2.9).
//
// The package holds no package-level mutable state and never reads the process environment: the
// caller passes a Source (os.LookupEnv standalone, the member's map in a shell, P2.1).
//
// Programming errors (reading an undeclared key, reading a key with the wrong getter) panic with a
// *Error; the runtime's start phase recovers it with AsError and exits 78 (P1.2, P2.2).
package config
