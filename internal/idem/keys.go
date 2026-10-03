package idem

import (
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// TTL is how long a key is bound after its first use (P13.7): 30 days, 720 h exactly.
const TTL = 30 * 24 * time.Hour

// ExpiresAt is created_at + 30 days (P13.7). Expiry is half-open: at expires_at the key is already new.
func ExpiresAt(createdAt time.Time) time.Time { return createdAt.Add(TTL) }

// CallerKind is who issued a command (P13.1).
type CallerKind string

// The three caller kinds of P13.1.
const (
	CallerUser       CallerKind = "user"        // a user request: namespace user:<sub>
	CallerSystemCall CallerKind = "system_call" // another component's system call: svc:<be-caller>
	CallerBackground CallerKind = "background"  // the component's own background work: system
)

// SystemNamespace is the namespace of the component's own background work (P13.1).
const SystemNamespace = "system"

// Caller identifies the issuer of a command; Sub is set for a user, BeCaller (the verified be-caller
// component ID, P7) for a system call.
type Caller struct {
	Kind     CallerKind
	Sub      string
	BeCaller string
}

// Namespace is the caller's key namespace (P13.1, P13.4): user:<sub>, svc:<be-caller> or system. A
// user without sub, a system call without be-caller or an unknown kind is a programming error.
func (c Caller) Namespace() (string, error) {
	switch c.Kind {
	case CallerUser:
		if c.Sub != "" {
			return "user:" + c.Sub, nil
		}
	case CallerSystemCall:
		if c.BeCaller != "" {
			return "svc:" + c.BeCaller, nil
		}
	case CallerBackground:
		return SystemNamespace, nil
	}
	return "", fmt.Errorf("idem: caller %+v has no namespace", c)
}

// ResolveKey unifies the Idempotency-Key header and the body field idempotency_key (P3.7); "" means
// absent. Equal or only one present → that key; neither → "" (the command runs without idempotency);
// both present and different → IDEMPOTENCY_MISMATCH (400).
func ResolveKey(header, body string) (string, error) {
	switch {
	case header == "":
		return body, nil
	case body == "" || body == header:
		return header, nil
	}
	return "", problem.Be("IDEMPOTENCY_MISMATCH", nil)
}
