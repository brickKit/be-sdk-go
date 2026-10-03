package authz

import (
	"slices"
	"time"
)

// Reasons a Decision carries when it denies (be-protocol P4 platform reasons).
const (
	// ReasonTokenStale: the token predates a role change or its delegation grant was revoked (P5.6, E2).
	ReasonTokenStale = "TOKEN_STALE"
	// ReasonUnsupportedDelegation: the token is delegated in a way the provider does not offer (P5.5, E2).
	ReasonUnsupportedDelegation = "UNSUPPORTED_DELEGATION"
	// ReasonMissingPermission: the caller does not hold the route's key (P6.2, E5).
	ReasonMissingPermission = "MISSING_PERMISSION"
	// ReasonAuthzNotReady: no bundle has been accepted yet (P6.2, E1).
	ReasonAuthzNotReady = "AUTHZ_NOT_READY"
)

// staleGrace is E2's 5 s allowance between stale_since and a token's iat (P5.6).
const staleGrace = 5

// Token is what the decisions need from a verified access token (P5.5). The root package
// maps authn.Claims onto it; this package never sees the raw token (P5.8).
type Token struct {
	Sub      string
	IssuedAt time.Time
	Roles    []string
	DeptPath string // the dept_path claim; empty or malformed means no department (P6.4, E6)
	Act      *Act
	Ceil     []string
	DG       string
}

// Act is one link of the token's act chain (RFC 8693 §4.1).
type Act struct {
	Sub  string `json:"sub"`
	Kind string `json:"kind"`
	Act  *Act   `json:"act,omitempty"`
}

// Decision is the outcome of the keys-only route decision (P6.2). Reason is empty when Allow is true.
type Decision struct {
	Allow  bool
	Reason string
}

// Decide is the keys-only route decision for key at now (P6.2): AUTHZ_NOT_READY without a bundle,
// then the token checks (E2), then has(key) (E3–E5).
func Decide(b *Bundle, t Token, key string, now time.Time) Decision {
	if reason := CheckToken(b, t); reason != "" {
		return Decision{Reason: reason}
	}
	if !HasKey(b, t, key, now) {
		return Decision{Reason: ReasonMissingPermission}
	}
	return Decision{Allow: true}
}

// CheckToken runs E2's token checks in order and returns the first failing reason, or "" when the
// token passes (P5.5, P5.6). Without a bundle it returns AUTHZ_NOT_READY.
func CheckToken(b *Bundle, t Token) string {
	if b == nil {
		return ReasonAuthzNotReady
	}
	if since, ok := b.StaleSince[t.Sub]; ok && t.IssuedAt.Unix() < since-staleGrace {
		return ReasonTokenStale
	}
	if t.DG != "" {
		if _, revoked := b.RevokedGrants[t.DG]; revoked {
			return ReasonTokenStale
		}
	}
	delegated := t.Act != nil || len(t.Ceil) > 0 || t.DG != ""
	if delegated && !b.Capabilities.Delegation {
		return ReasonUnsupportedDelegation
	}
	for a := t.Act; a != nil; a = a.Act {
		if !actKindOffered(b.Capabilities, a.Kind) {
			return ReasonUnsupportedDelegation
		}
	}
	return ""
}

// actKindOffered is E2 step 4: agent needs agents, user (impersonation) needs impersonation, svc
// needs nothing more, any other kind is unsupported.
func actKindOffered(c Capabilities, kind string) bool {
	switch kind {
	case "agent":
		return c.Agents
	case "user":
		return c.Impersonation
	case "svc":
		return true
	default:
		return false
	}
}

// HasKey is E5's has(key) at now: some active role (E3) holds key or an on_behalf delegation to the
// token's sub covers it, and the ceilings allow it (E4). It does not run the token checks.
func HasKey(b *Bundle, t Token, key string, now time.Time) bool {
	if b == nil || !ceilingsAllow(b, t.Ceil, key) {
		return false
	}
	unix := now.Unix()
	for _, r := range t.Roles {
		if roleActive(b, r, unix) && slices.Contains(b.Roles[r], key) {
			return true
		}
	}
	return len(OnBehalf(b, t.Sub, key, now)) > 0
}

// OnBehalf returns D_K (E5): the on_behalf delegations to sub that cover key and are inside their
// window at now, when the capability delegation is on; otherwise none.
func OnBehalf(b *Bundle, sub, key string, now time.Time) []Delegation {
	if b == nil || !b.Capabilities.Delegation {
		return nil
	}
	unix := now.Unix()
	var out []Delegation
	for _, d := range b.Delegations {
		if d.Mode == "on_behalf" && d.To == sub && slices.Contains(d.Keys, key) && inWindow(d.FromTS, d.Until, unix) {
			out = append(out, d)
		}
	}
	return out
}

// ceilingsAllow is E4: with no ceilings every key is allowed; otherwise every named profile lists key
// in keys or fields, and an unknown profile code is an empty profile.
func ceilingsAllow(b *Bundle, ceil []string, key string) bool {
	for _, code := range ceil {
		p, ok := b.Profiles[code]
		if !ok || !(slices.Contains(p.Keys, key) || slices.Contains(p.Fields, key)) {
			return false
		}
	}
	return true
}

// roleActive is E3: a role without a grants entry is active; otherwise its window must contain now.
func roleActive(b *Bundle, role string, unix int64) bool {
	g, ok := b.Grants[role]
	return !ok || inWindow(g.FromTS, g.Until, unix)
}

// inWindow reports whether unix lies in [from, until); a nil bound is open (E3).
func inWindow(from, until *int64, unix int64) bool {
	return (from == nil || *from <= unix) && (until == nil || unix < *until)
}
