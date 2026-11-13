// Package seed is for the seed commands of components (be-assembly-standard's seed and test data conventions): a
// small client of a running component's user surface, logging a test account in through the identity member's
// development exchange, and the authorization changes a seed makes for its own operations (roles, grants, revocations,
// management units) at the authorization member's effect point. Development and test environments only: the
// development exchange exists nowhere else. Never imported by module code.
//
// Every change carries a change key the effect point binds to its first result, so a second run answers the same
// records and creates nothing. A key that links to another component's row (a grantee's principal) carries that row's
// real identity: after the identity member is reset, the same account is another principal and gets a new grant.
package seed

import (
	"bytes"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	sdkauthz "github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/errs"
)

// Client calls one component's user surface with a bearer token.
type Client struct {
	Base, Token string
	HTTP        *http.Client
	// Patience is how long Do keeps sending a request that answers a retryable 503, Pause how long it waits between.
	Patience, Pause time.Duration
}

// NewClient is a client of the component at base, as the holder of token. Patience covers a member that has just
// restarted: its view arrives within seconds, or after the authorization member's own restart.
func NewClient(base, token string) Client {
	return Client{Base: base, Token: token, HTTP: &http.Client{Timeout: 30 * time.Second}, Patience: 90 * time.Second,
		Pause: time.Second}
}

// Do sends a request; header holds name and value pairs. The body is decoded as an object when it is one. An answer
// that says nothing was done (503 with a reason the protocol's catalog marks retryable: not ready, no authorization
// view yet, a dependency unavailable) is waited out: the same request is sent again for up to Patience, and the last
// answer comes back. Seeds are written to be run again, so a repeated request is safe.
func (c Client) Do(method, path string, body any, header ...string) (int, map[string]any, error) {
	deadline := time.Now().Add(c.Patience)
	for {
		code, out, err := c.once(method, path, body, header...)
		if err != nil || !notDone(code, out) || time.Now().After(deadline) {
			return code, out, err
		}
		time.Sleep(c.Pause)
	}
}

// notDone reports a 503 whose platform reason the catalog marks retryable.
func notDone(code int, out map[string]any) bool {
	if code != http.StatusServiceUnavailable {
		return false
	}
	reason, _ := out["reason"].(string)
	domain, _ := out["domain"].(string)
	e, ok := errs.Platform(reason)
	return ok && domain == errs.PlatformDomain && e.Retry == errs.RetryYes
}

// once sends the request one time.
func (c Client) once(method, path string, body any, header ...string) (int, map[string]any, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.Base+path, r)
	if err != nil {
		return 0, nil, err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out, nil
}

// Must is Do that wants 200.
func (c Client) Must(method, path string, body any, header ...string) (map[string]any, error) {
	code, out, err := c.Do(method, path, body, header...)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	if code != 200 {
		return out, fmt.Errorf("%s %s: %d %v", method, path, code, out)
	}
	return out, nil
}

// Ready reports whether the component at base answers its readiness probe.
func Ready(base string) bool {
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(base + "/readyz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

// Account is a test account logged in: its token and its platform principal.
type Account struct{ User, Token, Principal string }

// Login exchanges a test account's password for a platform token at the identity member's development exchange
// (iam is its base address); the first login also binds the account's principal.
func Login(iam, user, password string) (Account, error) {
	out, err := NewClient(iam, "").Must("POST", "/api/iam/dev/token", map[string]string{"username": user, "password": password})
	if err != nil {
		return Account{}, fmt.Errorf("the development exchange refused %s (are the seed accounts there? make -C "+
			"components/infra/iam-casdoor seed): %w", user, err)
	}
	tok, _ := out["access_token"].(string)
	p, _ := out["principal"].(string)
	if tok == "" || p == "" {
		return Account{}, fmt.Errorf("the development exchange answered no token or principal for %s", user)
	}
	return Account{User: user, Token: tok, Principal: p}, nil
}

// Accounts logs every user in (see Login).
func Accounts(iam, password string, users ...string) (map[string]Account, error) {
	out := map[string]Account{}
	for _, u := range users {
		a, err := Login(iam, u, password)
		if err != nil {
			return nil, err
		}
		out[u] = a
	}
	return out, nil
}

// WaitGranted waits until a write the holder of c's token sends with an empty body is refused for its body (400)
// rather than not found (404): the component checks the operation before the body, so 400 means a grant has reached
// the component's view, and nothing is written. A read does not tell when a list answers 200 with nothing in it to a
// user without the read operation. A component with nothing but reads (infra/workflow's worklist) has no write to
// probe: when its read checks the operation first and answers 404 without the grant, wait with UntilGranted on that
// read until it answers 200.
func (c Client) WaitGranted(method, path string, timeout time.Duration) error {
	if method == "GET" || method == "HEAD" { // a read never answers 400 for its body: it would wait out the timeout
		return fmt.Errorf("WaitGranted %s %s: a read; wait with UntilGranted until it answers 200", method, path)
	}
	deadline := time.Now().Add(timeout)
	for {
		code, out, err := c.Do(method, path, map[string]any{})
		if err == nil && code == 400 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s %s still answers %d %v after %s: the grant has not reached the component", method, path,
				code, out, timeout)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// UntilGranted sends one request until the grant that allows it has reached the component: while it answers 403 or
// 404 (not allowed, not visible: nothing was written) it waits and sends it again; any other answer is returned. For
// the first write of a seed to a component that checks the request (its body, its Idempotency-Key) before the
// operation, where WaitGranted cannot tell. A 404 that is not about the grant (a referenced record is missing) comes
// back after timeout.
func (c Client) UntilGranted(timeout time.Duration, method, path string, body any, header ...string) (int, map[string]any, error) {
	deadline := time.Now().Add(timeout)
	for {
		code, out, err := c.Do(method, path, body, header...)
		if err != nil || (code != 403 && code != 404) || time.Now().After(deadline) {
			return code, out, err
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// Since is the start of the seed grants' validity: a fixed instant, so a second run sends the same change.
const Since = "2026-01-01T00:00:00Z"

// Authz makes one component's authorization changes as an authorization admin (or a unit's admin).
type Authz struct {
	c         Client
	component string
	root      string
	owner     ed25519.PrivateKey // the deployment owner's confirmation key (SEED_OWNER_KEY), for protected changes
}

// NewAuthz is the effect point at base, used by component (it prefixes every change key) as the holder of token.
func NewAuthz(base, token, component string) (*Authz, error) {
	a := &Authz{c: NewClient(base, token), component: component}
	out, err := a.c.Must("GET", "/api/authz/records?type=management_unit", nil)
	if err != nil {
		return nil, err
	}
	recs, _ := out["records"].([]any)
	for _, r := range recs {
		rec, _ := r.(map[string]any)
		body, _ := rec["body"].(map[string]any)
		if root, _ := body["root"].(bool); root {
			a.root, _ = rec["id"].(string)
		}
	}
	if a.root == "" {
		return nil, fmt.Errorf("the authorization member has no root unit (is it claimed? AUTHZ_INSTALL_CLAIM, POST /api/authz/claim)")
	}
	return a, nil
}

// As is the same effect point used by another account (a unit's admin granting within its unit).
func (a *Authz) As(token string) *Authz {
	return &Authz{c: NewClient(a.c.Base, token), component: a.component, root: a.root, owner: a.owner}
}

// WithOwner confirms protected changes (new units, holdings) with the deployment owner's key, as a deployment with a
// single authorization admin must (security 08, A124).
func (a *Authz) WithOwner(key ed25519.PrivateKey) *Authz {
	b := *a
	b.owner = key
	return &b
}

// OwnerKey reads the deployment owner's private key: PEM, PKCS #8, Ed25519.
func OwnerKey(path string) (ed25519.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("%s: not PEM", path)
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	ed, ok := k.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s: not an Ed25519 key", path)
	}
	return ed, nil
}

// Root is the root management unit.
func (a *Authz) Root() string { return a.root }

// Segment grants operation on resourceType everywhere it is visible, from Since: every dimension named in all (a
// segment carries one atom for each dimension of the resource, security 09a) holds every value; none for a resource
// without dimensions.
func Segment(resourceType, operation string, all ...string) sdkauthz.Segment {
	atoms := map[string]sdkauthz.Atom{}
	for _, d := range all {
		atoms[d] = sdkauthz.Atom{Kind: "all"}
	}
	return sdkauthz.Segment{ResourceType: resourceType, Operation: operation, Atoms: atoms,
		ReadFields: []string{}, Valid: sdkauthz.Window{From: Since}}
}

// Change sends one change request (security 09a's be.security.change_request) under the key "seed:<component>:<key>"
// and returns the record it made (or made the first time). A tightening goes to the member's tightening route (its
// own operation, infra.authz.security_record.tighten), everything else to the changes.
func (a *Authz) Change(key string, change map[string]any) (string, error) {
	change["change_key"] = "seed:" + a.component + ":" + key
	path := "/api/authz/changes"
	if change["direction"] == "tighten" {
		path = "/api/authz/tightenings"
	}
	code, out, err := a.c.Do("POST", path, change)
	if err != nil {
		return "", fmt.Errorf("POST %s: %w", path, err)
	}
	rec, _ := out["record"].(string)
	switch {
	case code == 200:
		return rec, nil
	case code == 202 && out["outcome"] == "awaiting_confirmation" && rec != "":
		return rec, a.confirmAsOwner(rec, change)
	}
	return "", fmt.Errorf("POST %s: %d %v", path, code, out)
}

// confirmAsOwner confirms a saved protected change with the deployment owner's key, then waits for it to take effect
// (the member's job applies it once confirmed).
func (a *Authz) confirmAsOwner(record string, change map[string]any) error {
	if a.owner == nil {
		return fmt.Errorf("record %s waits for a confirmation: give the deployment owner's key (SEED_OWNER_KEY)", record)
	}
	p, err := a.c.Must("GET", "/api/authz/confirmations/owner-payload?target="+record, nil)
	if err != nil {
		return err
	}
	str := func(k string) string { v, _ := p[k].(string); return v }
	o := sdkauthz.OwnerConfirmation{Deployment: str("deployment"), Target: str("target"),
		CandidateDigest: str("candidate_digest"), ExpectedRevision: str("expected_revision"), ExpiresAt: str("expires_at")}
	sig, err := o.Sign(a.owner)
	if err != nil {
		return err
	}
	code, out, err := a.c.Do("POST", "/api/authz/confirmations", map[string]string{"target": record,
		"basis": "deployment_owner", "expires_at": o.ExpiresAt, "signature": sig})
	if err != nil || code != 201 {
		return fmt.Errorf("confirming %s as the deployment owner: %d %v %v", record, code, out, err)
	}
	recordType := ""
	if c, ok := change["candidate"].(json.RawMessage); ok {
		var head struct {
			RecordType string `json:"record_type"`
		}
		_ = json.Unmarshal(c, &head)
		recordType = head.RecordType
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		out, err := a.c.Must("GET", "/api/authz/records?type="+recordType, nil)
		if err != nil {
			return err
		}
		recs, _ := out["records"].([]any)
		for _, r := range recs {
			if m, _ := r.(map[string]any); m["id"] == record {
				return nil
			}
		}
	}
	return fmt.Errorf("record %s was confirmed but did not take effect within 30 s", record)
}

// widen submits a new record: direction widen with the candidate and its digest (security 09a).
func (a *Authz) widen(key, recordType string, candidate map[string]any) (string, error) {
	candidate["record_type"] = recordType
	raw, err := json.Marshal(candidate)
	if err != nil {
		return "", err
	}
	digest, err := sdkauthz.CandidateDigest(recordType, raw)
	if err != nil {
		return "", err
	}
	return a.Change(key, map[string]any{"direction": "widen", "candidate": json.RawMessage(raw), "candidate_digest": digest})
}

// Role defines a role whose template is segs; it answers the role revision.
func (a *Authz) Role(key, name string, segs []sdkauthz.Segment) (string, error) {
	return a.widen("role:"+key, "role_revision", map[string]any{"name": name, "segments_template": segs})
}

// GrantRole grants a role revision to a principal in unit (the root when ""); the key carries the principal.
func (a *Authz) GrantRole(key, unit, principal, roleRevision string) (string, error) {
	return a.widen("grant:"+key+":"+principal, "grant", map[string]any{"governance_unit": a.unitOr(unit),
		"grantee": principal, "role_revision": roleRevision})
}

// Grant grants segs to a principal in unit (the root when ""); the key carries the principal.
func (a *Authz) Grant(key, unit, principal string, segs []sdkauthz.Segment) (string, error) {
	return a.widen("grant:"+key+":"+principal, "grant", map[string]any{"governance_unit": a.unitOr(unit),
		"grantee": principal, "segments": segs})
}

// Revoke ends a grant (a tightening of its target).
func (a *Authz) Revoke(key, grant string) (string, error) {
	return a.Change("revoke:"+key+":"+grant, map[string]any{"direction": "tighten", "target": grant})
}

// Unit creates a management unit under parent (the root when "") whose admins grant operations within scope. It is a
// leaf: its holders grant, bind roles and revoke within it, and no unit is made below it (max_subdelegation_depth 0).
func (a *Authz) Unit(key, parent, name string, scope []sdkauthz.Segment, operations []string) (string, error) {
	return a.widen("unit:"+key, "management_unit", map[string]any{"parent": a.unitOr(parent), "name": name,
		"managed_scope": scope, "grantable_operations": operations,
		"management_actions": []string{"grant", "bind_role", "revoke"}, "max_subdelegation_depth": 0})
}

// Hold makes a principal an admin of unit; the key carries the principal.
func (a *Authz) Hold(key, unit, principal string) (string, error) {
	return a.widen("hold:"+key+":"+principal, "management_holding", map[string]any{"unit": unit, "holder": principal})
}

// RoleSpec is one role of a component's seed and the test accounts it is granted to.
type RoleSpec struct {
	Key, Name string
	Segments  []sdkauthz.Segment
	To        []string
}

// GrantRoles defines each role and grants it in the root unit to each of its accounts (logged in already); it answers
// the role revisions by key.
func (a *Authz) GrantRoles(roles []RoleSpec, accounts map[string]Account) (map[string]string, error) {
	out := map[string]string{}
	for _, r := range roles {
		rev, err := a.Role(r.Key, r.Name, r.Segments)
		if err != nil {
			return nil, err
		}
		out[r.Key] = rev
		for _, u := range r.To {
			acc, ok := accounts[u]
			if !ok {
				return nil, fmt.Errorf("role %s: account %s is not logged in", r.Key, u)
			}
			if _, err := a.GrantRole(r.Key, "", acc.Principal, rev); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func (a *Authz) unitOr(unit string) string {
	if unit == "" {
		return a.root
	}
	return unit
}
