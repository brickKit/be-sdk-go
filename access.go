package besdk

import (
	"context"
	"encoding/json"
	"regexp"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authn"
	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/problem"
)

// User is the verified caller of a user-plane request (P5.5). The raw token is never part of it (P5.8).
type User struct {
	Sub      string
	TenantID string
	Roles    []string
	DeptPath string
	HasDept  bool // dept_path is present and of the form /<seg>/…/ (P6.4: empty is never the root)
	Act      *Act
	Locale   string
}

// Act is one link of the token's act chain (RFC 8693).
type Act struct {
	Sub, Kind string
	Act       *Act
}

// Access is the evaluated access of the current request: the one entry point of authorization in
// component code (sdk-redesign-apis §2.5). This wave offers the user and feature keys; scopes, Can,
// masks and row actions come with the data-scope wave.
type Access struct {
	user   User
	token  authz.Token
	bundle *authz.Bundle
	key    PermKey
	now    func() time.Time
	rt     *Runtime         // the catalogue and the store, for scopes and record decisions
	eval   *authz.Evaluator // built on first use, for this request only (P6.14)
}

type accessKey struct{}
type rawTokenKey struct{}

// ErrUnauthenticated is returned where a user is required and the context has none.
var ErrUnauthenticated error = problem.Be("TOKEN_INVALID", nil)

// AccessFrom returns the request's evaluated access; without a user (an event handler, a job, a
// system call) it fails with UNAUTHENTICATED.
func AccessFrom(ctx context.Context) (*Access, error) {
	a, ok := ctx.Value(accessKey{}).(*Access)
	if !ok {
		return nil, ErrUnauthenticated
	}
	return a, nil
}

// User returns the caller.
func (a *Access) User() User { return a.user }

// Key returns the permission key the route was decided with ("" for Authenticated routes).
func (a *Access) Key() PermKey { return a.key }

// Has reports whether the caller holds a feature key, with ceilings applied (E4, E5).
func (a *Access) Has(k PermKey) bool {
	return a.bundle != nil && authz.HasKey(a.bundle, a.token, string(k), a.now())
}

var deptPathForm = regexp.MustCompile(`^/([^/]+/)*$`)

func userFromClaims(c *authn.Claims) User {
	u := User{Sub: c.Sub, TenantID: c.TenantID, Roles: c.Roles, DeptPath: c.DeptPath, Locale: c.Locale,
		Act: actFrom(c.Act)}
	u.HasDept = c.DeptPath != "" && deptPathForm.MatchString(c.DeptPath)
	return u
}

func actFrom(a *authn.Act) *Act {
	if a == nil {
		return nil
	}
	return &Act{Sub: a.Sub, Kind: a.Kind, Act: actFrom(a.Act)}
}

func tokenFromClaims(c *authn.Claims) authz.Token {
	return authz.Token{Sub: c.Sub, IssuedAt: c.IssuedAt, Roles: c.Roles, DeptPath: c.DeptPath, Act: authzAct(c.Act),
		Ceil: c.Ceil, DG: c.DG}
}

func authzAct(a *authn.Act) *authz.Act {
	if a == nil {
		return nil
	}
	return &authz.Act{Sub: a.Sub, Kind: a.Kind, Act: authzAct(a.Act)}
}

// actJSON renders the act chain for be-actor-act (P7.2), "" when absent.
func actJSON(a *Act) string {
	if a == nil {
		return ""
	}
	type wire struct {
		Sub  string `json:"sub"`
		Kind string `json:"kind"`
		Act  *wire  `json:"act,omitempty"`
	}
	var conv func(*Act) *wire
	conv = func(x *Act) *wire {
		if x == nil {
			return nil
		}
		return &wire{Sub: x.Sub, Kind: x.Kind, Act: conv(x.Act)}
	}
	b, _ := json.Marshal(conv(a))
	return string(b)
}

// rawToken is the caller's Authorization header, kept only in a private context slot for user-plane
// forwarding (P5.8, P8.1).
func rawToken(ctx context.Context) string {
	s, _ := ctx.Value(rawTokenKey{}).(string)
	return s
}
