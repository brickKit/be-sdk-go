package besdk

import (
	"context"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/internal/authn"
	"github.com/brickKit/be-sdk-go/internal/authz"
	"github.com/brickKit/be-sdk-go/internal/httpx"
	"github.com/brickKit/be-sdk-go/internal/logx"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/gin-gonic/gin"
)

// verifier and bundleSource are what the guard chain needs from internal/authn and internal/authz.
type verifier interface {
	Verify(ctx context.Context, authorization string) (*authn.Claims, error)
}
type bundleSource interface{ Current() *authz.Bundle }

// authGuardian is the route decision chain of P6.2, keys only (levels, dimensions and resource
// guards are the data-scope wave):
//
//	Public → allow
//	verify the token (P5) → 401 TOKEN_INVALID
//	bundle loaded: token checks E2 (stale, revoked grant, delegation capabilities) → 401
//	Authenticated → allow
//	no bundle yet → 503 AUTHZ_NOT_READY (P1.5)
//	key not held (E3–E5, ceilings) → 403 MISSING_PERMISSION {permission}
//	allow, with Access in the request context
type authGuardian struct {
	verifier verifier
	bundles  bundleSource
	now      func() time.Time
	denied   func(reason string)
}

func (g *authGuardian) check(c *gin.Context, gd Guard) *problem.Error {
	key, _ := gd.(PermKey)
	if key == Public {
		return nil
	}
	ctx := c.Request.Context()
	header := c.GetHeader("Authorization")
	claims, err := g.verifier.Verify(ctx, header)
	if err != nil {
		return g.deny(problem.Wrap(err, "TOKEN_INVALID", nil))
	}
	b := g.bundles.Current()
	tok := tokenFromClaims(claims)
	if b != nil {
		if reason := authz.CheckToken(b, tok); reason != "" {
			return g.deny(problem.Be(reason, nil))
		}
	}
	if key != Authenticated {
		if b == nil {
			return g.deny(problem.Be("AUTHZ_NOT_READY", nil))
		}
		if d := authz.Decide(b, tok, string(key), g.now()); !d.Allow {
			meta := map[string]string(nil)
			if d.Reason == authz.ReasonMissingPermission {
				meta = map[string]string{"permission": string(key)}
			}
			return g.deny(problem.Be(d.Reason, meta))
		}
	}
	perm := string(key)
	if key == Authenticated {
		perm = ""
	}
	a := &Access{user: userFromClaims(claims), token: tok, bundle: b, key: key, now: g.now}
	ctx = context.WithValue(ctx, accessKey{}, a)
	ctx = context.WithValue(ctx, rawTokenKey{}, header)
	attrs := []slog.Attr{slog.String("sub", claims.Sub)}
	if perm != "" {
		attrs = append(attrs, slog.String("perm", perm))
	}
	if j := actJSON(a.user.Act); j != "" {
		attrs = append(attrs, slog.String("act", j))
	}
	httpx.AddAccessAttrs(ctx, attrs...)
	c.Request = c.Request.WithContext(logx.WithFields(ctx, attrs...))
	return nil
}

func (g *authGuardian) deny(e *problem.Error) *problem.Error {
	if g.denied != nil {
		g.denied(e.Reason)
	}
	return e
}
