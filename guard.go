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
//	no bundle yet → 503 AUTHZ_NOT_READY for every non-Public route, Authenticated included
//	  (P1.5, stage-B ruling: fail closed, after the token check)
//	token checks E2 (stale, revoked grant, delegation capabilities) → 401
//	Authenticated → allow
//	key not held (E3–E5, ceilings) → 403 MISSING_PERMISSION {permission}
//	allow, with Access in the request context
type authGuardian struct {
	verifier verifier
	bundles  bundleSource
	now      func() time.Time
	denied   func(reason string)
	rt       *Runtime // scopes and record decisions read its catalogue and store
}

func (g *authGuardian) check(c *gin.Context, gd Guard) *problem.Error {
	key := guardKey(gd)
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
	if b == nil {
		return g.deny(problem.Be("AUTHZ_NOT_READY", nil))
	}
	tok := tokenFromClaims(claims)
	if reason := authz.CheckToken(b, tok); reason != "" {
		return g.deny(problem.Be(reason, nil))
	}
	if key != Authenticated {
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
	a := &Access{user: userFromClaims(claims), token: tok, bundle: b, key: key, now: g.now, rt: g.rt}
	if rg, ok := gd.(ResourceGuard); ok {
		a.typ = rg.Type
		if e := g.decideRecord(c, a, rg); e != nil {
			return g.deny(e)
		}
	}
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
