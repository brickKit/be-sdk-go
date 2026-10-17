// Package guard admits user-surface requests (protocol P8.1-P8.4, P8.12-P8.13): the bearer credential is
// verified, the route's operation must be declared, a valid authorization view must exist for it, and the request
// carries the verified user, its authorization subject and its cutoff.
package guard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/clock"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/httpx"
	"github.com/brickKit/be-sdk-go/v3/identity"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
)

// User admits requests made with a user's own credential.
type User struct {
	Verifier *identity.Verifier
	Defs     *authz.Definitions
	Holder   *authz.Holder
	Log      *slog.Logger
	Boot     func() time.Duration
	Now      func() time.Time
}

// Admit implements httpx.Guard.
func (u *User) Admit(ctx context.Context, r httpx.Route, req *http.Request) (context.Context, error) {
	boot, now := clock.Boot, time.Now
	if u.Boot != nil {
		boot = u.Boot
	}
	if u.Now != nil {
		now = u.Now
	}
	auth := req.Header.Get("Authorization")
	token, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok || token == "" {
		return ctx, errs.New(errs.ReasonTokenInvalid, "the credential is missing or not valid")
	}
	user, err := u.Verifier.Verify(token)
	if err != nil {
		var te *identity.TokenError
		if errors.As(err, &te) {
			u.Log.Info("a credential was refused", "problem", string(te.Problem)) // never the credential itself
		}
		return ctx, identity.Refusal(err)
	}
	op, ok := u.Defs.Operation(r.Operation)
	if !ok {
		return ctx, errs.From(fmt.Errorf("route operation %s is not declared", r.Operation))
	}
	res, _ := u.Defs.Resource(op.ResourceType)
	g, err := u.Holder.Lookup(res.Type, user.Subject, op.RiskClass)
	if err != nil {
		return ctx, err
	}
	ctx = reqctx.WithCutoff(ctx, boot()+user.Expiry.Sub(now()))
	ctx = reqctx.WithCutoff(ctx, g.Cutoff)
	ctx = identity.WithUser(ctx, user)
	return authz.WithSubject(ctx, &authz.Subject{Principal: user.Subject, Defs: u.Defs, Holder: u.Holder, At: now()}), nil
}

// CheckOperations refuses routes and methods whose operations are not declared (P8.7).
func CheckOperations(defs *authz.Definitions, ops []string) error {
	var missing []string
	for _, op := range ops {
		if _, ok := defs.Operation(op); !ok {
			missing = append(missing, op)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("operations used by routes or methods but not declared: %s", strings.Join(missing, ", "))
	}
	return nil
}
