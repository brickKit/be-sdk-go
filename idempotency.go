package besdk

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/brickKit/be-sdk-go/internal/idem"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/brickKit/be-sdk-go/internal/rpc"
	"github.com/gin-gonic/gin"
)

// Command is one idempotent write (P13.2): the key, the command it is bound to (the permission key or
// the rpc's full name), the target aggregate ("" for a create), the business fields that make up its
// fingerprint (RFC 8785, never the key itself), and the HTTP status a success answers with (0 = 200),
// which a replay answers with too.
type Command struct {
	Key     string
	Name    string
	Target  string
	Request any
	Status  int
}

// Prior is what an earlier use of the key left: nothing, a claim in progress, or a stored result.
type Prior struct {
	Found, InProgress bool
	Result            json.RawMessage
	Status            int
}

// Errors of an idempotent command (P13.2, P13.3): a key reused for another command, target or body
// (400 IDEMPOTENCY_MISMATCH), and a key still claimed by a two-step command (409
// IDEMPOTENCY_IN_PROGRESS). errors.Is works on them.
var (
	ErrIdempotencyMismatch   error = idem.ErrMismatch
	ErrIdempotencyInProgress error = idem.ErrInProgress
)

// CallerOf is the caller namespace of ctx (P13.1): "user:<sub>" for a user request, "svc:<be-caller>"
// for a system call, "system" for the component's own background work (jobs, event handlers), "" for
// anything else (a Public request has no caller and no idempotency).
func CallerOf(ctx context.Context) string {
	if a, err := AccessFrom(ctx); err == nil && a.user.Sub != "" {
		return "user:" + a.user.Sub
	}
	if c, ok := rpc.CallerFrom(ctx); ok && c.Caller != "" {
		return "svc:" + c.Caller
	}
	if ctx.Value(handlingKey{}) != nil {
		return idem.SystemNamespace
	}
	return ""
}

// idemCommand binds c to the caller of ctx and fingerprints its request (P13.1, P13.2).
func idemCommand(ctx context.Context, c Command) (idem.Command, error) {
	caller := CallerOf(ctx)
	if caller == "" {
		return idem.Command{}, problem.Wrap(errNoCaller, "INTERNAL", nil)
	}
	req := c.Request
	if req == nil {
		req = struct{}{}
	}
	hash, err := idem.FingerprintValue(req)
	if err != nil {
		return idem.Command{}, problem.Wrap(err, "INTERNAL", nil)
	}
	return idem.Command{Caller: caller, Key: c.Key, Name: c.Name, Target: c.Target, Hash: hash, Status: c.Status}, nil
}

var errNoCaller = errors.New("besdk: an idempotent command needs a caller: a user, a system call or background work (P13.1)")

// Idempotent runs a one-step command once per caller and key (P13.3, P13.6): it claims the key, runs
// do and stores the result, all in tx; a repeat returns the stored result without calling do
// (replayed = true). Call it after the argument checks and the authorization of the target, before
// the state machine (P13.5). An empty key runs do without idempotency.
func Idempotent[T any](ctx context.Context, tx *Tx, c Command, do func() (T, error)) (res T, replayed bool, err error) {
	if c.Key == "" {
		res, err = do()
		return res, false, err
	}
	ic, err := idemCommand(ctx, c)
	if err != nil {
		return res, false, err
	}
	res, _, replayed, err = idem.Idempotent(ctx, tx.Tx, ic, tx.rt.Now(), do)
	return res, replayed, err
}

// IdemLookup reports what an earlier use of the key left, without claiming (P13.9 GetStatus).
func (tx *Tx) IdemLookup(ctx context.Context, c Command) (Prior, error) {
	ic, err := idemCommand(ctx, c)
	if err != nil {
		return Prior{}, err
	}
	p, err := idem.Lookup(ctx, tx.Tx, ic, tx.rt.Now())
	return priorOf(p), err
}

// IdemClaim claims the key for a two-step command (claim, a network call, then IdemComplete in
// another transaction); the claim must be committed before the call. A completed key returns its
// result in Prior; a claim in progress is ErrIdempotencyInProgress.
func (tx *Tx) IdemClaim(ctx context.Context, c Command) (Prior, error) {
	ic, err := idemCommand(ctx, c)
	if err != nil {
		return Prior{}, err
	}
	p, err := idem.Claim(ctx, tx.Tx, ic, tx.rt.Now())
	return priorOf(p), err
}

// IdemComplete stores the result of a claimed command (P13.3).
func (tx *Tx) IdemComplete(ctx context.Context, c Command, result any) error {
	ic, err := idemCommand(ctx, c)
	if err != nil {
		return err
	}
	body, err := idem.EncodeBody(result)
	if err != nil {
		return problem.Wrap(err, "INTERNAL", nil)
	}
	status := c.Status
	if status == 0 {
		status = 200
	}
	return idem.Complete(ctx, tx.Tx, ic, idem.Result{HTTP: status, Body: body}, tx.rt.Now())
}

// IdemRelease drops a claim after a step that failed for certain, so the key may be retried (P13.3).
func (tx *Tx) IdemRelease(ctx context.Context, c Command) error {
	ic, err := idemCommand(ctx, c)
	if err != nil {
		return err
	}
	return idem.Release(ctx, tx.Tx, ic)
}

func priorOf(p idem.Prior) Prior {
	return Prior{Found: p.Found, InProgress: p.InProgress, Result: p.Result.Body, Status: p.Result.HTTP}
}

// IdempotencyKey resolves a REST request's key (P3.7): the Idempotency-Key header and the body field
// idempotency_key (pass "" when the body has none); both present and different is 400
// IDEMPOTENCY_MISMATCH; "" means no idempotency.
func IdempotencyKey(c *gin.Context, bodyKey string) (string, error) {
	return idem.ResolveKey(c.GetHeader("Idempotency-Key"), bodyKey)
}
