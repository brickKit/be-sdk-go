package authz

import (
	"context"
	"sort"
	"time"

	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/reqctx"
)

// Subject is the authorization of one verified request: who asks, the member's declarations and view. The user
// surface puts it into the request context; handlers use it to decide records and compile lists.
type Subject struct {
	Principal string
	Defs      *Definitions
	Holder    *Holder
	At        time.Time
}

type subjectKey struct{}

// WithSubject returns ctx carrying the subject.
func WithSubject(ctx context.Context, s *Subject) context.Context {
	return context.WithValue(ctx, subjectKey{}, s)
}

// From returns the request's subject.
func From(ctx context.Context) (*Subject, bool) {
	s, ok := ctx.Value(subjectKey{}).(*Subject)
	return s, ok
}

func (s *Subject) grant(ctx context.Context, op string) (context.Context, *Operation, *Resource, Grant, error) {
	o, ok := s.Defs.Operation(op)
	if !ok {
		return ctx, nil, nil, Grant{}, errs.From(errUndeclared(op))
	}
	r, _ := s.Defs.Resource(o.ResourceType)
	risk := o.RiskClass
	if v, ok := s.Defs.Operation(r.ViewOperation); ok && v.RiskClass == "high" {
		risk = "high"
	}
	g, err := s.Holder.LookupFor(r.Type, s.Principal, risk, o.TighteningOnly)
	if err != nil {
		return ctx, nil, nil, Grant{}, err
	}
	if g.Stale {
		ctx = withStaleBasis(ctx, g.Since)
	}
	return reqctx.WithCutoff(ctx, g.Cutoff), o, r, g, nil
}

type errUndeclared string

func (e errUndeclared) Error() string { return "operation " + string(e) + " is not declared" }

// Check decides one record for an operation; the error is the decision's Err. The returned context carries the cutoff that a
// write transaction checks before COMMIT.
func (s *Subject) Check(ctx context.Context, op string, facts Facts) (context.Context, Decision, error) {
	ctx, o, r, g, err := s.grant(ctx, op)
	if err != nil {
		return ctx, Decision{}, err
	}
	d := Evaluate(r, g.Segments, o.Key, facts, s.Principal, s.At)
	return ctx, d, d.Err()
}

// Err is the answer for a decision (P8.15, P8.16): not visible NOT_FOUND; the operation definitely not allowed
// PERMISSION_DENIED, even when visibility is unknown (a known mismatch is denied by the operation's rule, security
// 01); otherwise unknown AUTHORIZATION_UNPROVABLE; nil when allowed.
func (d Decision) Err() error {
	switch {
	case d.Visible == False:
		return errs.New(errs.ReasonNotFound, "not found")
	case d.Action == False:
		return errs.New(errs.ReasonPermissionDenied, "not allowed")
	case d.Visible == Unknown, d.Action == Unknown:
		return errs.New(errs.ReasonAuthorizationUnprovable, "the authorization cannot be proven right now")
	}
	return nil
}

// List compiles the subject's list authorization for an operation over a table alias (P8.17); AND its Where
// with the request's own filter and select its Hits to get each row's fields.
func (s *Subject) List(ctx context.Context, op, alias string, argBase int) (context.Context, *Plan, error) {
	ctx, o, r, g, err := s.grant(ctx, op)
	if err != nil {
		return ctx, nil, err
	}
	p, err := Compile(r, g.Segments, o.Key, s.Principal, s.At, alias, argBase)
	return ctx, p, err
}

// Masked is the key that names an output object's masked fields (P8.31).
const Masked = "_masked"

// Mask shapes an output object for the caller (P8.31): readable fields as they are; the resource's other ordinary
// and restricted fields null and named, sorted, under _masked (only when there is one), so a caller tells "no value"
// from "not yours to see"; secret and undeclared keys never pass.
func Mask(r *Resource, out map[string]any, readable []string) map[string]any {
	keep := map[string]bool{}
	for _, f := range readable {
		keep[f] = true
	}
	shaped := map[string]any{}
	var masked []string
	for k, v := range out {
		fd, declared := r.Fields[k]
		switch {
		case !declared || fd.Class == Secret:
		case keep[k]:
			shaped[k] = v
		default:
			shaped[k] = nil
			masked = append(masked, k)
		}
	}
	if len(masked) > 0 {
		sort.Strings(masked)
		shaped[Masked] = masked
	}
	return shaped
}

type staleKey struct{}

func withStaleBasis(ctx context.Context, since time.Time) context.Context {
	return context.WithValue(ctx, staleKey{}, since)
}

// StaleBasis reports a decision made on a view whose source the authorization member said is stale (P8.29): a
// tightening_only operation admitted on the shard's last view, and that view's last full renewal. The doer marks its
// audit "basis not proven latest" and applies security 03's table (one person within T_tight, two beyond).
func StaleBasis(ctx context.Context) (since time.Time, stale bool) {
	since, stale = ctx.Value(staleKey{}).(time.Time)
	return since, stale
}
