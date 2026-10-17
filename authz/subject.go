package authz

import (
	"context"
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
	g, err := s.Holder.Lookup(r.Type, s.Principal, risk)
	if err != nil {
		return ctx, nil, nil, Grant{}, err
	}
	return reqctx.WithCutoff(ctx, g.Cutoff), o, r, g, nil
}

type errUndeclared string

func (e errUndeclared) Error() string { return "operation " + string(e) + " is not declared" }

// Check decides one record for an operation (P8.15, P8.16): not visible answers NOT_FOUND, visible without the
// operation PERMISSION_DENIED, unknown AUTHORIZATION_UNPROVABLE. The returned context carries the cutoff that a
// write transaction checks before COMMIT.
func (s *Subject) Check(ctx context.Context, op string, facts Facts) (context.Context, Decision, error) {
	ctx, o, r, g, err := s.grant(ctx, op)
	if err != nil {
		return ctx, Decision{}, err
	}
	d := Evaluate(r, g.Segments, o.Key, facts, s.Principal, s.At)
	switch {
	case d.Visible == False:
		return ctx, d, errs.New(errs.ReasonNotFound, "not found")
	case d.Visible == Unknown, d.Action == Unknown:
		return ctx, d, errs.New(errs.ReasonAuthorizationUnprovable, "the authorization cannot be proven right now")
	case d.Action == False:
		return ctx, d, errs.New(errs.ReasonPermissionDenied, "not allowed")
	}
	return ctx, d, nil
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

// Mask keeps only the readable fields of an output object; fields that are not declared never pass.
func Mask(out map[string]any, readable []string) map[string]any {
	keep := map[string]bool{}
	for _, f := range readable {
		keep[f] = true
	}
	masked := map[string]any{}
	for k, v := range out {
		if keep[k] {
			masked[k] = v
		}
	}
	return masked
}
