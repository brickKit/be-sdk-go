package tiering

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/authz"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
)

// Holds and destruction approvals (be-protocol P15.28, P15.29): records of their own, orthogonal to a unit's state;
// a hold stops destruction and an erasure that loses content, an approval lets a due destruction go. Setting either
// needs the class's declared operation, checked here (the irreversible step has one entrance). The operations are on
// a resource the subject holds without record facts (no dimension, or unrestricted segments).

// Scope kinds of a hold or an approval.
const (
	ScopeUnit   = "unit"
	ScopeClass  = "class"
	ScopeRecord = "record" // holds only: "<class>/<root id>"
)

// Hold sources (P15.28).
var HoldSources = []string{"legal", "audit", "dispute"}

// Scope is what a hold or an approval covers.
type Scope struct {
	Kind, Value string // Value: a unit id, a class, or "<class>/<root id>"
}

// RecordScope is the scope of one aggregate.
func RecordScope(class, root string) Scope {
	return Scope{Kind: ScopeRecord, Value: class + "/" + root}
}

// allowed checks the subject's operation for a class (a decision without record facts).
func allowed(ctx context.Context, sub *authz.Subject, op string) error {
	if sub == nil {
		return errs.New(errs.ReasonPermissionDenied, "no authorized subject")
	}
	_, _, err := sub.Check(ctx, op, authz.Facts{})
	return err
}

// scopeOf checks that a scope names something of the class.
func (c *Catalog) scopeOf(ctx context.Context, tx db.Tx, class string, s Scope, record bool) error {
	switch s.Kind {
	case ScopeClass:
		if s.Value != class {
			return errs.New(errs.ReasonRequestInvalid, "the class scope names another class")
		}
	case ScopeUnit:
		var of string
		if err := tx.QueryRow(ctx, "SELECT class FROM be_tier_unit WHERE id::text = $1", s.Value).Scan(&of); err != nil || of != class {
			return errs.New(errs.ReasonNotFound, "no such unit of "+class)
		}
	case ScopeRecord:
		if !record || !strings.HasPrefix(s.Value, class+"/") || len(s.Value) == len(class)+1 {
			return errs.New(errs.ReasonRequestInvalid, "a record scope is <class>/<root id>, for holds only")
		}
	default:
		return errs.New(errs.ReasonRequestInvalid, "unknown scope kind "+s.Kind)
	}
	return nil
}

func (c *Catalog) declared(class string) (Class, error) {
	cl, ok := c.Classes[class]
	if !ok {
		return Class{}, errs.New(errs.ReasonNotFound, "no class "+class)
	}
	return cl, nil
}

// Hold sets a hold on a scope of a class (P15.28); it answers the hold's id.
func (c *Catalog) Hold(ctx context.Context, sub *authz.Subject, class string, s Scope, source, reason string) (string, error) {
	cl, err := c.declared(class)
	if err != nil {
		return "", err
	}
	if err := allowed(ctx, sub, cl.HoldOperation); err != nil {
		return "", err
	}
	if !slices.Contains(HoldSources, source) || reason == "" {
		return "", errs.New(errs.ReasonRequestInvalid, "a hold needs its source (legal, audit, dispute) and its reason")
	}
	id := ids.New().String()
	return id, c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := c.scopeOf(ctx, tx, class, s, true); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO be_tier_hold (id, scope_kind, scope, source, authorized_by, reason) "+
			"VALUES ($1, $2, $3, $4, $5, $6)", id, s.Kind, s.Value, source, sub.Principal, reason)
		return err
	})
}

// LiftHold lifts a live hold (P15.28).
func (c *Catalog) LiftHold(ctx context.Context, sub *authz.Subject, class, id string) error {
	cl, err := c.declared(class)
	if err != nil {
		return err
	}
	if err := allowed(ctx, sub, cl.HoldOperation); err != nil {
		return err
	}
	return c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE be_tier_hold SET released_at = now(), released_by = $3 WHERE id::text = $1 "+
			"AND released_at IS NULL AND (scope = $2 OR (scope_kind = 'unit' AND scope IN (SELECT id::text FROM be_tier_unit "+
			"WHERE class = $2)) OR (scope_kind = 'record' AND scope LIKE $2 || '/%'))", id, class, sub.Principal)
		if err == nil && tag.RowsAffected() == 0 {
			return errs.New(errs.ReasonNotFound, "no live hold "+id+" of "+class)
		}
		return err
	})
}

// Approve gives a destruction approval of a unit, or a standing one of the class (P15.29); it answers its id.
func (c *Catalog) Approve(ctx context.Context, sub *authz.Subject, class string, s Scope, reason string) (string, error) {
	cl, err := c.declared(class)
	if err != nil {
		return "", err
	}
	if err := allowed(ctx, sub, cl.ApproveOperation); err != nil {
		return "", err
	}
	if reason == "" {
		return "", errs.New(errs.ReasonRequestInvalid, "an approval needs its reason")
	}
	id := ids.New().String()
	return id, c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := c.scopeOf(ctx, tx, class, s, false); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "INSERT INTO be_tier_approval (id, scope_kind, scope, approved_by, reason) VALUES ($1, $2, $3, $4, $5)",
			id, s.Kind, s.Value, sub.Principal, reason)
		return err
	})
}

// Withdraw withdraws a live approval (P15.29).
func (c *Catalog) Withdraw(ctx context.Context, sub *authz.Subject, class, id string) error {
	cl, err := c.declared(class)
	if err != nil {
		return err
	}
	if err := allowed(ctx, sub, cl.ApproveOperation); err != nil {
		return err
	}
	return c.DB.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE be_tier_approval SET withdrawn_at = now(), withdrawn_by = $3 WHERE id::text = $1 "+
			"AND withdrawn_at IS NULL AND (scope = $2 OR (scope_kind = 'unit' AND scope IN (SELECT id::text FROM be_tier_unit "+
			"WHERE class = $2)))", id, class, sub.Principal)
		if err == nil && tag.RowsAffected() == 0 {
			return errs.New(errs.ReasonNotFound, "no live approval "+id+" of "+class)
		}
		return err
	})
}

// holdsOn counts the live holds covering a unit: on it, on its class, or on any of its records.
func holdsOn(ctx context.Context, tx db.Tx, unit, class string) (int, error) {
	var n int
	err := tx.QueryRow(ctx, "SELECT count(*) FROM be_tier_hold h WHERE h.released_at IS NULL AND ("+
		"(h.scope_kind = 'unit' AND h.scope = $1) OR (h.scope_kind = 'class' AND h.scope = $2) OR "+
		"(h.scope_kind = 'record' AND h.scope IN (SELECT $2 || '/' || m.root_id FROM be_tier_member m WHERE m.unit::text = $1)))",
		unit, class).Scan(&n)
	return n, err
}

// approvedFor says whether a live approval covers a unit: its own, or its class's standing one.
func approvedFor(ctx context.Context, tx db.Tx, unit, class string) (bool, error) {
	var ok bool
	err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM be_tier_approval WHERE withdrawn_at IS NULL AND "+
		"((scope_kind = 'unit' AND scope = $1) OR (scope_kind = 'class' AND scope = $2)))", unit, class).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("tiering: the approvals of %s: %w", unit, err)
	}
	return ok, nil
}
