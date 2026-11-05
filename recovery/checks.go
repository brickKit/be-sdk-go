// Package recovery answers a full recovery's checks (be-protocol chapter 14; the assembly project's protocol 15, steps
// 8-10) over the standard system method be.recovery.v1.CheckService: the consistency checks (the SDK's own and the
// component's), which objects a lost change touched, and the continuous document number series. Only the recovery
// tool calls it, in the verifying phase.
package recovery

import (
	"context"
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// The outcomes of a check (P14.1).
const (
	Pass       = "pass"
	Diff       = "diff"
	Unprovable = "unprovable"
)

// The answers of a presence question (P14.3).
const (
	Present = "present" // in the database, at the version asked or later
	Older   = "older"   // in the database at a lower version: that change was lost with the recovery
	Absent  = "absent"
)

// Difference is one object a check found out of line.
type Difference struct {
	ObjectType, ObjectID, Detail string
}

// Result is a check's answer. A check is read-only and gives the same answer in the same state.
type Result struct {
	Outcome     string
	Differences []Difference
	Disposal    string // what to do about the differences
}

// Check is one consistency check: its identity, what it rests on (the assembly project's protocol and item) and how
// it runs, in a transaction the server rolls back. A check across components is reported by one side only (P14.2).
type Check struct {
	Name  string
	Basis string
	Run   func(ctx context.Context, tx db.Tx) (Result, error)
}

// PresenceFunc answers, for ids of one object type, each present object's revision; an id not in the map is absent.
type PresenceFunc func(ctx context.Context, tx db.Tx, ids []string) (map[string]int64, error)

// VoidFunc registers as void the numbers from..to of a continuous series raised over them (P14.5), in the raising
// transaction.
type VoidFunc func(ctx context.Context, tx db.Tx, s db.Series, from, to int64) error

// Checks is what a component registers.
type Checks struct {
	checks   []Check
	presence map[string]PresenceFunc
	void     VoidFunc
}

// Add registers a consistency check; names are unique.
func (c *Checks) Add(ch Check) error {
	if ch.Name == "" || ch.Run == nil {
		return fmt.Errorf("a check needs a name and Run")
	}
	for _, have := range c.checks {
		if have.Name == ch.Name {
			return fmt.Errorf("check %s registered twice", ch.Name)
		}
	}
	c.checks = append(c.checks, ch)
	return nil
}

// Presence registers the presence question of an object type (a resource of the component's definitions): every
// type the component audits business changes or sensitive reads of.
func (c *Checks) Presence(objectType string, f PresenceFunc) {
	if c.presence == nil {
		c.presence = map[string]PresenceFunc{}
	}
	c.presence[objectType] = f
}

// Void registers how the component voids the numbers a raise skips over; required when it has continuous series.
func (c *Checks) Void(f VoidFunc) { c.void = f }

// Checker is a module that registers recovery checks.
type Checker interface {
	RecoveryChecks(c *Checks) error
}
