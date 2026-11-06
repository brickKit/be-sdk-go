package tiering

import (
	"sort"
	"time"
)

// The planning function (be-protocol P15.37): what each unit is to do next and what holds it back. The same function
// in every SDK, checked by the vectors tier-plan; the transfer job asks it for every unit it considers.

// Actions.
const (
	ActNone     = "none"
	ActSeal     = "seal"
	ActTransfer = "transfer"
	ActRelease  = "release"
	ActDestroy  = "destroy"
)

// Reasons a unit is held back (besides a class's own blockers, which are the module's).
const (
	WhyNotTierable    = "not_tierable"
	WhyResidenceBelow = "residence_below_n"
	WhyNever          = "never"
	WhyResidence      = "residence"
	WhyImmutableRoute = "immutable_route"
	WhyGrace          = "grace"
	WhyRecentRecords  = "recent_records"
	WhyLease          = "lease"
	WhyNoBackup       = "no_backup"
	WhyPending        = "retention_pending"
	WhyHeld           = "held"
	WhyApproval       = "approval"
)

// BlockedAlertAfter is how long past its residence a unit may stay blocked before an alert (protocol 11: 30 days).
const BlockedAlertAfter = 30 * 24 * time.Hour

// PlanClass is what the plan needs of a class.
type PlanClass struct {
	Category         string
	ResidenceDays    int // Never for "never"
	RetentionPending bool
	Erasable         bool
}

// Never is a residence that keeps a class online.
const Never = -1

// PlanUnit is what the plan needs of a unit.
type PlanUnit struct {
	State              string
	AnchorMax          time.Time // open: the latest anchor among the candidates
	Blockers           []string
	RouteImmutable     bool
	PublishedAt        time.Time
	NewestCreated      time.Time
	OldLeases          int
	BackupAfterPublish bool
	RetentionDue       bool
	Approved           bool
	Holds              int
}

// Decision is a unit's action and the reasons it is held back.
type Decision struct {
	Action  string
	Reasons []string
	Alert   bool // blocked for more than BlockedAlertAfter past its residence
}

// Plan decides one unit's next action at now (P15.37), with N (the list's default range) and the grace after a
// publish.
func Plan(now time.Time, n, grace time.Duration, c PlanClass, u PlanUnit) Decision {
	if c.Category != CategoryCompleted && c.Category != CategoryAudit {
		return Decision{Action: ActNone, Reasons: []string{WhyNotTierable}}
	}
	residence := time.Duration(c.ResidenceDays) * 24 * time.Hour
	if c.ResidenceDays != Never && residence < n {
		return Decision{Action: ActNone, Reasons: []string{WhyResidenceBelow}}
	}
	if u.State == StateDestroyed {
		return Decision{Action: ActNone, Reasons: []string{}}
	}
	if u.State != StateOpen && u.RetentionDue {
		var why []string
		if c.RetentionPending {
			why = append(why, WhyPending)
		}
		if u.Holds > 0 {
			why = append(why, WhyHeld)
		}
		if !u.Approved {
			why = append(why, WhyApproval)
		}
		return held(ActDestroy, why)
	}
	switch u.State {
	case StateOpen:
		if c.ResidenceDays == Never {
			return Decision{Action: ActNone, Reasons: []string{WhyNever}}
		}
		due := u.AnchorMax.Add(residence)
		if due.After(now) {
			return Decision{Action: ActNone, Reasons: []string{WhyResidence}}
		}
		if len(u.Blockers) > 0 {
			d := held(ActSeal, append([]string(nil), u.Blockers...))
			d.Alert = !now.Before(due.Add(BlockedAlertAfter))
			return d
		}
		return Decision{Action: ActSeal, Reasons: []string{}}
	case StateSealed:
		if u.RouteImmutable && c.Erasable {
			return Decision{Action: ActNone, Reasons: []string{WhyImmutableRoute}}
		}
		return Decision{Action: ActTransfer, Reasons: []string{}}
	case StatePublished:
		var why []string
		if u.PublishedAt.Add(grace).After(now) {
			why = append(why, WhyGrace)
		}
		if u.NewestCreated.After(now.Add(-n)) {
			why = append(why, WhyRecentRecords)
		}
		if u.OldLeases > 0 {
			why = append(why, WhyLease)
		}
		if !u.BackupAfterPublish {
			why = append(why, WhyNoBackup)
		}
		return held(ActRelease, why)
	}
	return Decision{Action: ActNone, Reasons: []string{}} // released
}

// held is the action when nothing holds it back, else none with the reasons sorted.
func held(action string, why []string) Decision {
	if len(why) == 0 {
		return Decision{Action: action, Reasons: []string{}}
	}
	sort.Strings(why)
	return Decision{Action: ActNone, Reasons: why}
}
