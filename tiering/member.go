package tiering

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/work"
)

// A member's tiering: its configuration keys, its catalog and the candidates job (be-protocol P15.3, P15.11).

// Configuration keys.
const (
	KeyWindow   = "TIERING_WINDOW"    // the candidates job's cron schedule: the declared low-peak hours
	KeyListDays = "TIERING_LIST_DAYS" // N, the lists' default range in days (protocol 11: 90)
	KeyGrace    = "TIERING_GRACE"     // the grace after a publish before the online rows go (protocol 11: 7 days)
)

// ResidenceKey is the key that overrides a class's residence: days, or "never".
func ResidenceKey(class string) string {
	return "TIERING_RESIDENCE_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(class))
}

// RetentionKey is a class's retention given at deployment ("<shortest days>/<longest days>/<basis>", P15.4): the
// retention depends on the class, the legal entity's region and the customer, set at go-live.
func RetentionKey(class string) string {
	return "TIERING_RETENTION_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(class))
}

// retentionOf parses a retention given at deployment.
func retentionOf(raw string) (Retention, error) {
	parts := strings.SplitN(raw, "/", 3)
	if len(parts) != 3 {
		return Retention{}, fmt.Errorf("%q is not <shortest days>/<longest days>/<basis>", raw)
	}
	lo, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	hi, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	basis := strings.TrimSpace(parts[2])
	if err1 != nil || err2 != nil || lo <= 0 || hi < lo || basis == "" {
		return Retention{}, fmt.Errorf("%q: the shortest days above 0, the longest not below it, and a basis", raw)
	}
	return Retention{MinDays: lo, MaxDays: hi, Basis: basis}, nil
}

// PendingRetentions are the tierable classes whose retention is pending, neither declared nor given at deployment:
// never destroyed, and listed at go-live for the customer to set (P15.4, TR24).
func (c *Catalog) PendingRetentions() []string {
	var out []string
	for name, cl := range c.Classes {
		if cl.Tierable() && cl.Retention.Pending {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Keys are the keys of a member that declares classes.
func Keys(classes []Class) []config.Key {
	one := int64(1)
	keys := []config.Key{{Name: KeyWindow, Type: config.String, Default: "0 2 * * *"},
		{Name: KeyListDays, Type: config.Integer, Default: int64(90), Min: &one},
		{Name: KeyGrace, Type: config.Duration, Default: 7 * 24 * time.Hour}}
	for _, c := range classes {
		if c.Tierable() {
			keys = append(keys, config.Key{Name: ResidenceKey(c.Name), Type: config.String},
				config.Key{Name: RetentionKey(c.Name), Type: config.String})
		}
	}
	return append(append(keys, ColdKeys()...), StoreKeys()...)
}

// NewCatalog checks the classes (P15.1-P15.5; the SDK's own tables, be_*, are never a class's) and the configured
// residences against N, and builds the member's catalog with its cold store (prefix: the component's in the bucket).
// A class whose route is the object store needs one; an erasable class may not go to a locked bucket (P15.32).
func NewCatalog(ctx context.Context, d *db.DB, classes []Class, values *config.Values, prefix string) (*Catalog, error) {
	n := int(values.Int(KeyListDays))
	store, err := StoreFrom(values, prefix)
	if err != nil {
		return nil, err
	}
	c := &Catalog{DB: d, Classes: map[string]Class{}, Registry: NewRegistry(), Residence: map[string]int{}, Store: store,
		N: time.Duration(n) * 24 * time.Hour, Grace: values.Duration(KeyGrace), ColdSyncLimit: values.Duration(KeyColdSyncLimit),
		ColdConcurrency: int(values.Int(KeyColdConcurrency))}
	locked := false
	if store != nil {
		if locked, err = store.Locked(ctx); err != nil {
			return nil, err
		}
		c.StoreLocked = locked
	}
	for _, cl := range classes {
		if raw := values.String(RetentionKey(cl.Name)); raw != "" && cl.Tierable() { // the deployment's retention (P15.4)
			r, err := retentionOf(raw)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", RetentionKey(cl.Name), err)
			}
			cl.Retention = r
		}
		var own []string
		for _, t := range cl.Tables() {
			if strings.HasPrefix(t.Name, "be_") {
				own = append(own, t.Name)
			}
		}
		if err := cl.Check(n, own); err != nil {
			return nil, err
		}
		if _, dup := c.Classes[cl.Name]; dup {
			return nil, fmt.Errorf("tiering: class %s declared twice", cl.Name)
		}
		c.Classes[cl.Name] = cl
		if !cl.Tierable() {
			continue
		}
		if slices.Contains(cl.Routes, RouteObject) {
			switch {
			case store == nil && cl.Routes[0] == RouteObject:
				return nil, fmt.Errorf("%s: its route is the object store but %s is not set", cl.Name, KeyStoreURL)
			case locked && len(cl.Erasable) > 0:
				return nil, fmt.Errorf("%s: erasable columns may not go to the object-locked bucket %s (P15.32)",
					cl.Name, store.Bucket)
			}
		}
		raw := values.String(ResidenceKey(cl.Name))
		switch {
		case raw == "":
		case raw == "never":
			c.Residence[cl.Name] = Never
		default:
			days, err := strconv.Atoi(raw)
			if err != nil || days < n {
				return nil, fmt.Errorf("%s: %q is not a residence of at least N = %d days or never (P15.3)",
					ResidenceKey(cl.Name), raw, n)
			}
			c.Residence[cl.Name] = days
		}
	}
	return c, nil
}

// CandidatesJob is the SDK's job that seals what has come due (P15.11): every tierable class's groupings, each sealed
// in its own transaction; a unit blocked for 30 days past its residence is an error-level alert.
func (c *Catalog) CandidatesJob(schedule string, log *slog.Logger) work.Job {
	return work.Job{Name: "be.tiering.candidates", Kind: work.Cron, Cron: schedule, TimeZone: "UTC",
		Effect: work.Local, MaxAttempts: work.Unlimited, Timeout: time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			if err := c.SealDue(ctx, log); err != nil {
				return err
			}
			return run.Complete(ctx, func(context.Context, db.Tx) error { return nil })
		}}
}

// SealDue seals every grouping that has come due, and alerts on the units blocked too long.
func (c *Catalog) SealDue(ctx context.Context, log *slog.Logger) error {
	for name, cl := range c.Classes {
		if !cl.Tierable() {
			continue
		}
		groups, err := c.Groupings(ctx, name)
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		for _, g := range groups {
			res, err := c.Seal(ctx, name, g)
			if err != nil {
				return fmt.Errorf("%s %s: %w", name, g.Key, err)
			}
			if len(res.Blocked) == 0 {
				log.Info("an archive unit is sealed", "event", "tiering_sealed", "class", name, "unit", res.Unit,
					"members", res.Members)
			}
		}
	}
	return c.alertBlocked(ctx, log)
}

// alertBlocked logs every open unit blocked for more than BlockedAlertAfter past its residence (protocol 11, TR30).
func (c *Catalog) alertBlocked(ctx context.Context, log *slog.Logger) error {
	return c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, "SELECT id::text, class, grouping, blocked::text, blocked_since FROM be_tier_unit "+
			"WHERE state = 'open' AND blocked IS NOT NULL AND blocked_since <= now() - make_interval(days => $1)",
			int(BlockedAlertAfter/(24*time.Hour)))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, class, grouping, blocked string
			var since time.Time
			if err := rows.Scan(&id, &class, &grouping, &blocked, &since); err != nil {
				return err
			}
			log.Error("an archive unit is blocked past its residence", "event", "tiering_blocked", "class", class,
				"unit", id, "grouping", grouping, "due_since", since, "blocked", blocked)
		}
		return rows.Err()
	})
}
