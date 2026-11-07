package tiering

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/evidence"
	"github.com/brickKit/be-sdk-go/v3/work"
	"github.com/jackc/pgx/v5"
)

// Destruction (be-protocol P15.29, P15.30): decided in one local transaction that first takes the hold table in SHARE
// mode (a hold set meanwhile waits for it, TR14), then checks the retention is due, no live hold covers the unit, an
// approval does, and the retention is not pending. Its evidence is written in two phases: the intent before the
// decision (cancelled when the decision does not hold), the conclusion in it. The data goes after the commit and may
// be deleted again: the online rows, the warm rows, the thawed copy and the objects. The members stay (a destroyed
// record read by key is not found).

// Evidence category and the reason a destruction is not due yet.
const (
	CategoryDestruction = "destruction"
	WhyNotDue           = "retention_not_due"
	// PurgeAgain is how long after a destruction the job deletes its data again (a purge that failed, a replay's).
	PurgeAgain = 7 * 24 * time.Hour
)

// unitFacts is a unit as the destruction reads it.
type unitFacts struct {
	class, state, digest string
	manifest             int
	anchor               time.Time
}

func readUnit(ctx context.Context, tx db.Tx, unit string, lock bool) (unitFacts, error) {
	var u unitFacts
	var digest *string
	var anchor *time.Time
	q := "SELECT class, state, digest, manifest_version, anchor_max FROM be_tier_unit WHERE id::text = $1"
	if lock {
		q += " FOR UPDATE"
	}
	err := tx.QueryRow(ctx, q, unit).Scan(&u.class, &u.state, &digest, &u.manifest, &anchor)
	if digest != nil {
		u.digest = *digest
	}
	if anchor != nil {
		u.anchor = *anchor
	}
	return u, err
}

// destroyReasons are what holds a unit's destruction back now (P15.29), with the plan's names.
func (c *Catalog) destroyReasons(ctx context.Context, tx db.Tx, unit string, u unitFacts) ([]string, error) {
	cl := c.Classes[u.class]
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return nil, err
	}
	due := !u.anchor.IsZero() && !u.anchor.Add(time.Duration(cl.Retention.MinDays)*24*time.Hour).After(now)
	if !cl.Retention.Pending && !due { // a pending retention is never due: the plan says why (TR24)
		return []string{WhyNotDue}, nil
	}
	holds, err := holdsOn(ctx, tx, unit, u.class)
	if err != nil {
		return nil, err
	}
	approved, err := approvedFor(ctx, tx, unit, u.class)
	if err != nil {
		return nil, err
	}
	d := Plan(now, c.N, c.Grace, PlanClass{Category: cl.Category, ResidenceDays: cl.ResidenceDays,
		RetentionPending: cl.Retention.Pending, Erasable: len(cl.Erasable) > 0},
		PlanUnit{State: u.state, RetentionDue: true, Approved: approved, Holds: holds})
	switch {
	case d.Action == ActDestroy:
		return nil, nil
	case len(d.Reasons) > 0: // held back: the plan names why
		return d.Reasons, nil
	}
	return []string{WhyNotDue}, nil
}

// markDestroyed turns a unit destroyed with its tombstone and moves the catalog version.
func markDestroyed(ctx context.Context, tx db.Tx, unit string, u unitFacts, basis string) error {
	if _, err := tx.Exec(ctx, "UPDATE be_tier_unit SET state = 'destroyed', destroyed_at = now(), tombstone = "+
		"jsonb_build_object('scope', 'unit', 'unit', id::text, 'class', class, 'digest', digest, 'members', members, "+
		"'basis', $2::text, 'at', now()) WHERE id::text = $1", unit, basis); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "UPDATE be_tier_catalog SET version = version + 1")
	return err
}

// Destroy destroys a unit whose destruction is due and allowed; it answers the reasons it did not. A unit already
// destroyed has its data deleted again.
func (c *Catalog) Destroy(ctx context.Context, unit string) ([]string, error) {
	var u unitFacts
	var why []string
	if err := c.tx(ctx, "destroy.reasons", func(ctx context.Context, tx db.Tx) (err error) {
		if u, err = readUnit(ctx, tx, unit, false); err != nil || u.state == StateDestroyed || u.state == StateOpen {
			return err
		}
		why, err = c.destroyReasons(ctx, tx, unit, u)
		return err
	}); err != nil {
		return nil, err
	}
	switch {
	case u.state == StateDestroyed:
		return nil, c.purge(ctx, unit, c.Classes[u.class])
	case u.state == StateOpen:
		return nil, fmt.Errorf("tiering: unit %s is open: nothing to destroy", unit)
	case len(why) > 0:
		return why, nil
	case c.Evidence == nil:
		return nil, errors.New("tiering: no evidence writer: a destruction is never done without its evidence (P15.29)")
	}
	cl := c.Classes[u.class]
	p, err := c.Evidence.Intent(ctx, CategoryDestruction, "", evidence.Object{Type: "be.tier.unit", ID: unit,
		Version: strconv.Itoa(u.manifest)}, map[string]any{"unit": unit, "class": u.class, "digest": u.digest,
		"manifest_version": strconv.Itoa(u.manifest), "basis": cl.Retention.Basis})
	if err != nil {
		return nil, err
	}
	err = c.tx(ctx, "destroy.decide", func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, "LOCK TABLE be_tier_hold IN SHARE MODE"); err != nil {
			return err
		}
		cur, err := readUnit(ctx, tx, unit, true)
		if err != nil {
			return err
		}
		if cur.state == StateDestroyed || cur.state == StateOpen {
			why = []string{WhyNotDue}
			return nil
		}
		if why, err = c.destroyReasons(ctx, tx, unit, cur); err != nil || len(why) > 0 {
			return err
		}
		if err := p.Effective(ctx, tx); err != nil {
			return err
		}
		return markDestroyed(ctx, tx, unit, cur, cl.Retention.Basis)
	})
	if err != nil || len(why) > 0 {
		if cerr := p.Cancelled(context.WithoutCancel(ctx)); cerr != nil {
			err = errors.Join(err, cerr)
		}
		return why, err
	}
	return nil, c.purge(ctx, unit, cl)
}

// purge deletes a destroyed unit's data: online rows, warm rows, the thawed copy, the objects. It may run again.
func (c *Catalog) purge(ctx context.Context, unit string, cl Class) error {
	if err := c.deleteOnline(ctx, unit, cl, nil); err != nil {
		return err
	}
	if err := c.tx(ctx, "destroy.purge", func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, "DELETE FROM be_tier_thaw WHERE unit::text = $1", unit); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "DELETE FROM be_tier_row WHERE unit::text = $1", unit)
		return err
	}); err != nil {
		return err
	}
	if c.Store == nil {
		return nil
	}
	objects, err := c.Store.List(ctx, fmt.Sprintf("%s/%s/%s/", c.Store.Prefix, cl.Name, unit))
	if err != nil {
		return err
	}
	for _, o := range objects {
		if err := c.Store.Delete(ctx, o.Key); err != nil {
			return err
		}
		crash("destroy.object/done")
	}
	return nil
}

// DestroyJob is the SDK's daily job (P15.29): every unit past open is tried (Destroy checks), and the data of the
// units destroyed in the last PurgeAgain is deleted again.
func (c *Catalog) DestroyJob(log *slog.Logger) work.Job {
	return work.Job{Name: "be.tiering.destroy", Kind: work.Singleton, Effect: work.Local, Interval: 24 * time.Hour,
		MaxAttempts: work.Unlimited, Timeout: 6 * time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			var units []string
			if err := c.tx(ctx, "destroy.due", func(ctx context.Context, tx db.Tx) error {
				rows, err := tx.Query(ctx, "SELECT id::text FROM be_tier_unit WHERE state IN ('sealed', 'published', 'released') "+
					"OR (state = 'destroyed' AND destroyed_at > now() - make_interval(secs => $1)) ORDER BY id", PurgeAgain.Seconds())
				if err != nil {
					return err
				}
				units, err = pgx.CollectRows(rows, pgx.RowTo[string])
				return err
			}); err != nil {
				return err
			}
			continued, err := c.continueReleases(ctx) // released units with online rows left (the recovery check)
			if err != nil {
				log.Error("a released unit's online rows could not be deleted", "event", "tiering_release_continue_failed",
					"error", err.Error())
			}
			for _, u := range continued {
				log.Warn("a released unit still had online rows: deleted", "event", "tiering_release_continued", "unit", u)
			}
			for _, u := range units {
				why, err := c.Destroy(ctx, u)
				switch {
				case err != nil:
					log.Error("an archive unit's destruction failed", "event", "tiering_destroy_failed", "unit", u, "error", err.Error())
				case len(why) == 0:
					log.Info("an archive unit is destroyed (or its data deleted again)", "event", "tiering_destroyed", "unit", u)
				}
			}
			return run.Complete(ctx, func(context.Context, db.Tx) error { return nil })
		}}
}

// Replaying says whether the catalog has evidence to replay: a class that can be destroyed (its retention is not
// pending) or erased. The component then declares the replay's operation (Definition.Replay).
func (c *Catalog) Replaying() bool {
	for _, cl := range c.Classes {
		if !cl.Retention.Pending || len(cl.Erasable) > 0 {
			return true
		}
	}
	return false
}

// Replays registers the re-execution of the destructions and erasures a full recovery finds after its point (P15.30,
// TR16): a destroyed unit turns destroyed without the business preconditions, its data going with the next
// destruction run (PurgeAgain); an erasure as replayErasure says.
func (c *Catalog) Replays(r *evidence.Replays) {
	r.Add(CategoryErasure, c.replayErasure)
	r.Add(CategoryDestruction, func(ctx context.Context, tx db.Tx, rec evidence.Replayed) error {
		unit := rec.Object.ID
		u, err := readUnit(ctx, tx, unit, true)
		if errors.Is(err, pgx.ErrNoRows) {
			return evidence.ErrDangling
		}
		if err != nil || u.state == StateDestroyed {
			return err
		}
		basis, _ := rec.Change["basis"].(string)
		return markDestroyed(ctx, tx, unit, u, basis)
	})
}
