package tiering

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/runprofile"
	"github.com/brickKit/be-sdk-go/v3/work"
)

// Releasing the online rows of a published unit (be-protocol P15.19, P15.21): only when the grace has passed, the
// unit's newest record is older than N days, no read lease taken before the route changed is still running, and a
// backup finished after the publish. The rows go in batches, the children first; the SDK names the unit in each
// transaction (be.tier_unit) so the write barrier lets this one change through.

// ReleaseBatch bounds the rows one release transaction deletes per table (Catalog.Batch overrides it).
const ReleaseBatch = 10_000

// BackupSource answers when the deployment's latest backup finished (the run profile's kind backup, P15.21); ok=false
// when it cannot tell (no run profile configured).
type BackupSource func(ctx context.Context) (finished time.Time, ok bool, err error)

// Lister is what the release needs of the run profile (runprofile.Store).
type Lister interface {
	ListAll(ctx context.Context, kind string) ([]runprofile.Listed, error)
}

// BackupsFrom reads the latest backup from the run profile's kind backup: the greatest finished_at among the records
// whose signature holds (P12.4, P15.21).
func BackupsFrom(rp Lister) BackupSource {
	return func(ctx context.Context) (time.Time, bool, error) {
		listed, err := rp.ListAll(ctx, runprofile.KindBackup)
		if err != nil {
			return time.Time{}, false, err
		}
		var latest time.Time
		found := false
		for _, l := range listed {
			if l.Err != nil || !l.Read {
				continue
			}
			var b struct {
				FinishedAt time.Time `json:"finished_at"`
			}
			if json.Unmarshal(l.Record.Body, &b) != nil || b.FinishedAt.IsZero() {
				continue
			}
			if b.FinishedAt.After(latest) {
				latest, found = b.FinishedAt, true
			}
		}
		return latest, found, nil
	}
}

// unitAt is a published unit as the release reads it.
type unitAt struct {
	class                      string
	state                      string
	publishedAt, newestCreated time.Time
	routeVersion               int64
}

// releaseReasons are what holds a published unit's release back now (P15.19), with the plan's names.
func (c *Catalog) releaseReasons(ctx context.Context, tx db.Tx, u unitAt) ([]string, error) {
	var leases int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM be_tier_lease WHERE catalog_version < $1 AND expires_at > now()",
		u.routeVersion).Scan(&leases); err != nil {
		return nil, err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return nil, err
	}
	backed := false
	if c.Backups != nil {
		finished, ok, err := c.Backups(ctx)
		if err != nil {
			return nil, err
		}
		backed = ok && finished.After(u.publishedAt)
	}
	d := Plan(now, c.N, c.Grace, PlanClass{Category: c.Classes[u.class].Category, ResidenceDays: c.Classes[u.class].ResidenceDays},
		PlanUnit{State: StatePublished, PublishedAt: u.publishedAt, NewestCreated: u.newestCreated, OldLeases: leases,
			BackupAfterPublish: backed})
	return d.Reasons, nil
}

// Release releases one published unit's online rows if nothing holds it back; it answers the reasons it did not.
func (c *Catalog) Release(ctx context.Context, unit string) ([]string, error) {
	var why []string
	var u unitAt
	err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var rv *int64
		if err := tx.QueryRow(ctx, "SELECT class, state, published_at, newest_created, route_version FROM be_tier_unit "+
			"WHERE id = $1", unit).Scan(&u.class, &u.state, &u.publishedAt, &u.newestCreated, &rv); err != nil {
			return err
		}
		if u.state != StatePublished {
			return fmt.Errorf("tiering: unit %s is %s, not published", unit, u.state)
		}
		if rv != nil {
			u.routeVersion = *rv
		}
		var err error
		why, err = c.releaseReasons(ctx, tx, u)
		return err
	})
	if err != nil || len(why) > 0 {
		return why, err
	}
	err = c.deleteOnline(ctx, unit, c.Classes[u.class], func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE be_tier_unit SET state = 'released', released_at = now() WHERE id = $1 "+
			"AND state = 'published'", unit); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "UPDATE be_tier_catalog SET version = version + 1")
		return err
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// deleteOnline deletes a unit's online rows in batches, the children first, each transaction naming the unit for the
// write barrier (be.tier_unit); last runs in the transaction that finds none left (nil: nothing more).
func (c *Catalog) deleteOnline(ctx context.Context, unit string, cl Class, last func(context.Context, db.Tx) error) error {
	batch := c.Batch
	if batch <= 0 {
		batch = ReleaseBatch
	}
	members := "(SELECT m.root_id FROM be_tier_member m WHERE m.unit = $1)"
	for done := false; !done; {
		err := c.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			if _, err := tx.Exec(ctx, "SELECT set_config('be.tier_unit', $1, true)", unit); err != nil {
				return err
			}
			left := int64(0)
			for _, ch := range cl.Children {
				tag, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %[1]s WHERE ctid IN (SELECT ctid FROM %[1]s WHERE %[2]s::text "+
					"IN %[3]s LIMIT %[4]d)", ident(ch.Name), ident(ch.FK), members, batch), unit)
				if err != nil {
					return err
				}
				left += tag.RowsAffected()
			}
			if left > 0 { // children first: the roots go once no child of the unit is left
				return nil
			}
			tag, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %[1]s WHERE ctid IN (SELECT ctid FROM %[1]s WHERE %[2]s::text "+
				"IN %[3]s LIMIT %[4]d)", ident(cl.Root.Name), ident(cl.Root.PK), members, batch), unit)
			if err != nil {
				return err
			}
			if tag.RowsAffected() > 0 {
				return nil
			}
			done = true
			if last == nil {
				return nil
			}
			return last(ctx, tx)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// ReleaseJob is the SDK's job that releases the published units whose conditions hold (P15.19).
func (c *Catalog) ReleaseJob(log *slog.Logger) work.Job {
	return work.Job{Name: "be.tiering.release", Kind: work.Singleton, Effect: work.Local, Interval: time.Hour,
		MaxAttempts: work.Unlimited, Timeout: time.Hour,
		Handle: func(ctx context.Context, run *work.Run) error {
			units, err := c.unitsIn(ctx, StatePublished)
			if err != nil {
				return err
			}
			for _, u := range units {
				why, err := c.Release(ctx, u)
				switch {
				case err != nil:
					log.Error("an archive unit's release failed", "event", "tiering_release_failed", "unit", u, "error", err.Error())
				case len(why) == 0:
					log.Info("an archive unit's online rows are released", "event", "tiering_released", "unit", u)
				default:
					sort.Strings(why)
					if c.Backups == nil {
						log.Error("no run profile: archive units are never released", "event", "tiering_no_backup_record", "unit", u)
					}
				}
			}
			return run.Complete(ctx, func(context.Context, db.Tx) error { return nil })
		}}
}
