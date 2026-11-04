package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/brickKit/be-sdk-go/v3/db"
)

// Record is one record as the source returns it.
type Record struct {
	ID      string
	Version int64
	Data    json.RawMessage
}

// Read results of a batch read by ID (protocol 02's S3).
const (
	Exists = "exists"
	Gone   = "tombstone" // deleted: the source keeps a tombstone with its version
	Purged = "purged"    // the tombstone is gone too
)

// ReadResult is one ID's answer to a batch read.
type ReadResult struct {
	ID      string
	State   string
	Version int64
	Data    json.RawMessage
}

// ErrOtherSource means a page was answered by a replica, or by another deployment, instance or epoch than the one the
// alignment or repair runs for (P9.33): the page is discarded and the run fails; the next pull handles the change.
var ErrOtherSource = errors.New("projection: the read was not answered by the adopted source's primary")

// accepts is P9.33's check of one answer against what the run uses (an empty Deployment is not compared).
func (d Identity) accepts(want Identity) error {
	if !d.Primary || d.Impl != want.Impl || d.Epoch != want.Epoch || (want.Deployment != "" && d.Deployment != want.Deployment) {
		return fmt.Errorf("%w: answered by %+v, the run uses %s/%s/%s", ErrOtherSource, d, want.Deployment, want.Impl, want.Epoch)
	}
	return nil
}

// Source is the projected component, read over its contract (RecordClient: be.source.v1.RecordService). Every
// answer declares who answered.
type Source interface {
	// Snapshot returns one page of every existing record, in a stable order; an empty next ends it.
	Snapshot(ctx context.Context, cursor string) (records []Record, next string, from Identity, err error)
	// Read answers by ID: current, deleted (with the deleting version) or purged; never "not found".
	Read(ctx context.Context, ids []string) ([]ReadResult, Identity, error)
}

// ErrReplaced means a newer alignment started while this one ran; its results were discarded.
var ErrReplaced = errAlignmentReplaced

// Align re-fetches everything (P9.12): a new alignment generation; every snapshot row written as complete for the
// adopted instance and epoch without comparing versions; local rows the snapshot did not contain read by ID
// (tombstone, still there, or purged as a version-0 tombstone); then aligned. Every write checks that its
// generation is still the current one, so results of an older alignment never land.
// The source's identity is the first page's declaration; every later page and read must declare the same.
func (p Projection) Align(ctx context.Context, d *db.DB, src Source) error {
	return p.alignTo(ctx, d, src, Identity{}, alignHooks{})
}

// alignHooks are what a watermark source adds to an alignment (P9.28): started runs once the generation began and
// before the snapshot (the echo-only pull that gives P0), write guarded by the generation; purgedAt is the position a
// purged record's tombstone accepts inputs past (its partition's P0); done replaces marking the projection aligned,
// in the completion transaction.
type alignHooks struct {
	status   string // the state the source shows while aligning ("": aligning; a stalled source re-fetches stalled)
	started  func(ctx context.Context, gen int64, write func(func(context.Context, db.Tx) error) error) error
	purgedAt func(recordID string) *int64
	done     func(ctx context.Context, tx db.Tx, gen int64) error
}

// alignTo runs for want's identity; an empty want.Impl takes the first page's declaration (it must be a primary's).
func (p Projection) alignTo(ctx context.Context, d *db.DB, src Source, want Identity, h alignHooks) error {
	learn := want.Impl == ""
	impl, epoch := want.Impl, want.Epoch
	var gen int64
	status := h.status
	if status == "" {
		status = Aligning
	}
	if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO be_projection_source (projection, impl, epoch, align_gen, status)
			VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), 1, $4)
			ON CONFLICT (projection) DO UPDATE SET impl = coalesce(excluded.impl, be_projection_source.impl),
				epoch = coalesce(excluded.epoch, be_projection_source.epoch),
				align_gen = be_projection_source.align_gen + 1, status = excluded.status
			RETURNING align_gen`, p.Name, impl, epoch, status).Scan(&gen)
	}); err != nil {
		return err
	}
	write := func(fn func(context.Context, db.Tx) error) error {
		return d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			var current int64
			if err := tx.QueryRow(ctx, "SELECT align_gen FROM be_projection_source WHERE projection = $1 FOR SHARE", p.Name).
				Scan(&current); err != nil {
				return err
			}
			if current != gen {
				return ErrReplaced
			}
			return fn(ctx, tx)
		})
	}
	if h.started != nil {
		if err := h.started(ctx, gen, write); err != nil {
			return err
		}
	}
	// put overwrites without comparing versions, except rows written during this alignment: those only by a newer one
	put := func(ctx context.Context, tx db.Tx, id, state string, version int64, data json.RawMessage, minRead *int64) error {
		var raw any
		if data != nil {
			raw = string(data)
		}
		tag, err := tx.Exec(ctx, `INSERT INTO be_projection (projection, record_id, version, state, impl, epoch, data, aligned_gen,
				min_read_seq)
			VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
			ON CONFLICT (projection, record_id) DO UPDATE SET version = excluded.version, state = excluded.state,
				need_version = NULL, impl = excluded.impl, epoch = excluded.epoch, data = excluded.data,
				aligned_gen = excluded.aligned_gen, min_read_seq = excluded.min_read_seq, updated_at = now()
			WHERE be_projection.aligned_gen IS DISTINCT FROM $8 OR be_projection.version < excluded.version`,
			p.Name, id, version, state, impl, epoch, raw, gen, minRead)
		if err == nil && tag.RowsAffected() > 0 {
			err = p.changed(ctx, tx, id)
		}
		return err
	}
	for cursor := ""; ; {
		records, next, from, err := src.Snapshot(ctx, cursor)
		if err != nil {
			return err
		}
		if learn {
			if !from.Primary {
				return fmt.Errorf("%w: a replica answered", ErrOtherSource)
			}
			want, impl, epoch, learn = from, from.Impl, from.Epoch, false
			if err := write(func(ctx context.Context, tx db.Tx) error {
				_, err := tx.Exec(ctx, "UPDATE be_projection_source SET impl = $2, epoch = $3 WHERE projection = $1",
					p.Name, impl, epoch)
				return err
			}); err != nil {
				return err
			}
		}
		if err := from.accepts(want); err != nil {
			return err
		}
		if err := write(func(ctx context.Context, tx db.Tx) error {
			for _, r := range records {
				if err := put(ctx, tx, r.ID, Complete, r.Version, r.Data, nil); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if next == "" {
			break
		}
		cursor = next
	}
	// local rows the snapshot did not touch; every round writes its rows with this generation, so a round that finds
	// the same rows again made no progress and would loop forever
	var last []string
	for {
		var ids []string
		if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			rows, err := tx.Query(ctx, `SELECT record_id FROM be_projection WHERE projection = $1
				AND aligned_gen IS DISTINCT FROM $2 ORDER BY record_id LIMIT 500`, p.Name, gen)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					return err
				}
				ids = append(ids, id)
			}
			return rows.Err()
		}); err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		if slices.Equal(ids, last) {
			return fmt.Errorf("projection %s: the re-fetch wrote none of %d rows read by ID; it stops", p.Name, len(ids))
		}
		last = ids
		results, from, err := src.Read(ctx, ids)
		if err != nil {
			return err
		}
		if err := from.accepts(want); err != nil {
			return err
		}
		answered := map[string]bool{}
		if err := write(func(ctx context.Context, tx db.Tx) error {
			for _, r := range results {
				answered[r.ID] = true
				switch r.State {
				case Exists:
					err = put(ctx, tx, r.ID, Complete, r.Version, r.Data, nil)
				case Gone:
					err = put(ctx, tx, r.ID, Tombstone, r.Version, nil, nil)
				case Purged:
					var minRead *int64
					if h.purgedAt != nil {
						minRead = h.purgedAt(r.ID)
					}
					err = put(ctx, tx, r.ID, Tombstone, 0, nil, minRead)
				default:
					err = errors.New("projection: the source answered " + r.State)
				}
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, id := range ids {
			if !answered[id] {
				return errors.New("projection: the source did not answer for " + id + " (a batch read never omits an ID)")
			}
		}
	}
	return write(func(ctx context.Context, tx db.Tx) error {
		if h.done != nil {
			return h.done(ctx, tx, gen)
		}
		_, err := tx.Exec(ctx, "UPDATE be_projection_source SET status = 'aligned', aligned_at = now() WHERE projection = $1", p.Name)
		return err
	})
}

// Repair fetches the incomplete rows' current state and applies it (protocol 02, "repairing incomplete rows"); it
// returns how many rows changed. A source answering for another instance or epoch than the adopted one changes
// nothing (Apply compares the declared identity): align first; a replica's answer is not used (P9.33).
func (p Projection) Repair(ctx context.Context, d *db.DB, src Source) (int, error) {
	var ids []string
	if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `SELECT record_id FROM be_projection WHERE projection = $1 AND state = 'incomplete'
			ORDER BY record_id LIMIT 500`, p.Name)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return rows.Err()
	}); err != nil || len(ids) == 0 {
		return 0, err
	}
	results, from, err := src.Read(ctx, ids)
	if err != nil {
		return 0, err
	}
	if !from.Primary {
		return 0, fmt.Errorf("%w: a replica answered", ErrOtherSource)
	}
	impl, epoch := from.Impl, from.Epoch
	n := 0
	err = d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		n = 0
		for _, r := range results {
			kind, ok := map[string]string{Exists: Full, Gone: Delete, Purged: Purged}[r.State]
			if !ok {
				return errors.New("projection: the source answered " + r.State)
			}
			out, err := p.Apply(ctx, tx, Input{RecordID: r.ID, Version: r.Version, Impl: impl, Epoch: epoch, Data: r.Data, Kind: kind})
			if err != nil {
				return err
			}
			if out.Changed {
				n++
			}
		}
		return nil
	})
	return n, err
}
