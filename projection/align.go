package projection

import (
	"context"
	"encoding/json"
	"errors"

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

// Source is the projected component, read over its contract.
type Source interface {
	// Snapshot returns one page of every record, in a stable order; an empty next ends it.
	Snapshot(ctx context.Context, cursor string) (records []Record, next string, err error)
	// Read answers by ID: current, deleted (with the deleting version) or purged; never "not found".
	Read(ctx context.Context, ids []string) ([]ReadResult, error)
	// Identity is the source's implementation instance and epoch as its responses declare them.
	Identity(ctx context.Context) (impl, epoch string, err error)
}

// ErrReplaced means a newer alignment started while this one ran; its results were discarded.
var ErrReplaced = errAlignmentReplaced

// Align re-fetches everything (P9.12): a new alignment generation; every snapshot row written as complete for the
// adopted instance and epoch without comparing versions; local rows the snapshot did not contain read by ID
// (tombstone, still there, or purged as a version-0 tombstone); then aligned. Every write checks that its
// generation is still the current one, so results of an older alignment never land.
func (p Projection) Align(ctx context.Context, d *db.DB, src Source) error {
	impl, epoch, err := src.Identity(ctx)
	if err != nil {
		return err
	}
	var gen int64
	if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, `INSERT INTO be_projection_source (projection, impl, epoch, align_gen, status)
			VALUES ($1, $2, $3, 1, 'aligning')
			ON CONFLICT (projection) DO UPDATE SET impl = excluded.impl, epoch = excluded.epoch,
				align_gen = be_projection_source.align_gen + 1, status = 'aligning'
			RETURNING align_gen`, p.Name, impl, epoch).Scan(&gen)
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
	// put overwrites without comparing versions, except rows written during this alignment: those only by a newer one
	put := func(ctx context.Context, tx db.Tx, id, state string, version int64, data json.RawMessage) error {
		var raw any
		if data != nil {
			raw = string(data)
		}
		_, err := tx.Exec(ctx, `INSERT INTO be_projection (projection, record_id, version, state, impl, epoch, data, aligned_gen)
			VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
			ON CONFLICT (projection, record_id) DO UPDATE SET version = excluded.version, state = excluded.state,
				need_version = NULL, impl = excluded.impl, epoch = excluded.epoch, data = excluded.data,
				aligned_gen = excluded.aligned_gen, updated_at = now()
			WHERE be_projection.aligned_gen IS DISTINCT FROM $8 OR be_projection.version < excluded.version`,
			p.Name, id, version, state, impl, epoch, raw, gen)
		return err
	}
	for cursor := ""; ; {
		records, next, err := src.Snapshot(ctx, cursor)
		if err != nil {
			return err
		}
		if err := write(func(ctx context.Context, tx db.Tx) error {
			for _, r := range records {
				if err := put(ctx, tx, r.ID, Complete, r.Version, r.Data); err != nil {
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
	// local rows the snapshot did not touch
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
		results, err := src.Read(ctx, ids)
		if err != nil {
			return err
		}
		answered := map[string]bool{}
		if err := write(func(ctx context.Context, tx db.Tx) error {
			for _, r := range results {
				answered[r.ID] = true
				switch r.State {
				case Exists:
					err = put(ctx, tx, r.ID, Complete, r.Version, r.Data)
				case Gone:
					err = put(ctx, tx, r.ID, Tombstone, r.Version, nil)
				case Purged:
					err = put(ctx, tx, r.ID, Tombstone, 0, nil)
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
		_, err := tx.Exec(ctx, "UPDATE be_projection_source SET status = 'aligned', aligned_at = now() WHERE projection = $1", p.Name)
		return err
	})
}

// Repair fetches the incomplete rows' current state and applies it (protocol 02, "repairing incomplete rows"); it
// returns how many rows changed. A source answering for another instance or epoch than the adopted one changes
// nothing: align first.
func (p Projection) Repair(ctx context.Context, d *db.DB, src Source) (int, error) {
	impl, epoch, err := src.Identity(ctx)
	if err != nil {
		return 0, err
	}
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
	results, err := src.Read(ctx, ids)
	if err != nil {
		return 0, err
	}
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
