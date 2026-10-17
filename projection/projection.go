// Package projection keeps local copies of other components' records (protocol P9.10-P9.12; assembly project
// protocol 02): each row carries the source's version and a state, inputs are received by the protocol's state
// table, and a full re-fetch aligns the projection with its source.
package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/jackc/pgx/v5"
)

// States of a row.
const (
	Complete   = "complete"
	Incomplete = "incomplete"
	Tombstone  = "tombstone"
)

// Kinds of input.
const (
	Full    = "full"
	Partial = "partial"
	Delete  = "delete"
)

// Actions a receive leads to.
const (
	Write         = "write"
	Overwrite     = "overwrite"
	MergeAndFetch = "merge_and_fetch"
	WriteAndFetch = "write_and_fetch"
	Ignore        = "ignore"
)

// Row is a projection row's receiving state.
type Row struct {
	State       string
	Version     int64
	NeedVersion int64 // incomplete rows
	Old         bool  // belongs to an older epoch or implementation instance than the adopted one
}

// Decide is protocol 02's state table for one row: given the local row (nil: none) and an input's kind and version,
// the row's next state and what to do.
func Decide(cur *Row, kind string, v int64) (Row, string) {
	if cur == nil {
		switch kind {
		case Full:
			return Row{State: Complete, Version: v}, Write
		case Partial:
			return Row{State: Incomplete, Version: v, NeedVersion: v}, WriteAndFetch
		}
		return Row{State: Tombstone, Version: v}, Write
	}
	if cur.Old {
		switch kind {
		case Partial:
			return Row{State: Incomplete, Version: v, NeedVersion: v}, WriteAndFetch
		case Full:
			return Row{State: Complete, Version: v}, Overwrite
		}
		return Row{State: Tombstone, Version: v}, Overwrite
	}
	same := *cur
	same.Old = false
	switch {
	case cur.State == Tombstone && v <= cur.Version:
		return same, Ignore // a tombstone is never revived by the same or an older version
	case cur.State == Tombstone:
		switch kind {
		case Full:
			return Row{State: Complete, Version: v}, Overwrite
		case Partial:
			return Row{State: Incomplete, Version: v, NeedVersion: v}, WriteAndFetch
		}
		return Row{State: Tombstone, Version: v}, Write
	case kind == Full && cur.State == Complete && v > cur.Version,
		kind == Full && cur.State == Incomplete && v >= cur.NeedVersion:
		return Row{State: Complete, Version: v}, Overwrite
	case kind == Partial && v > cur.Version:
		return Row{State: Incomplete, Version: v, NeedVersion: v}, MergeAndFetch
	case kind == Delete && v >= cur.Version:
		return Row{State: Tombstone, Version: v}, Write
	}
	return same, Ignore
}

// Input is one received event or read result for a projection.
type Input struct {
	RecordID string
	Kind     string
	Version  int64
	Impl     string
	Epoch    string
	Data     json.RawMessage // full: the whole content; partial: the fields to merge
}

// Projection is one named projection in the consumer's schema.
type Projection struct {
	Name string
}

// Apply receives one input inside the consumer's transaction and reports whether the row now waits for a fetch.
func (p Projection) Apply(ctx context.Context, tx db.Tx, in Input) (fetch bool, err error) {
	var cur *Row
	var r Row
	var impl, epoch *string
	err = tx.QueryRow(ctx, `SELECT state, version, coalesce(need_version, 0), impl, epoch FROM be_projection
		WHERE projection = $1 AND record_id = $2 FOR UPDATE`, p.Name, in.RecordID).Scan(&r.State, &r.Version, &r.NeedVersion, &impl, &epoch)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return false, err
	default:
		r.Old = deref(impl) != in.Impl || deref(epoch) != in.Epoch
		cur = &r
	}
	next, action := Decide(cur, in.Kind, in.Version)
	if action == Ignore {
		return false, nil
	}
	var need any
	if next.State == Incomplete {
		need = next.NeedVersion
	}
	data := any(nil)
	switch {
	case action == MergeAndFetch:
		_, err = tx.Exec(ctx, `UPDATE be_projection SET state = $3, version = $4, need_version = $5,
			data = coalesce(data, '{}'::jsonb) || $6::jsonb, impl = $7, epoch = $8, updated_at = now(), aligned_gen = `+duringAlignment+`
			WHERE projection = $1 AND record_id = $2`, p.Name, in.RecordID, next.State, next.Version, need, string(in.Data), in.Impl, in.Epoch)
		return true, err
	case next.State == Complete || (next.State == Incomplete && cur == nil):
		data = string(in.Data)
	}
	_, err = tx.Exec(ctx, `INSERT INTO be_projection (projection, record_id, version, state, need_version, impl, epoch, data, aligned_gen)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, `+duringAlignment+`)
		ON CONFLICT (projection, record_id) DO UPDATE SET version = excluded.version, state = excluded.state,
			need_version = excluded.need_version, impl = excluded.impl, epoch = excluded.epoch, data = excluded.data,
			aligned_gen = excluded.aligned_gen, updated_at = now()`, p.Name, in.RecordID, next.Version, next.State, need, in.Impl, in.Epoch, data)
	return next.State == Incomplete, err
}

// duringAlignment marks a row received while its source aligns ("written during alignment", protocol 02): the
// snapshot then overwrites it only with a newer version, and the by-ID read leaves it to the stream.
const duringAlignment = `(SELECT align_gen FROM be_projection_source WHERE projection = $1 AND status = 'aligning')`

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Get reads a row: its state, version and content (ok=false: no row).
func (p Projection) Get(ctx context.Context, tx db.Tx, id string) (state string, version int64, data json.RawMessage, ok bool, err error) {
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT state, version, data FROM be_projection WHERE projection = $1 AND record_id = $2`,
		p.Name, id).Scan(&state, &version, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, nil, false, nil
	}
	return state, version, raw, err == nil, err
}

var errAlignmentReplaced = fmt.Errorf("projection: a newer alignment started; this one's results are discarded")
