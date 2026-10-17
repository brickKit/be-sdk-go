// Package projection keeps local copies of other components' records (protocol P9.10-P9.12; assembly project
// protocol 02): each row carries the source's version and a state, inputs are received by the protocol's state
// table, and a full re-fetch aligns the projection with its source.
package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/jackc/pgx/v5"
)

// States of a row.
const (
	Complete   = "complete"
	Incomplete = "incomplete"
	Tombstone  = "tombstone"
)

// Kinds of input. Purged is a by-ID read answering "purged" (the same word as the read state).
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
	if kind == Purged {
		return Row{State: Tombstone}, Write // a read answering "purged": a version-0 tombstone from any state
	}
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

// Outcome is what receiving one input did.
type Outcome struct {
	Fetch   bool   // the row now waits for a fetch (Repair)
	Changed bool   // the row was written
	Skipped string // why the input was not applied: OtherInstance, OlderEpoch or NewerEpoch; "" otherwise
}

// Reasons an input is not applied (protocol 02's receiving rules, before the state table).
const (
	OtherInstance = "other_instance" // not the adopted implementation instance: retired, or the next one to adopt
	OlderEpoch    = "older_epoch"    // ignored
	NewerEpoch    = "newer_epoch"    // the source moved on: align (Align) before applying its inputs
)

// Apply receives one input inside the consumer's transaction (P9.11). Inputs are first checked against the
// adopted implementation instance and epoch of the projection's source (when it has aligned once): another
// instance's or an older epoch's inputs are not applied, nor a newer epoch's until the projection aligns to it. A
// row of an older instance or epoch than the adopted one is overwritten without comparing versions; everything else
// follows the state table. Inputs for one record are serialized, so concurrent inputs for a row that does not exist
// yet cannot overwrite each other unchecked. Kind Purged (a read answering "purged") writes a version-0 tombstone.
func (p Projection) Apply(ctx context.Context, tx db.Tx, in Input) (Outcome, error) {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended('be_projection:' || $1 || '/' || $2, 0))",
		p.Name, in.RecordID); err != nil {
		return Outcome{}, err
	}
	adoptedImpl, adoptedEpoch, err := p.adopted(ctx, tx)
	if err != nil {
		return Outcome{}, err
	}
	if adoptedImpl != "" {
		if in.Impl != adoptedImpl {
			return Outcome{Skipped: OtherInstance}, nil
		}
		switch c, err := compareEpochs(in.Epoch, adoptedEpoch); {
		case err != nil:
			return Outcome{}, err
		case c < 0:
			return Outcome{Skipped: OlderEpoch}, nil
		case c > 0:
			return Outcome{Skipped: NewerEpoch}, nil
		}
	}
	var cur *Row
	var r Row
	var impl, epoch *string
	err = tx.QueryRow(ctx, `SELECT state, version, coalesce(need_version, 0), impl, epoch FROM be_projection
		WHERE projection = $1 AND record_id = $2 FOR UPDATE`, p.Name, in.RecordID).Scan(&r.State, &r.Version, &r.NeedVersion, &impl, &epoch)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return Outcome{}, err
	default:
		// the input is of the adopted instance and epoch (or nothing is adopted yet): the row is old when it differs
		r.Old = deref(impl) != in.Impl || deref(epoch) != in.Epoch
		cur = &r
	}
	next, action := Decide(cur, in.Kind, in.Version)
	if action == Ignore {
		return Outcome{}, nil
	}
	return p.write(ctx, tx, in, cur, next, action)
}

func (p Projection) write(ctx context.Context, tx db.Tx, in Input, cur *Row, next Row, action string) (Outcome, error) {
	var need any
	if next.State == Incomplete {
		need = next.NeedVersion
	}
	if action == MergeAndFetch {
		_, err := tx.Exec(ctx, `UPDATE be_projection SET state = $3, version = $4, need_version = $5,
			data = coalesce(data, '{}'::jsonb) || $6::jsonb, impl = $7, epoch = $8, updated_at = now(), aligned_gen = `+duringAlignment+`
			WHERE projection = $1 AND record_id = $2`, p.Name, in.RecordID, next.State, next.Version, need, string(in.Data), in.Impl, in.Epoch)
		return Outcome{Fetch: true, Changed: true}, err
	}
	var data any
	if next.State == Complete || (next.State == Incomplete && cur == nil) {
		data = string(in.Data)
	}
	_, err := tx.Exec(ctx, `INSERT INTO be_projection (projection, record_id, version, state, need_version, impl, epoch, data, aligned_gen)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, `+duringAlignment+`)
		ON CONFLICT (projection, record_id) DO UPDATE SET version = excluded.version, state = excluded.state,
			need_version = excluded.need_version, impl = excluded.impl, epoch = excluded.epoch, data = excluded.data,
			aligned_gen = excluded.aligned_gen, updated_at = now()`, p.Name, in.RecordID, next.Version, next.State, need, in.Impl, in.Epoch, data)
	return Outcome{Fetch: next.State == Incomplete, Changed: true}, err
}

// adopted is the source's adopted implementation instance and epoch ("": the projection never aligned).
func (p Projection) adopted(ctx context.Context, tx db.Tx) (impl, epoch string, err error) {
	var i, e *string
	err = tx.QueryRow(ctx, "SELECT impl, epoch FROM be_projection_source WHERE projection = $1", p.Name).Scan(&i, &e)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	return deref(i), deref(e), err
}

// compareEpochs orders epochs written "<recovery generation>.<timeline>" (P6.7, P6.9) by generation, then timeline.
func compareEpochs(a, b string) (int, error) {
	pa, err := parseEpoch(a)
	if err != nil {
		return 0, err
	}
	pb, err := parseEpoch(b)
	if err != nil {
		return 0, err
	}
	for i := range pa {
		if pa[i] != pb[i] {
			if pa[i] < pb[i] {
				return -1, nil
			}
			return 1, nil
		}
	}
	return 0, nil
}

func parseEpoch(s string) ([2]uint64, error) {
	g, t, ok := strings.Cut(s, ".")
	gen, err1 := strconv.ParseUint(g, 10, 64)
	tl, err2 := strconv.ParseUint(t, 10, 64)
	if !ok || err1 != nil || err2 != nil {
		return [2]uint64{}, fmt.Errorf("projection: epoch %q is not <generation>.<timeline>", s)
	}
	return [2]uint64{gen, tl}, nil
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
