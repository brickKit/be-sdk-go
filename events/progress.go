package events

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/jackc/pgx/v5"
)

// DeliveryLifetime is P6.37's L: within it, counted in running time, a message delivered for the first time has an
// outcome (handled, a dead letter, or not this subscription's). It is at least the acknowledgement wait times the
// delivery limit plus the backoffs between them (P6.19, P6.20).
const DeliveryLifetime = 10 * time.Minute

// SourceKey is one partition of a source stream, as an inbox row records it (P6.36).
type SourceKey struct {
	Component string // the producing component (the envelope's source without urn:be:)
	Impl      string // its implementation instance
	Stream    string
	Partition int
}

// Point is a place on one partition within one implementation instance (the watermark's Position carries the
// instance and the epoch as written): the epoch, then the sequence.
type Point struct {
	Epoch Epoch
	Seq   int64
}

// Less orders positions: the epoch (generation, then timeline), then the sequence.
func (p Point) Less(q Point) bool {
	if p.Epoch.Generation != q.Epoch.Generation {
		return p.Epoch.Generation < q.Epoch.Generation
	}
	if p.Epoch.Timeline != q.Epoch.Timeline {
		return p.Epoch.Timeline < q.Epoch.Timeline
	}
	return p.Seq < q.Seq
}

func (p Point) String() string { return p.Epoch.String() + "/" + strconv.FormatInt(p.Seq, 10) }

// Handled is what the progress needs of one inbox row: its source partition, its position (nil for a row written
// before positions were kept) and when it was handled.
type Handled struct {
	Key SourceKey
	Pos *Point
	At  time.Time
}

// ProgressOf is P6.37 over a subscription's run starts and inbox rows: the cut is the last handling of the latest run
// that lasted the lifetime from its start, less the lifetime; the progress of each source partition is the largest
// position handled at or before the cut. No cut, no progress: redeliver from the first row kept.
func ProgressOf(runs []time.Time, rows []Handled, lifetime time.Duration) map[SourceKey]Point {
	at := make([]time.Time, len(rows))
	for i, r := range rows {
		at[i] = r.At
	}
	cut, ok := cutOf(runs, func(from, to time.Time) (time.Time, bool) {
		var last time.Time
		found := false
		for _, t := range at {
			if !t.Before(from) && (to.IsZero() || t.Before(to)) && (!found || t.After(last)) {
				last, found = t, true
			}
		}
		return last, found
	}, lifetime)
	out := map[SourceKey]Point{}
	if !ok {
		return out
	}
	for _, r := range rows {
		if r.Pos == nil || r.At.After(cut) {
			continue
		}
		if cur, seen := out[r.Key]; !seen || cur.Less(*r.Pos) {
			out[r.Key] = *r.Pos
		}
	}
	return out
}

// cutOf finds P6.37's cut: lastIn answers the last handling in [from, to) (to zero: no end).
func cutOf(runs []time.Time, lastIn func(from, to time.Time) (time.Time, bool), lifetime time.Duration) (time.Time, bool) {
	starts := append([]time.Time(nil), runs...)
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	for k := len(starts) - 1; k >= 0; k-- {
		var to time.Time
		if k+1 < len(starts) {
			to = starts[k+1]
		}
		last, ok := lastIn(starts[k], to)
		if ok && !starts[k].After(last.Add(-lifetime)) {
			return last.Add(-lifetime), true
		}
	}
	return time.Time{}, false
}

// StartRun records that an instance of a subscription starts receiving (P6.36): its runs bound the cut.
func StartRun(ctx context.Context, d *db.DB, subscription string) error {
	return d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO be_subscription_run (subscription) VALUES ($1)`, subscription)
		return err
	})
}

// Progress reads a subscription's progress from its database (P6.37): the runs, the last handling in each, then the
// largest position per source partition at or before the cut. The inbox has no index for it: read at a recovery.
func Progress(ctx context.Context, d *db.DB, subscription string, lifetime time.Duration) (map[SourceKey]Point, error) {
	out := map[SourceKey]Point{}
	err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var runs []time.Time
		rows, err := tx.Query(ctx, `SELECT started_at FROM be_subscription_run WHERE subscription = $1 ORDER BY started_at`,
			subscription)
		if err != nil {
			return err
		}
		runs, err = pgx.CollectRows(rows, pgx.RowTo[time.Time])
		if err != nil {
			return err
		}
		var lastErr error
		cut, ok := cutOf(runs, func(from, to time.Time) (time.Time, bool) {
			var last *time.Time
			var e error
			if to.IsZero() {
				e = tx.QueryRow(ctx, `SELECT max(processed_at) FROM be_inbox WHERE subscription = $1 AND processed_at >= $2`,
					subscription, from).Scan(&last)
			} else {
				e = tx.QueryRow(ctx, `SELECT max(processed_at) FROM be_inbox WHERE subscription = $1 AND processed_at >= $2
					AND processed_at < $3`, subscription, from, to).Scan(&last)
			}
			if e != nil {
				lastErr = e
				return time.Time{}, false
			}
			if last == nil {
				return time.Time{}, false
			}
			return *last, true
		}, lifetime)
		if lastErr != nil {
			return lastErr
		}
		if !ok {
			return nil
		}
		rows, err = tx.Query(ctx, `SELECT DISTINCT ON (source_component, source_impl, source_stream, source_partition)
			source_component, source_impl, source_stream, source_partition, epoch_generation, epoch_timeline, seq
			FROM be_inbox WHERE subscription = $1 AND seq IS NOT NULL AND processed_at <= $2
			ORDER BY source_component, source_impl, source_stream, source_partition,
				epoch_generation DESC, epoch_timeline DESC, seq DESC`, subscription, cut)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var k SourceKey
			var p Point
			if err := rows.Scan(&k.Component, &k.Impl, &k.Stream, &k.Partition, &p.Epoch.Generation, &p.Epoch.Timeline,
				&p.Seq); err != nil {
				return err
			}
			out[k] = p
		}
		return rows.Err()
	})
	return out, err
}

// sourceOf is the producing component of an envelope's source (urn:be:<component>).
func sourceOf(source string) string { return strings.TrimPrefix(source, "urn:be:") }
