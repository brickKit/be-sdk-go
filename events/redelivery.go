package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
)

// Redelivery is a member's consumer side after its database went back (P6.39, P6.40): it notices from the database
// epoch, asks every producer it has seen for a redelivery from the smallest progress of its subscriptions, and
// records what a redelivery cannot cover.
type Redelivery struct {
	DB   *db.DB
	Subs []Sub
	// Conn is the producing component's system surface (the dependency's address); false: none is configured.
	Conn     func(component string) (grpc.ClientConnInterface, bool)
	Lifetime time.Duration // P6.37's L; zero: DeliveryLifetime
	Log      *slog.Logger
}

func (r *Redelivery) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.New(slog.DiscardHandler)
}

// WentBack compares the database's epoch with the one the consumer side last ran on (be_consumer_epoch). The first
// start records the current one and reports false.
func (r *Redelivery) WentBack(ctx context.Context) (bool, Epoch, error) {
	var back bool
	var cur Epoch
	err := r.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").
			Scan(&cur.Generation, &cur.Timeline); err != nil {
			return errors.Join(ErrNoEpoch, err)
		}
		var last Epoch
		err := tx.QueryRow(ctx, "SELECT epoch_generation, epoch_timeline FROM be_consumer_epoch").
			Scan(&last.Generation, &last.Timeline)
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = tx.Exec(ctx, `INSERT INTO be_consumer_epoch (epoch_generation, epoch_timeline) VALUES ($1, $2)
				ON CONFLICT DO NOTHING`, cur.Generation, cur.Timeline)
			return err
		}
		if err != nil {
			return err
		}
		back = last != cur
		return nil
	})
	return back, cur, err
}

// Settle records the epoch the consumer side now runs on, once every producer answered.
func (r *Redelivery) Settle(ctx context.Context, e Epoch) error {
	return r.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE be_consumer_epoch SET epoch_generation = $1, epoch_timeline = $2, updated_at = now()`,
			e.Generation, e.Timeline)
		return err
	})
}

// ResendCallTimeout bounds one call for a redelivery: the producer may send a partition's whole retention again.
const ResendCallTimeout = 10 * time.Minute

type sourceStream struct{ component, stream string }

// Request asks each producer seen in the inbox for a redelivery of each stream, every partition from the smallest
// progress of the subscriptions that read it (a partition some subscription has no progress on, or where they saw
// different implementation instances, from the first row kept), and records the partitions it cannot cover. It
// returns an error when some producer did not answer: ask again later.
func (r *Redelivery) Request(ctx context.Context) error {
	lifetime := r.Lifetime
	if lifetime <= 0 {
		lifetime = DeliveryLifetime
	}
	seen := map[sourceStream][]string{} // the subscriptions that read each source stream
	err := r.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `SELECT DISTINCT subscription, source_component, source_stream FROM be_inbox
			WHERE source_component IS NOT NULL AND seq IS NOT NULL`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var sub string
			var ss sourceStream
			if err := rows.Scan(&sub, &ss.component, &ss.stream); err != nil {
				return err
			}
			seen[ss] = append(seen[ss], sub)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	progress := map[string]map[SourceKey]Point{}
	for _, subs := range seen {
		for _, s := range subs {
			if _, done := progress[s]; done {
				continue
			}
			if progress[s], err = Progress(ctx, r.DB, s, lifetime); err != nil {
				return err
			}
		}
	}
	var failed error
	for ss, subs := range seen {
		if err := r.one(ctx, ss, subs, progress); err != nil {
			failed = errors.Join(failed, err)
		}
	}
	return failed
}

func (r *Redelivery) one(ctx context.Context, ss sourceStream, subs []string, progress map[string]map[SourceKey]Point) error {
	req := &sourcepb.ResendRequest{Stream: ss.stream, RequestId: ids.New().String(), From: fromOf(ss, subs, progress)}
	conn, ok := r.Conn(ss.component)
	if !ok {
		r.log().Error("a producer cannot be asked for a redelivery: no address for it", "event", "event_resend_unreachable",
			"component", ss.component, "stream", ss.stream)
		return nil // the recovery tool asks for it (P6.39); asking again would not change it
	}
	resp := &sourcepb.ResendResponse{}
	cctx, cancel := context.WithTimeout(ctx, ResendCallTimeout) // calls need a deadline (P4.4)
	defer cancel()
	if err := conn.Invoke(cctx, sourcepb.ResendService_Resend_FullMethodName, req, resp); err != nil {
		return fmt.Errorf("the redelivery from %s (%s): %w", ss.component, ss.stream, err)
	}
	r.log().Info("a producer redelivered", "event", "event_resend_requested", "component", ss.component,
		"stream", ss.stream, "resent", resp.GetResent(), "beyond_retention", resp.GetBeyondRetention(), "ahead", resp.GetAhead())
	return r.gaps(ctx, ss, subs, progress, resp)
}

// fromOf is where each partition's redelivery starts (P6.39): the smallest progress of the subscriptions that read the
// source stream, when every one of them has one there on the same implementation instance; the other partitions are
// left out, so they start at the first row kept.
func fromOf(ss sourceStream, subs []string, progress map[string]map[SourceKey]Point) []*sourcepb.PartitionFrom {
	type start struct {
		impl  string
		point Point
	}
	starts := map[int]*start{}
	open := map[int]bool{}
	for i, s := range subs {
		mine := map[int]bool{}
		for k, p := range progress[s] {
			if k.Component != ss.component || k.Stream != ss.stream {
				continue
			}
			mine[k.Partition] = true
			cur, ok := starts[k.Partition]
			switch {
			case open[k.Partition]:
			case i > 0 && !ok: // an earlier subscription has none here: no start, so from the first row kept
			case !ok:
				starts[k.Partition] = &start{impl: k.Impl, point: p}
			case cur.impl != k.Impl:
				open[k.Partition] = true
			case p.Less(cur.point):
				cur.point = p
			}
		}
		for n := range starts {
			if !mine[n] {
				open[n] = true
			}
		}
	}
	parts := make([]int, 0, len(starts))
	for n := range starts {
		if !open[n] {
			parts = append(parts, n)
		}
	}
	sort.Ints(parts)
	var out []*sourcepb.PartitionFrom
	for _, n := range parts {
		st := starts[n]
		out = append(out, &sourcepb.PartitionFrom{Partition: uint32(n), After: &sourcepb.Position{
			Impl: st.impl, Epoch: st.point.Epoch.String(), Seq: uint64(st.point.Seq)}})
	}
	return out
}

// gaps records the partitions a redelivery could not cover, for every subscription that reads them (P6.40), and
// repairs a read model whose subscription says how.
func (r *Redelivery) gaps(ctx context.Context, ss sourceStream, subs []string, progress map[string]map[SourceKey]Point,
	resp *sourcepb.ResendResponse) error {
	byName := map[string]Sub{}
	for _, s := range r.Subs {
		byName[s.Name] = s
	}
	record := func(kind string, n int) error {
		for _, s := range subs {
			var at any
			key := SourceKey{Component: ss.component, Stream: ss.stream, Partition: n}
			for k, p := range progress[s] {
				if k.Component == ss.component && k.Stream == ss.stream && k.Partition == n {
					at, key = k.Impl+"/"+p.String(), k
				}
			}
			if err := r.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO be_resend_gap (subscription, source_component, source_stream,
					source_partition, kind, progress) VALUES ($1, $2, $3, $4, $5, $6)`, s, ss.component, ss.stream, n, kind, at)
				return err
			}); err != nil {
				return err
			}
			event := "event_resend_beyond_retention"
			if kind == "ahead" {
				event = "event_resend_consumer_ahead"
			}
			r.log().Error("a redelivery cannot cover a subscription's progress", "event", event, "subscription", s,
				"component", ss.component, "stream", ss.stream, "partition", strconv.Itoa(n), "progress", at)
			if sub := byName[s]; kind == "beyond_retention" && sub.Repair != nil {
				if err := sub.Repair(ctx, key); err != nil {
					r.log().Error("a read model could not be repaired from its source", "event", "event_resend_repair_failed",
						"subscription", s, "component", ss.component, "error", err.Error())
				}
			}
		}
		return nil
	}
	for _, n := range resp.GetBeyondRetention() {
		if err := record("beyond_retention", int(n)); err != nil {
			return err
		}
	}
	for _, n := range resp.GetAhead() {
		if err := record("ahead", int(n)); err != nil {
			return err
		}
	}
	return nil
}

// Run is the start-up check (P6.39): when the database went back, ask the producers until every one answered, then
// record the epoch; the subscriptions receive meanwhile (the inbox removes duplicates).
func (r *Redelivery) Run(ctx context.Context) {
	back, cur, err := r.WentBack(ctx)
	for err != nil {
		r.log().Warn("the consumer's database epoch could not be read; trying again", "error", err.Error())
		if !sleep(ctx, 5*time.Second) {
			return
		}
		back, cur, err = r.WentBack(ctx)
	}
	if !back {
		return
	}
	r.log().Warn("the database went back since the consumer side last ran: asking the producers for a redelivery",
		"event", "event_resend_needed", "epoch", cur.String())
	wait := time.Second
	for {
		err := r.Request(ctx)
		if err == nil {
			break
		}
		r.log().Warn("a redelivery was not answered; asking again", "error", err.Error(), "in", wait.String())
		if !sleep(ctx, wait) {
			return
		}
		wait = min(2*wait, time.Minute)
	}
	if err := r.Settle(ctx, cur); err != nil {
		r.log().Error("the consumer's epoch could not be recorded: the next start asks again", "error", err.Error())
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
