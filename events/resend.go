package events

import (
	"context"
	"errors"
	"log/slog"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/jackc/pgx/v5"
)

// Resender answers be.source.v1.ResendService for a member's streams (P6.38): the published rows of each partition
// after the asked position, again to the transport in position order, with the event's identity and headers and a
// message identity of their own.
type Resender struct {
	sourcepb.UnimplementedResendServiceServer
	DB        *db.DB
	Producer  *Producer
	Transport Transport
	Source    string // the member's component ID
	Impl      string // IMPLEMENTATION_INSTANCE
	Log       *slog.Logger
	Batch     int
}

// Resend implements be.source.v1.ResendService.
func (r *Resender) Resend(ctx context.Context, req *sourcepb.ResendRequest) (*sourcepb.ResendResponse, error) {
	var st *Stream
	for _, s := range r.Producer.Streams() {
		if s.Name == req.GetStream() {
			st = &s
			break
		}
	}
	if st == nil {
		return nil, errs.New(errs.ReasonRequestInvalid, "no such stream: "+req.GetStream())
	}
	request := req.GetRequestId()
	if request == "" {
		request = ids.New().String()
	}
	from := map[int]*Point{}
	for _, f := range req.GetFrom() {
		if int(f.GetPartition()) >= st.Partitions {
			return nil, errs.New(errs.ReasonRequestInvalid, "no such partition")
		}
		a := f.GetAfter()
		if a == nil || a.GetImpl() != r.Impl {
			continue // another implementation instance's position: from the first row kept
		}
		e, err := ParseEpoch(a.GetEpoch())
		if err != nil {
			return nil, errs.New(errs.ReasonRequestInvalid, "an epoch is <generation>.<timeline>")
		}
		from[int(f.GetPartition())] = &Point{Epoch: e, Seq: int64(a.GetSeq())}
	}
	out := &sourcepb.ResendResponse{}
	for n := 0; n < st.Partitions; n++ {
		after := from[n]
		beyond, ahead, err := r.judge(ctx, st.Name, n, after)
		if err != nil {
			return nil, err
		}
		if beyond {
			out.BeyondRetention = append(out.BeyondRetention, uint32(n))
		}
		if ahead {
			out.Ahead = append(out.Ahead, uint32(n)) // nothing after it to send: the consumer saw what is not here
			continue
		}
		sent, err := r.partition(ctx, st.Name, n, after, request)
		out.Resent += uint64(sent)
		if err != nil {
			r.log().Error("a redelivery stopped", "event", "event_resend_failed", "stream", st.Name, "partition", n,
				"resent", sent, "error", err.Error())
			return nil, errs.New(errs.ReasonDependencyUnavailable, "the redelivery stopped (the transport or the database): ask again")
		}
	}
	r.log().Info("redelivered", "event", "event_resent", "stream", st.Name, "request", request, "resent", out.Resent,
		"beyond_retention", out.BeyondRetention, "ahead", out.Ahead)
	return out, nil
}

func (r *Resender) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.New(slog.DiscardHandler)
}

// judge reads whether rows after the start were purged (beyond) and whether the start is past what this partition
// has (ahead: past the fork point of its epoch, past the counter of the current one, or an epoch it never had).
func (r *Resender) judge(ctx context.Context, stream string, n int, after *Point) (beyond, ahead bool, err error) {
	err = r.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		var last int64
		var gen, tl, pg, pt, ps *int64
		err := tx.QueryRow(ctx, `SELECT last_seq, epoch_generation, epoch_timeline, purged_generation, purged_timeline,
			purged_seq FROM be_outbox_partition WHERE stream = $1 AND partition = $2`, stream, n).
			Scan(&last, &gen, &tl, &pg, &pt, &ps)
		if errors.Is(err, pgx.ErrNoRows) {
			ahead = after != nil // nothing ever numbered here
			return nil
		}
		if err != nil {
			return err
		}
		if ps != nil {
			purged := Point{Epoch: Epoch{Generation: *pg, Timeline: *pt}, Seq: *ps}
			beyond = after == nil || after.Less(purged)
		}
		if after == nil {
			return nil
		}
		if gen != nil && after.Epoch == (Epoch{Generation: *gen, Timeline: *tl}) {
			ahead = after.Seq > last
			return nil
		}
		var fork int64
		err = tx.QueryRow(ctx, `SELECT fork_seq FROM be_epoch_history WHERE stream = $1 AND partition = $2
			AND old_generation = $3 AND old_timeline = $4`, stream, n, after.Epoch.Generation, after.Epoch.Timeline).Scan(&fork)
		if errors.Is(err, pgx.ErrNoRows) {
			ahead = true // an epoch this partition never had
			return nil
		}
		if err != nil {
			return err
		}
		ahead = after.Seq > fork
		return nil
	})
	return beyond, ahead, err
}

// partition sends one partition's published rows after the start, a batch at a time, in position order.
func (r *Resender) partition(ctx context.Context, stream string, n int, after *Point, request string) (int, error) {
	batch := r.Batch
	if batch <= 0 {
		batch = DefaultBatch
	}
	sent := 0
	for {
		var rows []Row
		err := r.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			var q pgx.Rows
			var err error
			cols := `SELECT ord, event_id::text, type, occurred_at, record_id, version, projection, coalesce(legal_entity, ''),
				subjects, coalesce(causation::text, ''), hops, trace, payload, digest, seq, epoch_generation, epoch_timeline
				FROM be_outbox WHERE stream = $1 AND partition = $2 AND published_at IS NOT NULL`
			if after == nil {
				q, err = tx.Query(ctx, cols+` ORDER BY epoch_generation, epoch_timeline, seq LIMIT $3`, stream, n, batch)
			} else {
				q, err = tx.Query(ctx, cols+` AND (epoch_generation, epoch_timeline, seq) > ($3, $4, $5)
					ORDER BY epoch_generation, epoch_timeline, seq LIMIT $6`, stream, n, after.Epoch.Generation,
					after.Epoch.Timeline, after.Seq, batch)
			}
			if err != nil {
				return err
			}
			rows, err = scanRows(q, stream, n)
			return err
		})
		if err != nil {
			return sent, err
		}
		for _, row := range rows {
			m, _, err := r.Producer.message(ctx, row, r.Source, r.Impl)
			if err != nil {
				return sent, err
			}
			m.MsgID = row.EventID + "/resend/" + request
			if err := r.Transport.Publish(ctx, m); err != nil {
				return sent, err
			}
			sent++
			after = &Point{Epoch: row.Epoch, Seq: row.Seq}
		}
		if len(rows) < batch {
			return sent, nil
		}
	}
}
