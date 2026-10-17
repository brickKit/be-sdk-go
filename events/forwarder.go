package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
)

// Forwarder defaults.
const (
	DefaultLease = 10 * time.Second
	DefaultPoll  = 250 * time.Millisecond
	DefaultBatch = 500
)

// Forwarder numbers and publishes a member's outbox (P6.10, P6.11).
type Forwarder struct {
	DB        *db.DB
	Producer  *Producer
	Transport Transport
	Source    string // the member's component ID
	Impl      string // IMPLEMENTATION_INSTANCE
	Log       *slog.Logger
	Lease     time.Duration
	Poll      time.Duration
	Batch     int
	owner     string
}

func (f *Forwarder) defaults() {
	if f.Lease <= 0 {
		f.Lease = DefaultLease
	}
	if f.Poll <= 0 {
		f.Poll = DefaultPoll
	}
	if f.Batch <= 0 {
		f.Batch = DefaultBatch
	}
	if f.owner == "" {
		f.owner = ids.New().String()
	}
}

// Start creates the streams' storage, then forwards in the background until ctx ends.
func (f *Forwarder) Start(ctx context.Context) error {
	f.defaults()
	for _, s := range f.Producer.Streams() {
		if err := f.Transport.EnsureStream(ctx, s); err != nil {
			return fmt.Errorf("event stream %s: %w", s.Name, err)
		}
	}
	go f.run(ctx)
	return nil
}

func (f *Forwarder) run(ctx context.Context) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := f.Once(ctx)
		if err != nil && ctx.Err() == nil {
			f.Log.Warn("forwarding events failed; retrying", "error", err.Error())
		}
		if n == 0 || err != nil {
			t.Reset(f.Poll)
		} else {
			t.Reset(0)
		}
	}
}

// Once forwards every partition once and returns how many messages were published.
func (f *Forwarder) Once(ctx context.Context) (int, error) {
	f.defaults()
	total := 0
	var firstErr error
	for _, s := range f.Producer.Streams() {
		for p := 0; p < s.Partitions; p++ {
			n, err := f.partition(ctx, s, p)
			total += n
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("%s/%d: %w", s.Name, p, err)
			}
		}
	}
	return total, firstErr
}

func (f *Forwarder) partition(ctx context.Context, s Stream, p int) (int, error) {
	held := false
	err := f.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO be_outbox_partition (stream, partition) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, s.Name, p); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE be_outbox_partition SET lease_owner = $3,
			lease_until = clock_timestamp() + make_interval(secs => $4)
			WHERE stream = $1 AND partition = $2
			  AND (lease_until IS NULL OR lease_until < clock_timestamp() OR lease_owner = $3)`,
			s.Name, p, f.owner, f.Lease.Seconds())
		held = err == nil && tag.RowsAffected() == 1
		return err
	})
	if err != nil || !held {
		return 0, err // another replica holds the lease: it only reduces contention (P6.11)
	}
	if _, err := Number(ctx, f.DB, s.Name, p, f.Batch); err != nil {
		return 0, err
	}
	rows, err := f.unpublished(ctx, s.Name, p)
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	var done []int64
	var claimed []int64
	defer func() {
		if len(done) > 0 {
			_ = f.markPublished(context.WithoutCancel(ctx), done, claimed)
		}
	}()
	for _, r := range rows {
		m, isClaim, err := f.message(ctx, r)
		if err != nil {
			return len(done), err
		}
		if err := f.Transport.Publish(ctx, m); err != nil {
			return len(done), err // published later again: at least once (P6.10)
		}
		done = append(done, r.Ord)
		if isClaim {
			claimed = append(claimed, r.Ord)
		}
	}
	return len(done), nil
}

func (f *Forwarder) message(ctx context.Context, r Row) (Message, bool, error) {
	payload, claim := r.Payload, false
	if len(payload) > MaxPayload {
		if f.Producer.claims == nil {
			return Message{}, false, fmt.Errorf("event %s: %d bytes and no object storage for a claim check", r.EventID, len(payload))
		}
		uri, err := f.Producer.claims.Put(ctx, r.EventID, payload)
		if err != nil {
			return Message{}, false, fmt.Errorf("event %s: claim check: %w", r.EventID, err)
		}
		ref := ClaimRef{URI: uri, Size: len(payload), SHA256: sha256Hex(payload)}
		payload, _ = json.Marshal(map[string]ClaimRef{"claim": ref})
		claim = true
	}
	r.Claim = claim
	return Message{Subject: Subject(r.Stream, r.Partition, r.Type), Headers: r.Headers(f.Source, f.Impl), Data: payload}, claim, nil
}

func (f *Forwarder) unpublished(ctx context.Context, stream string, p int) ([]Row, error) {
	var out []Row
	err := f.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		out = out[:0]
		rows, err := tx.Query(ctx, `SELECT ord, event_id::text, type, occurred_at, record_id, version, projection,
			coalesce(legal_entity, ''), subjects, coalesce(causation::text, ''), hops, trace, payload, digest, seq,
			epoch_generation, epoch_timeline
			FROM be_outbox WHERE stream = $1 AND partition = $2 AND seq IS NOT NULL AND published_at IS NULL
			ORDER BY epoch_generation, epoch_timeline, seq LIMIT $3`, stream, p, f.Batch)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			r := Row{Stream: stream, Partition: p}
			var subjects, trace []byte
			var proj string
			if err := rows.Scan(&r.Ord, &r.EventID, &r.Type, &r.OccurredAt, &r.RecordID, &r.Version, &proj,
				&r.LegalEntity, &subjects, &r.Causation, &r.Hops, &trace, &r.Payload, &r.Digest, &r.Seq,
				&r.Epoch.Generation, &r.Epoch.Timeline); err != nil {
				return err
			}
			r.Projection = Projection(proj)
			if subjects != nil {
				_ = json.Unmarshal(subjects, &r.Subjects)
			}
			if trace != nil {
				_ = json.Unmarshal(trace, &r.Trace)
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}

func (f *Forwarder) markPublished(ctx context.Context, ords, claimed []int64) error {
	return f.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE be_outbox SET published_at = now(), claim = coalesce(ord = ANY($2), false)
			WHERE ord = ANY($1) AND published_at IS NULL`, ords, claimed)
		return err
	})
}

// Purge deletes published rows older than the redelivery retention (P6.12); unpublished rows are never deleted.
func Purge(ctx context.Context, d *db.DB, retention time.Duration) (int64, error) {
	var n int64
	err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM be_outbox WHERE published_at IS NOT NULL
			AND published_at < now() - make_interval(secs => $1)`, retention.Seconds())
		n = tag.RowsAffected()
		return err
	})
	return n, err
}
