package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/work"
)

// Forwarder defaults.
const (
	DefaultLease = 10 * time.Second
	DefaultPoll  = 250 * time.Millisecond
	DefaultBatch = 500
	// ClaimConcurrency bounds the claim-check uploads of one batch in flight at once (P6.14): one at a time would
	// cap a partition at 1/upload-time events a second whatever the machine.
	ClaimConcurrency = 8
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
	mu        sync.Mutex
	held      map[streamPart]time.Time // until when this forwarder holds each partition's lease, by its own clock
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
	f.mu.Lock()
	if f.held == nil {
		f.held = map[streamPart]time.Time{}
	}
	f.mu.Unlock()
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

// run forwards on two signals (P6.29): a commit of this member wakes it for the partitions that commit wrote to, and
// the poll takes, at its own pace, every partition that has rows to forward (P6.32), for other replicas' rows and
// retries. A wake never postpones the poll: under a steady stream of commits it would never come.
func (f *Forwarder) run(ctx context.Context) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		var parts []streamPart
		poll := false
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			parts, poll = f.all(), true
		case <-f.Producer.written:
			parts = f.Producer.takeWritten()
		}
		var err error
		if poll {
			parts, err = f.pending(ctx, parts)
		}
		if err == nil {
			_, err = f.forward(ctx, parts)
		}
		if err != nil && ctx.Err() == nil {
			f.Log.Warn("forwarding events failed; retrying", "error", err.Error())
		}
		if poll {
			t.Reset(f.Poll)
		}
	}
}

// Once forwards every partition that has rows to forward, again while one had a full batch, and returns how many
// messages were published.
func (f *Forwarder) Once(ctx context.Context) (int, error) {
	f.defaults()
	parts, err := f.pending(ctx, f.all())
	if err != nil {
		return 0, err
	}
	return f.forward(ctx, parts)
}

// pending keeps the parts that have unnumbered or numbered but unpublished rows (P6.32): one read-only statement over
// the two partial indexes, so a poll of an idle member opens no numbering transaction and takes no lease. Other
// replicas' rows and rows to retry are unpublished rows, so the poll still finds them.
func (f *Forwarder) pending(ctx context.Context, parts []streamPart) ([]streamPart, error) {
	if len(parts) == 0 {
		return nil, nil
	}
	var streams []string
	seen := map[string]bool{}
	for _, sp := range parts {
		if !seen[sp.stream] {
			seen[sp.stream] = true
			streams = append(streams, sp.stream)
		}
	}
	has := map[streamPart]bool{}
	err := f.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		rows, err := tx.Query(ctx, `SELECT stream, partition FROM be_outbox WHERE seq IS NULL AND stream = ANY($1)
			UNION SELECT stream, partition FROM be_outbox WHERE seq IS NOT NULL AND published_at IS NULL AND stream = ANY($1)`,
			streams)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var sp streamPart
			if err := rows.Scan(&sp.stream, &sp.n); err != nil {
				return err
			}
			has[sp] = true
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	var out []streamPart
	for _, sp := range parts {
		if has[sp] {
			out = append(out, sp)
		}
	}
	return out, nil
}

func (f *Forwarder) all() []streamPart {
	var out []streamPart
	for _, s := range f.Producer.Streams() {
		for p := 0; p < s.Partitions; p++ {
			out = append(out, streamPart{s.Name, p})
		}
	}
	return out
}

// forward forwards parts, then again the ones whose batch was full, until none is; it returns how many messages were
// published and the first error.
func (f *Forwarder) forward(ctx context.Context, parts []streamPart) (int, error) {
	total := 0
	var firstErr error
	for len(parts) > 0 && ctx.Err() == nil {
		var full []streamPart
		for _, sp := range parts {
			n, err := f.partition(ctx, sp)
			total += n
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("%s/%d: %w", sp.stream, sp.n, err)
				}
				continue
			}
			if n == f.Batch {
				full = append(full, sp)
			}
		}
		parts = full
	}
	return total, firstErr
}

// lease takes or renews the partition's lease (P6.11), but only once half of it has passed: within its first half
// this forwarder still holds it, and the lease only reduces contention.
func (f *Forwarder) lease(ctx context.Context, sp streamPart) (bool, error) {
	f.mu.Lock()
	until, ok := f.held[sp]
	f.mu.Unlock()
	if ok && time.Until(until) > f.Lease/2 {
		return true, nil
	}
	asked := time.Now() // the database's lease ends Lease after it granted it, so no sooner than this plus Lease
	held := false
	err := f.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO be_outbox_partition (stream, partition) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, sp.stream, sp.n); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE be_outbox_partition SET lease_owner = $3,
			lease_until = clock_timestamp() + make_interval(secs => $4)
			WHERE stream = $1 AND partition = $2
			  AND (lease_until IS NULL OR lease_until < clock_timestamp() OR lease_owner = $3)`,
			sp.stream, sp.n, f.owner, f.Lease.Seconds())
		held = err == nil && tag.RowsAffected() == 1
		return err
	})
	f.mu.Lock()
	if held {
		f.held[sp] = asked.Add(f.Lease)
	} else {
		delete(f.held, sp)
	}
	f.mu.Unlock()
	return held, err
}

func (f *Forwarder) partition(ctx context.Context, sp streamPart) (int, error) {
	if held, err := f.lease(ctx, sp); err != nil || !held {
		return 0, err // another replica holds the lease: it only reduces contention (P6.11)
	}
	var rows []Row
	err := f.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		// numbering and reading what is numbered and unpublished in one transaction: one round trip fewer an event
		if _, err := numberIn(ctx, tx, sp.stream, sp.n, f.Batch); err != nil {
			return err
		}
		var err error
		rows, err = unpublishedIn(ctx, tx, sp.stream, sp.n, f.Batch)
		return err
	}, numberLimits...)
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
	prepared, stop := f.prepare(ctx, rows)
	defer stop()
	for i, r := range rows {
		p := <-prepared[i]
		if p.err != nil {
			return len(done), p.err
		}
		if err := f.Transport.Publish(ctx, p.msg); err != nil {
			return len(done), err // published later again: at least once (P6.10)
		}
		done = append(done, r.Ord)
		if p.claim {
			claimed = append(claimed, r.Ord)
		}
	}
	return len(done), nil
}

type preparedMessage struct {
	msg   Message
	claim bool
	err   error
}

// prepare builds the batch's messages in the background, each delivered on its own channel so publishing goes on in
// position order as soon as the next one is ready. Claim-check uploads run up to ClaimConcurrency at a time: they do
// not depend on each other, and only the publishing order is fixed (P6.10). stop cancels what has not started and
// waits for the uploads in flight.
func (f *Forwarder) prepare(ctx context.Context, rows []Row) ([]chan preparedMessage, func()) {
	ctx, cancel := context.WithCancel(ctx)
	out := make([]chan preparedMessage, len(rows))
	for i := range out {
		out[i] = make(chan preparedMessage, 1)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		slots := make(chan struct{}, ClaimConcurrency)
		for i, r := range rows {
			if len(r.Payload) <= MaxPayload {
				m, c, err := f.message(ctx, r)
				out[i] <- preparedMessage{m, c, err}
				continue
			}
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				for j := i; j < len(rows); j++ {
					out[j] <- preparedMessage{err: ctx.Err()}
				}
				return
			}
			wg.Add(1)
			go func(i int, r Row) {
				defer func() { <-slots; wg.Done() }()
				m, c, err := f.message(ctx, r)
				out[i] <- preparedMessage{m, c, err}
			}(i, r)
		}
	}()
	return out, func() { cancel(); wg.Wait() }
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

// unpublishedIn reads a partition's numbered, unpublished rows in position order, at most batch of them.
func unpublishedIn(ctx context.Context, tx db.Tx, stream string, p, batch int) ([]Row, error) {
	rows, err := tx.Query(ctx, `SELECT ord, event_id::text, type, occurred_at, record_id, version, projection,
		coalesce(legal_entity, ''), subjects, coalesce(causation::text, ''), hops, trace, payload, digest, seq,
		epoch_generation, epoch_timeline
		FROM be_outbox WHERE stream = $1 AND partition = $2 AND seq IS NOT NULL AND published_at IS NULL
		ORDER BY epoch_generation, epoch_timeline, seq LIMIT $3`, stream, p, batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		r := Row{Stream: stream, Partition: p}
		var subjects, trace []byte
		var proj string
		if err := rows.Scan(&r.Ord, &r.EventID, &r.Type, &r.OccurredAt, &r.RecordID, &r.Version, &proj,
			&r.LegalEntity, &subjects, &r.Causation, &r.Hops, &trace, &r.Payload, &r.Digest, &r.Seq,
			&r.Epoch.Generation, &r.Epoch.Timeline); err != nil {
			return nil, err
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
	return out, rows.Err()
}

func (f *Forwarder) markPublished(ctx context.Context, ords, claimed []int64) error {
	return f.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE be_outbox SET published_at = now(), claim = coalesce(ord = ANY($2), false)
			WHERE ord = ANY($1) AND published_at IS NULL`, ords, claimed)
		return err
	})
}

// Purge deletes published rows older than the redelivery retention, in batches (P6.12); unpublished rows are never
// deleted.
func Purge(ctx context.Context, d *db.DB, retention time.Duration) (int64, error) {
	return db.DeleteInBatches(ctx, d, `DELETE FROM be_outbox WHERE ord IN (SELECT ord FROM be_outbox
		WHERE published_at IS NOT NULL AND published_at < now() - make_interval(secs => $1)
		LIMIT $2 FOR UPDATE SKIP LOCKED)`, retention.Seconds())
}

// PurgeJob is the hourly purge every member with a database runs (P6.12): published rows past the redelivery
// retention go; unpublished rows stay.
func PurgeJob(d *db.DB) work.Job {
	return work.Job{Name: "be.outbox.purge", Kind: work.Singleton, Effect: work.Local, Interval: time.Hour,
		MaxAttempts: work.Unlimited,
		Handle: func(ctx context.Context, run *work.Run) error {
			if _, err := Purge(ctx, d, RedeliveryRetention); err != nil {
				return err
			}
			return run.Complete(ctx, nil)
		}}
}
