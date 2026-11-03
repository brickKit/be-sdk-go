package events

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
)

// RecordPage is how many records one Read asks for and one Snapshot page returns at most (P9.31, P9.32).
const RecordPage = 500

// Records declares a stream's batch read and snapshot (P9.30): who may read, and two read functions over the
// member's own tables. Both run inside the SDK's transaction, at REPEATABLE READ, so the partitions' high points
// describe exactly what they read.
type Records struct {
	// Operation is the operation both methods perform, declared in the component's resource definition.
	Operation string
	// Readers are the components (service identities) that may read; a watermark stream lists its pullers among them.
	Readers []string
	// Read answers every ID, in order: exists (version and data), tombstone (the deleting version) or purged.
	Read func(ctx context.Context, tx db.Tx, ids []string) ([]ReadResult, error)
	// Page returns up to limit existing records whose ID is greater than after (bytes), by ID ascending.
	Page func(ctx context.Context, tx db.Tx, after string, limit int) ([]Record, error)
}

// Record is one existing record; Data is what its current full-state event carries (encoded the same way).
type Record struct {
	ID      string
	Version int64
	Data    any
}

// States of a read result (P9.31).
const (
	Exists    = "exists"
	Tombstone = "tombstone"
	Purged    = "purged" // the tombstone is gone too, or the record never existed
)

// ReadResult is one ID's answer.
type ReadResult struct {
	ID      string
	State   string
	Version int64
	Data    any // Exists only
}

// RecordSource answers the batch reads and snapshots of a member's streams (be.source.v1.RecordService).
type RecordSource struct {
	sourcepb.UnimplementedRecordServiceServer
	DB         *db.DB
	Producer   *Producer
	Impl       string // IMPLEMENTATION_INSTANCE
	Deployment string // DEPLOYMENT_ID
	Log        *slog.Logger
}

// Method is the service's system-surface declaration: the streams' operation and every stream's readers; false when
// the member declares no reads. Streams declaring different operations are refused.
func (r *RecordSource) Method() (grpcx.Method, bool, error) {
	var m grpcx.Method
	seen := map[string]bool{}
	for _, s := range r.Producer.Streams() {
		if s.Records == nil {
			continue
		}
		if m.Operation != "" && m.Operation != s.Records.Operation {
			return m, false, fmt.Errorf("events: streams declare different record read operations (%s, %s)",
				m.Operation, s.Records.Operation)
		}
		m.Operation = s.Records.Operation
		for _, c := range s.Records.Readers {
			if !seen[c] {
				seen[c] = true
				m.Callers = append(m.Callers, c)
			}
		}
	}
	return m, m.Operation != "", nil
}

// stream finds the declared stream and checks the caller against its readers.
func (r *RecordSource) stream(ctx context.Context, name, method string) (Stream, error) {
	caller, _ := grpcx.Caller(ctx)
	s, ok := r.Producer.streams[name]
	if !ok || s.Records == nil {
		return s, errs.New(errs.ReasonNotFound, fmt.Sprintf("no stream %q with record reads", name))
	}
	if !contains(s.Records.Readers, caller) {
		return s, errs.New(errs.ReasonPermissionDenied, fmt.Sprintf("%s may not read %s", caller, s.Name))
	}
	r.Log.Info("record read", "event", "record_read", "caller", caller, "stream", s.Name, "method", method)
	return s, nil
}

// snapshotOf is what one read transaction declares: who answered, the epoch and the partitions' high points (P9.18).
type snapshotOf struct {
	decl  *sourcepb.Declaration
	epoch Epoch
	highs []*sourcepb.PartitionHigh
}

func (r *RecordSource) declare(ctx context.Context, tx db.Tx, s Stream) (snapshotOf, error) {
	var out snapshotOf
	var primary bool
	var system string
	if err := tx.QueryRow(ctx, "SELECT NOT pg_is_in_recovery(), (SELECT system_identifier::text FROM pg_control_system())").
		Scan(&primary, &system); err != nil {
		return out, err
	}
	if err := tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").
		Scan(&out.epoch.Generation, &out.epoch.Timeline); err != nil {
		return out, errors.Join(ErrNoEpoch, err)
	}
	out.decl = &sourcepb.Declaration{Deployment: r.Deployment, Impl: r.Impl, SystemId: system,
		Epoch: out.epoch.String(), Primary: primary}
	rows, err := tx.Query(ctx, `SELECT g, coalesce((SELECT last_seq FROM be_outbox_partition p WHERE p.stream = $1
			AND p.partition = g AND p.epoch_generation = $3 AND p.epoch_timeline = $4), 0)
		FROM generate_series(0, $2 - 1) g ORDER BY g`, s.Name, s.Partitions, out.epoch.Generation, out.epoch.Timeline)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var n uint32
		var seq int64
		if err := rows.Scan(&n, &seq); err != nil {
			return out, err
		}
		out.highs = append(out.highs, &sourcepb.PartitionHigh{Partition: n, Seq: uint64(seq)})
	}
	return out, rows.Err()
}

// payload encodes a record's data as the producer encodes an event's; a big one goes to object storage and the
// record carries the claim check (P6.14).
func (r *RecordSource) payload(ctx context.Context, s Stream, id string, version int64, data any) ([]byte, bool, error) {
	b, err := json.Marshal(data)
	if err != nil || len(b) <= MaxPayload {
		return b, false, err
	}
	if r.Producer.claims == nil {
		return nil, false, fmt.Errorf("record %s of %s: %d bytes and no object storage for a claim check", id, s.Name, len(b))
	}
	uri, err := r.Producer.claims.Put(ctx, fmt.Sprintf("record-%s-%s-%d-%s", s.Name, id, version, sha256Hex(b)[:16]), b)
	if err != nil {
		return nil, false, fmt.Errorf("record %s of %s: claim check: %w", id, s.Name, err)
	}
	ref, err := json.Marshal(map[string]ClaimRef{"claim": {URI: uri, Size: len(b), SHA256: sha256Hex(b)}})
	return ref, true, err
}

// Read is the batch read (P9.31).
func (r *RecordSource) Read(ctx context.Context, req *sourcepb.ReadRequest) (*sourcepb.ReadResponse, error) {
	s, err := r.stream(ctx, req.GetStream(), "Read")
	if err != nil {
		return nil, err
	}
	ids := req.GetRecordIds()
	if len(ids) == 0 || len(ids) > RecordPage {
		return nil, errs.New(errs.ReasonRequestInvalid, fmt.Sprintf("a read asks for 1-%d record IDs", RecordPage))
	}
	var snap snapshotOf
	var results []ReadResult
	if err := r.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if snap, err = r.declare(ctx, tx, s); err != nil {
			return err
		}
		results, err = s.Records.Read(ctx, tx, ids)
		return err
	}, db.Snapshot()); err != nil {
		return nil, err
	}
	if len(results) != len(ids) {
		return nil, fmt.Errorf("stream %s: the read answered %d of %d IDs", s.Name, len(results), len(ids))
	}
	resp := &sourcepb.ReadResponse{Source: snap.decl, Highs: snap.highs}
	for i, res := range results {
		if res.ID != ids[i] {
			return nil, fmt.Errorf("stream %s: the read answered %s where %s was asked", s.Name, res.ID, ids[i])
		}
		out := &sourcepb.ReadResult{RecordId: res.ID, Version: res.Version}
		switch res.State {
		case Exists:
			out.State = sourcepb.ReadState_READ_STATE_EXISTS
			if out.Payload, out.Claim, err = r.payload(ctx, s, res.ID, res.Version, res.Data); err != nil {
				return nil, err
			}
		case Tombstone:
			out.State = sourcepb.ReadState_READ_STATE_TOMBSTONE
		case Purged:
			out.State, out.Version = sourcepb.ReadState_READ_STATE_PURGED, 0
		default:
			return nil, fmt.Errorf("stream %s: record %s read as %q", s.Name, res.ID, res.State)
		}
		resp.Results = append(resp.Results, out)
	}
	return resp, nil
}

// snapshotToken is a later page: after which ID, bound to the instance and epoch of the first page (P9.32).
type snapshotToken struct {
	Stream string `json:"s"`
	After  string `json:"a"`
	Impl   string `json:"i"`
	Epoch  Epoch  `json:"e"`
}

// Snapshot is one page of every existing record (P9.32).
func (r *RecordSource) Snapshot(ctx context.Context, req *sourcepb.SnapshotRequest) (*sourcepb.SnapshotResponse, error) {
	var tok snapshotToken
	if req.GetPageToken() != "" {
		raw, err := base64.RawURLEncoding.DecodeString(req.GetPageToken())
		if err != nil || json.Unmarshal(raw, &tok) != nil || tok.Stream == "" {
			return nil, errs.New(errs.ReasonRequestInvalid, "page token")
		}
		if req.GetStream() != "" && req.GetStream() != tok.Stream {
			return nil, errs.New(errs.ReasonRequestInvalid, "the page token is for another stream")
		}
	} else {
		tok.Stream = req.GetStream()
	}
	s, err := r.stream(ctx, tok.Stream, "Snapshot")
	if err != nil {
		return nil, err
	}
	var snap snapshotOf
	var recs []Record
	if err := r.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if snap, err = r.declare(ctx, tx, s); err != nil {
			return err
		}
		if tok.Impl != "" && (tok.Impl != r.Impl || tok.Epoch != snap.epoch) {
			return errs.New(errs.ReasonQueryPlanChanged, "the source's implementation instance or epoch changed: align again")
		}
		recs, err = s.Records.Page(ctx, tx, tok.After, RecordPage+1)
		return err
	}, db.Snapshot()); err != nil {
		return nil, err
	}
	resp := &sourcepb.SnapshotResponse{Source: snap.decl, Highs: snap.highs}
	more := len(recs) > RecordPage
	if more {
		recs = recs[:RecordPage]
	}
	last := tok.After
	for _, rec := range recs {
		if rec.ID <= last {
			return nil, fmt.Errorf("stream %s: the page is not by record ID ascending (%s after %s)", s.Name, rec.ID, last)
		}
		last = rec.ID
		p, claim, err := r.payload(ctx, s, rec.ID, rec.Version, rec.Data)
		if err != nil {
			return nil, err
		}
		resp.Records = append(resp.Records, &sourcepb.Record{RecordId: rec.ID, Version: rec.Version, Payload: p,
			Claim: claim})
	}
	if more {
		b, _ := json.Marshal(snapshotToken{Stream: s.Name, After: last, Impl: r.Impl, Epoch: snap.epoch})
		resp.NextPageToken = base64.RawURLEncoding.EncodeToString(b)
	}
	return resp, nil
}
