package events

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/grpcx"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/jackc/pgx/v5"
)

// EchoType is the type of a watermark source's echo rows (P9.16).
const EchoType = "be.source.echo.v1"

// PullPage is how many rows of one partition a pull's page returns at most (P9.16).
const PullPage = 500

// WatermarkSource answers the pulls of a member's watermark streams (P9.13-P9.18; protocol 02 steps 1-3): it checks
// the row at the consumer's contiguous position, writes an echo into the same outbox, numbers under the counter lock
// with the forwarder's own code, and returns the rows after the position up to the echo, as the stream carries them.
type WatermarkSource struct {
	sourcepb.UnimplementedWatermarkServiceServer
	DB         *db.DB
	Producer   *Producer
	Source     string // the member's component ID
	Impl       string // IMPLEMENTATION_INSTANCE
	Deployment string // DEPLOYMENT_ID
	Log        *slog.Logger
}

// Method is the pull's system-surface declaration for the member's watermark streams: their operation and every
// stream's pullers; false when the member has none. Streams declaring different operations are refused.
func (w *WatermarkSource) Method() (grpcx.Method, bool, error) {
	var m grpcx.Method
	seen := map[string]bool{}
	for _, s := range w.Producer.Streams() {
		if s.Watermark == nil {
			continue
		}
		if m.Operation != "" && m.Operation != s.Watermark.Operation {
			return m, false, fmt.Errorf("events: watermark streams declare different pull operations (%s, %s)",
				m.Operation, s.Watermark.Operation)
		}
		m.Operation = s.Watermark.Operation
		for _, c := range s.Watermark.Pullers {
			if !seen[c] {
				seen[c] = true
				m.Callers = append(m.Callers, c)
			}
		}
	}
	return m, m.Operation != "", nil
}

// pullToken is a later page: which partitions still have rows, from where to which echo; bound to the instance and
// epoch the first page declared (P9.16).
type pullToken struct {
	Stream string        `json:"s"`
	Nonce  string        `json:"n"`
	Impl   string        `json:"i"`
	Epoch  Epoch         `json:"e"`
	Parts  []tokenCursor `json:"p"`
}

type tokenCursor struct {
	Partition int   `json:"p"`
	After     int64 `json:"a"`
	Echo      int64 `json:"e"`
}

// Pull is the system method.
func (w *WatermarkSource) Pull(ctx context.Context, req *sourcepb.PullRequest) (*sourcepb.PullResponse, error) {
	caller, _ := grpcx.Caller(ctx)
	var tok pullToken
	if req.GetPageToken() != "" {
		raw, err := base64.RawURLEncoding.DecodeString(req.GetPageToken())
		if err != nil || json.Unmarshal(raw, &tok) != nil {
			return nil, errs.New(errs.ReasonRequestInvalid, "page token")
		}
		req = &sourcepb.PullRequest{Stream: tok.Stream, Nonce: tok.Nonce}
	}
	s, ok := w.Producer.streams[req.GetStream()]
	if !ok || s.Watermark == nil {
		return nil, errs.New(errs.ReasonNotFound, fmt.Sprintf("no watermark stream %q", req.GetStream()))
	}
	if !allowed(s.Watermark.Pullers, caller) {
		return nil, errs.New(errs.ReasonPermissionDenied, fmt.Sprintf("%s may not pull %s", caller, s.Name))
	}
	if b, err := hex.DecodeString(req.GetNonce()); err != nil || len(b) < 16 {
		return nil, errs.New(errs.ReasonRequestInvalid, "the nonce is at least 128 random bits, hexadecimal")
	}
	var primary bool
	var system string
	if err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT NOT pg_is_in_recovery(), (SELECT system_identifier::text FROM pg_control_system())").
			Scan(&primary, &system)
	}); err != nil {
		return nil, err
	}
	if !primary {
		return nil, errs.New(errs.ReasonNotReady, "a pull is answered by the primary") // P9.18
	}
	if tok.Stream == "" {
		w.Log.Info("watermark pull", "event", "watermark_pull", "caller", caller, "stream", s.Name, "nonce", req.GetNonce())
		if before := s.Watermark.Before; before != nil {
			if err := before(ctx); err != nil { // no echo: the watermark must not pass changes not brought in (P9.16)
				w.Log.Warn("the source could not bring its changes up to date before the echo; no echo written",
					"event", "watermark_before_failed", "stream", s.Name, "error", err.Error())
				return nil, errs.New(errs.ReasonDependencyUnavailable, "the source's changes could not be brought up to date").
					Wrap(err)
			}
		}
		var err error
		if tok, err = w.firstPage(ctx, s, req, caller); err != nil {
			return nil, err
		}
	} else if err := w.sameSource(ctx, tok); err != nil {
		return nil, err
	}
	return w.page(ctx, s, tok, system)
}

// sameSource refuses a later page once the instance or the epoch the first page declared changed (P9.16).
func (w *WatermarkSource) sameSource(ctx context.Context, tok pullToken) error {
	var now Epoch
	if err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		return tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").Scan(&now.Generation, &now.Timeline)
	}); err != nil {
		return errors.Join(ErrNoEpoch, err)
	}
	if tok.Impl != w.Impl || now != tok.Epoch {
		return errs.New(errs.ReasonQueryPlanChanged, "the source's implementation instance or epoch changed: pull again")
	}
	return nil
}

// firstPage checks every partition, writes the echoes, numbers up to them, and returns the cursors of all pages.
func (w *WatermarkSource) firstPage(ctx context.Context, s Stream, req *sourcepb.PullRequest, caller string) (pullToken, error) {
	asked := map[int]*sourcepb.PartitionPosition{}
	for _, p := range req.GetPartitions() {
		if int(p.GetPartition()) >= s.Partitions {
			return pullToken{}, errs.New(errs.ReasonRequestInvalid, fmt.Sprintf("partition %d of %s", p.GetPartition(), s.Name))
		}
		asked[int(p.GetPartition())] = p
	}
	tok := pullToken{Stream: s.Name, Nonce: req.GetNonce(), Impl: w.Impl}
	results := map[int]string{} // partition -> ok, echo_only, beyond_retention, rollback_detected
	after := map[int]int64{}
	var checked Epoch
	err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
		if err := tx.QueryRow(ctx, "SELECT generation, timeline FROM be_current_db_epoch()").
			Scan(&checked.Generation, &checked.Timeline); err != nil {
			return errors.Join(ErrNoEpoch, err)
		}
		for n := 0; n < s.Partitions; n++ {
			r, a, err := w.check(ctx, tx, s.Name, n, checked, asked[n])
			if err != nil {
				return err
			}
			results[n], after[n] = r, a
			if r == "ok" || r == "echo_only" {
				if _, err := w.Producer.insert(ctx, tx, streamPart{s.Name, n}, Event{Type: EchoType,
					RecordID: req.GetNonce(), Version: 1, Projection: Full, Subjects: []string{},
					Data: map[string]string{"nonce": req.GetNonce(), "consumer": caller}}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return tok, err
	}
	for n := 0; n < s.Partitions; n++ {
		r := results[n]
		if r != "ok" && r != "echo_only" {
			tok.Parts = append(tok.Parts, tokenCursor{Partition: n, After: -1, Echo: needCode(r)})
			if r == "rollback_detected" {
				w.Log.Error("a pull found the row at the consumer's position replaced", "event",
					"watermark_rollback_detected", "stream", s.Name, "partition", n, "position", asked[n].GetPosition())
			}
			continue
		}
		echo, epoch, err := w.numberEcho(ctx, s.Name, n, req.GetNonce())
		if err != nil {
			return tok, err
		}
		tok.Epoch = epoch
		a := after[n]
		if r == "echo_only" || epoch != checked {
			a = echo - 1 // the epoch moved on while numbering: the position belongs to another one, echo only
		}
		tok.Parts = append(tok.Parts, tokenCursor{Partition: n, After: a, Echo: echo})
	}
	if tok.Epoch == (Epoch{}) {
		tok.Epoch = checked
	}
	return tok, nil
}

// Codes of a partition that needs a full snapshot, kept in the token's Echo when After is -1.
const (
	needBeyondRetention  = -1
	needRollbackDetected = -2
)

func needCode(r string) int64 {
	if r == "beyond_retention" {
		return needBeyondRetention
	}
	return needRollbackDetected
}

// RowIdentity is the row at a position as a pull names it: an echo's nonce, or a record's ID, version and row digest.
type RowIdentity struct {
	Echo     string
	RecordID string
	Version  int64
	Digest   string
}

// PullCheck is P9.15's decision for one partition: "echo_only", "ok", "beyond_retention" or "rollback_detected".
// pos is the consumer's position (nil: echo only) and want the row it names; counter is the partition's last sequence
// in the current epoch (0 when nothing is numbered in it yet); at is the outbox row at the position's sequence (nil:
// not there). The watermark-pull-check vectors run against it.
func PullCheck(impl, epoch string, counter int64, pos *Position, want RowIdentity, at *RowIdentity) string {
	switch {
	case pos == nil || pos.Impl != impl || pos.Epoch != epoch:
		return "echo_only"
	case pos.Seq > counter:
		return "rollback_detected"
	case at == nil:
		return "beyond_retention"
	case *at != want:
		return "rollback_detected"
	}
	return "ok"
}

// Position is a place on a stream (P9.14).
type Position struct {
	Impl  string
	Epoch string
	Seq   int64
}

// check reads what P9.15 decides on and returns the decision with the position's sequence.
func (w *WatermarkSource) check(ctx context.Context, tx db.Tx, stream string, n int, current Epoch,
	asked *sourcepb.PartitionPosition) (string, int64, error) {
	p := asked.GetPosition()
	if p == nil || p.GetImpl() != w.Impl || p.GetEpoch() != current.String() {
		return "echo_only", 0, nil
	}
	pos := &Position{Impl: p.GetImpl(), Epoch: p.GetEpoch(), Seq: int64(p.GetSeq())}
	var counter int64
	var gen, tl *int64
	err := tx.QueryRow(ctx, `SELECT last_seq, epoch_generation, epoch_timeline FROM be_outbox_partition
		WHERE stream = $1 AND partition = $2`, stream, n).Scan(&counter, &gen, &tl)
	if err != nil && !isNoRows(err) {
		return "", 0, err
	}
	if gen == nil || *gen != current.Generation || *tl != current.Timeline {
		counter = 0 // nothing numbered in this epoch yet
	}
	var at *RowIdentity
	var typ string
	var r RowIdentity
	err = tx.QueryRow(ctx, `SELECT type, record_id, version, digest FROM be_outbox WHERE stream = $1 AND partition = $2
		AND epoch_generation = $3 AND epoch_timeline = $4 AND seq = $5`, stream, n, current.Generation,
		current.Timeline, pos.Seq).Scan(&typ, &r.RecordID, &r.Version, &r.Digest)
	switch {
	case isNoRows(err):
	case err != nil:
		return "", 0, err
	case typ == EchoType:
		at = &RowIdentity{Echo: r.RecordID}
	default:
		at = &r
	}
	want := RowIdentity{Echo: asked.GetRow().GetEchoNonce()}
	if rec := asked.GetRow().GetRecord(); rec != nil {
		want = RowIdentity{RecordID: rec.GetRecordId(), Version: rec.GetVersion(), Digest: rec.GetDigest()}
	}
	return PullCheck(w.Impl, current.String(), counter, pos, want, at), pos.Seq, nil
}

// numberEcho numbers the partition until the echo has its position (the forwarder's numbering, P6.5) and returns it.
func (w *WatermarkSource) numberEcho(ctx context.Context, stream string, n int, nonce string) (int64, Epoch, error) {
	for {
		var seq *int64
		var e Epoch
		var gen, tl *int64
		if err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			return tx.QueryRow(ctx, `SELECT seq, epoch_generation, epoch_timeline FROM be_outbox
				WHERE stream = $1 AND partition = $2 AND type = $3 AND record_id = $4`, stream, n, EchoType, nonce).
				Scan(&seq, &gen, &tl)
		}); err != nil {
			return 0, e, err
		}
		if seq != nil {
			e = Epoch{Generation: *gen, Timeline: *tl}
			return *seq, e, nil
		}
		if _, err := Number(ctx, w.DB, stream, n, DefaultBatch); err != nil {
			return 0, e, err
		}
	}
}

// page reads up to PullPage rows of every partition after its cursor and up to its echo.
func (w *WatermarkSource) page(ctx context.Context, s Stream, tok pullToken, system string) (*sourcepb.PullResponse, error) {
	resp := &sourcepb.PullResponse{Source: &sourcepb.Declaration{Deployment: w.Deployment, Impl: w.Impl,
		SystemId: system, Epoch: tok.Epoch.String(), Primary: true}}
	next := tok
	next.Parts = nil
	for _, c := range tok.Parts {
		res := &sourcepb.PartitionResult{Partition: uint32(c.Partition)}
		resp.Partitions = append(resp.Partitions, res)
		if c.After < 0 {
			res.NeedSnapshot = sourcepb.NeedSnapshot_NEED_SNAPSHOT_ROLLBACK_DETECTED
			if c.Echo == needBeyondRetention {
				res.NeedSnapshot = sourcepb.NeedSnapshot_NEED_SNAPSHOT_BEYOND_RETENTION
			}
			continue
		}
		var rows []Row
		err := w.DB.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			var err error
			rows, err = rowsBetween(ctx, tx, s.Name, c.Partition, tok.Epoch, c.After, c.Echo, PullPage+1)
			return err
		})
		if err != nil {
			return nil, err
		}
		more := len(rows) > PullPage
		if more {
			rows = rows[:PullPage]
		}
		for _, r := range rows {
			m, _, err := w.Producer.message(ctx, r, w.Source, w.Impl)
			if err != nil {
				return nil, err
			}
			res.Events = append(res.Events, &sourcepb.Event{Headers: m.Headers, Payload: m.Data})
		}
		if more {
			next.Parts = append(next.Parts, tokenCursor{Partition: c.Partition, After: rows[len(rows)-1].Seq, Echo: c.Echo})
		} else if len(rows) == 0 || rows[len(rows)-1].Seq != c.Echo {
			// the rows up to the echo are gone (purged meanwhile) or the epoch moved: this pull cannot finish
			return nil, errs.New(errs.ReasonQueryPlanChanged, "the rows up to the echo are no longer there: pull again")
		} else {
			res.Echo = &sourcepb.Position{Impl: w.Impl, Epoch: tok.Epoch.String(), Seq: uint64(c.Echo)}
		}
	}
	if len(next.Parts) > 0 {
		b, _ := json.Marshal(next)
		resp.NextPageToken = base64.RawURLEncoding.EncodeToString(b)
	}
	return resp, nil
}

// rowsBetween reads a partition's rows of one epoch with after < seq <= upTo, in position order.
func rowsBetween(ctx context.Context, tx db.Tx, stream string, p int, e Epoch, after, upTo int64, limit int) ([]Row, error) {
	rows, err := tx.Query(ctx, `SELECT ord, event_id::text, type, occurred_at, record_id, version, projection,
		coalesce(legal_entity, ''), subjects, coalesce(causation::text, ''), hops, trace, payload, digest, seq,
		epoch_generation, epoch_timeline
		FROM be_outbox WHERE stream = $1 AND partition = $2 AND epoch_generation = $3 AND epoch_timeline = $4
			AND seq > $5 AND seq <= $6
		ORDER BY seq LIMIT $7`, stream, p, e.Generation, e.Timeline, after, upTo, limit)
	if err != nil {
		return nil, err
	}
	return scanRows(rows, stream, p)
}

// allowed says whether a caller (a service identity) is among the declared components, every component when they list
// grpcx.AnyComponent; no identity is never allowed.
func allowed(list []string, caller string) bool {
	return caller != "" && (contains(list, caller) || contains(list, grpcx.AnyComponent))
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
