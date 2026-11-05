package evidence

import (
	"context"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/work"
	auditv3 "github.com/brickKit/infra-audit/contract/v3/gen/go/be/audit/v3"
	"google.golang.org/grpc"
)

// Batch is the most records one Append carries (P12.12).
const Batch = 500

// TransferEvery is how often be.evidence.transfer runs.
const TransferEvery = 2 * time.Second

// GRPCStore is the audit component's EvidenceService over the system surface (mTLS, the call policy).
type GRPCStore struct{ Conn *grpc.ClientConn }

// Append implements Store.
func (g GRPCStore) Append(ctx context.Context, recs []Sealed) ([]string, error) {
	in := &auditv3.AppendRequest{}
	for _, r := range recs {
		in.Records = append(in.Records, &auditv3.Evidence{Record: r.Record, Digest: r.Digest})
	}
	out, err := auditv3.NewEvidenceServiceClient(g.Conn).Append(ctx, in, calls.Idempotent())
	if err != nil {
		return nil, err
	}
	st := make([]string, 0, len(out.GetReceipts()))
	for _, r := range out.GetReceipts() {
		switch r.GetStatus() {
		case auditv3.EvidenceStatus_EVIDENCE_STATUS_ACCEPTED:
			st = append(st, "accepted")
		case auditv3.EvidenceStatus_EVIDENCE_STATUS_DUPLICATE:
			st = append(st, "duplicate")
		case auditv3.EvidenceStatus_EVIDENCE_STATUS_CONFLICT:
			st = append(st, "conflict")
		default:
			st = append(st, "invalid")
		}
	}
	return st, nil
}

// Transfer sends the evidence outbox's rows not stopped, in order, a batch at a time, until none is left (P12.15):
// accepted and duplicate rows are deleted; conflict and invalid rows are stopped and logged, never sent again. A
// failed call leaves the rest for the next run. It returns how many rows were settled.
func Transfer(ctx context.Context, d *db.DB, store Store, log *slog.Logger) (int, error) {
	total := 0
	for {
		type row struct {
			ord int64
			s   Sealed
		}
		var rows []row
		if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			rows = rows[:0]
			rs, err := tx.Query(ctx, `SELECT ord, id, phase, record, digest FROM be_evidence_outbox WHERE stopped IS NULL
				ORDER BY ord LIMIT $1`, Batch)
			if err != nil {
				return err
			}
			defer rs.Close()
			for rs.Next() {
				var r row
				if err := rs.Scan(&r.ord, &r.s.ID, &r.s.Phase, &r.s.Record, &r.s.Digest); err != nil {
					return err
				}
				rows = append(rows, r)
			}
			return rs.Err()
		}); err != nil || len(rows) == 0 {
			return total, err
		}
		recs := make([]Sealed, len(rows))
		for i, r := range rows {
			recs[i] = r.s
		}
		st, err := store.Append(ctx, recs)
		if err != nil {
			return total, err
		}
		if err := d.BackgroundTx(ctx, func(ctx context.Context, tx db.Tx) error {
			for i, r := range rows {
				if i >= len(st) {
					break
				}
				switch st[i] {
				case "accepted", "duplicate":
					if _, err := tx.Exec(ctx, "DELETE FROM be_evidence_outbox WHERE ord = $1", r.ord); err != nil {
						return err
					}
				default:
					log.Error("the evidence store refused a record: it is kept here and not sent again", "event",
						"evidence_refused", "id", r.s.ID, "phase", r.s.Phase, "status", st[i])
					if _, err := tx.Exec(ctx, "UPDATE be_evidence_outbox SET stopped = $2 WHERE ord = $1", r.ord, st[i]); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return total, err
		}
		total += len(rows)
		if len(rows) < Batch {
			return total, nil
		}
	}
}

// Job is be.evidence.transfer, a singleton work item (P12.15). Without a store the records stay local.
func Job(d *db.DB, store Store, log *slog.Logger) work.Job {
	return work.Job{Name: "be.evidence.transfer", Kind: work.Singleton, Effect: work.IdempotentExternal,
		Interval: TransferEvery, MaxAttempts: work.Unlimited, Handle: func(ctx context.Context, _ *work.Run) error {
			if store == nil {
				return nil
			}
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second) // calls need a deadline (P4.4)
			defer cancel()
			if _, err := Transfer(ctx, d, store, log); err != nil {
				log.Warn("evidence could not be transferred; it stays local", "error", err.Error())
			}
			return nil
		}}
}
