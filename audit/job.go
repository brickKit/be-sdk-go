package audit

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/work"
	auditv3 "github.com/brickKit/infra-audit/contract/v3/gen/go/be/audit/v3"
	"google.golang.org/grpc"
)

// Methods of the audit component's system surface (infra-audit's contract).
const (
	SubmitMethod    = auditv3.IntakeService_Submit_FullMethodName
	WatermarkMethod = auditv3.IntakeService_Watermark_FullMethodName
)

// SendTimeout bounds one run's sending to the audit component.
const SendTimeout = 30 * time.Second

// GRPCSender sends to the audit component over the system surface (mTLS, the call policy).
type GRPCSender struct{ Conn *grpc.ClientConn }

// Send implements Sender.
func (g GRPCSender) Send(ctx context.Context, records []Wire) ([]Receipt, error) {
	in := &auditv3.SubmitRequest{}
	for _, r := range records {
		in.Items = append(in.Items, &auditv3.Item{Id: r.ID, Seq: r.Seq, Epoch: r.Epoch, Digest: r.Digest, Record: r.Record})
	}
	out, err := auditv3.NewIntakeServiceClient(g.Conn).Submit(ctx, in, calls.Idempotent())
	if err != nil {
		return nil, err
	}
	rs := make([]Receipt, 0, len(out.GetReceipts()))
	for _, r := range out.GetReceipts() {
		status := "conflict"
		switch r.GetStatus() {
		case auditv3.ReceiptStatus_RECEIPT_STATUS_RECEIVED:
			status = "received"
		case auditv3.ReceiptStatus_RECEIPT_STATUS_INVALID:
			status = "invalid"
		}
		rs = append(rs, Receipt{ID: r.GetId(), Status: status, Epoch: r.GetEpoch(), Seq: r.GetSeq(), Chain: r.GetChain()})
	}
	return rs, nil
}

// Watermark reads the audit component's backed-up watermark.
func (g GRPCSender) Watermark(ctx context.Context) (Watermark, error) {
	out, err := auditv3.NewIntakeServiceClient(g.Conn).Watermark(ctx, &auditv3.WatermarkRequest{}, calls.Idempotent())
	if err != nil {
		return Watermark{}, err
	}
	w := Watermark{Epoch: out.GetEpoch(), Seq: out.GetSeq(), BackedUpInEpoch: out.GetBackedUpInEpoch(),
		Ancestors: map[string]int64{}}
	for _, a := range out.GetAncestors() {
		w.Ancestors[a.GetEpoch()] = a.GetFork()
	}
	return w, nil
}

// Job is the transfer as a singleton work item (P8.21): number, summarize finished denial windows, send until
// nothing is pending, refresh the backlog, and delete what the backup covers. Without a sender the records stay
// local and the backlog grows (and is alerted on).
func (a *Auditor) Job(sender *GRPCSender, log *slog.Logger) work.Job {
	return work.Job{Name: "be.audit.transfer", Kind: work.Singleton, Effect: work.IdempotentExternal,
		Interval: TransferEvery, MaxAttempts: work.Unlimited, Handle: func(ctx context.Context, _ *work.Run) error {
			if _, err := a.Number(ctx); err != nil {
				return fmt.Errorf("numbering audit records: %w", err)
			}
			if _, err := a.Flush(ctx); err != nil {
				return fmt.Errorf("summarizing denials: %w", err)
			}
			if sender != nil {
				// calls need a deadline (P4.4): each batch gets one
				ctx, cancel := context.WithTimeout(ctx, SendTimeout)
				defer cancel()
				for {
					n, err := a.Transfer(ctx, sender)
					if err != nil {
						log.Warn("audit records could not be transferred; they stay local", "error", err.Error())
						break
					}
					if n < Batch {
						break
					}
				}
				if w, err := sender.Watermark(ctx); err == nil {
					if _, _, err := a.Purge(ctx, w); err != nil {
						return err
					}
				}
			}
			n, err := a.Backlog(ctx)
			if err == nil && a.Capacity > 0 && n >= a.Capacity {
				log.Error("the audit backlog reached its capacity; required audited operations are refused", "backlog", n)
			}
			return err
		}}
}
