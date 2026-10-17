package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/brickKit/be-sdk-go/v3/calls"
	"github.com/brickKit/be-sdk-go/v3/work"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Methods of the audit component's system surface. Until its contract is generated the records travel as JSON in a
// BytesValue.
const (
	SubmitMethod    = "/be.audit.v3.Intake/Submit"
	WatermarkMethod = "/be.audit.v3.Intake/Watermark"
)

// SendTimeout bounds one run's sending to the audit component.
const SendTimeout = 30 * time.Second

// GRPCSender sends to the audit component over the system surface (mTLS, the call policy).
type GRPCSender struct{ Conn *grpc.ClientConn }

// Send implements Sender.
func (g GRPCSender) Send(ctx context.Context, records []Wire) ([]Receipt, error) {
	body, err := json.Marshal(records)
	if err != nil {
		return nil, err
	}
	out := new(wrapperspb.BytesValue)
	if err := g.Conn.Invoke(ctx, SubmitMethod, wrapperspb.Bytes(body), out, calls.Idempotent()); err != nil {
		return nil, err
	}
	var rs []Receipt
	return rs, json.Unmarshal(out.Value, &rs)
}

// Watermark reads the audit component's backed-up watermark.
func (g GRPCSender) Watermark(ctx context.Context) (Watermark, error) {
	out := new(wrapperspb.BytesValue)
	if err := g.Conn.Invoke(ctx, WatermarkMethod, wrapperspb.Bytes([]byte("{}")), out, calls.Idempotent()); err != nil {
		return Watermark{}, err
	}
	var w Watermark
	return w, json.Unmarshal(out.Value, &w)
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
