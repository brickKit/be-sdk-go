package events

import (
	"context"
	"fmt"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
)

// handle decides one delivery (P12.6–P12.9). Decision tree:
//   - delivery count above MaxDeliver → dead letters (MAX_DELIVER), handler not run;
//   - envelope rejected by envelope.Accept, or the payload violates the contract → dead letters;
//   - otherwise the handler runs (InProgress every 10 s, deadline 25 s) behind the cursor:
//     success or stale → Ack after the commit; Permanent → dead letters (PERMANENT);
//     any other error → nak with EVENTS_BACKOFF[n − 1] (envelope.Redelivery).
func (r *consumerRunner) handle(ctx context.Context, in Inbound) {
	s := r.Subscription
	delivery := int(in.NumDelivered)
	if envelope.MaxDeliverReached(delivery, s.MaxDeliver) {
		r.deadLetter(ctx, in, envelope.ReasonMaxDeliver)
		return
	}
	dec, err := envelope.Accept(r.accept, in.Header, in.Data, delivery)
	if err != nil { // validated in newRunner; kept safe
		r.log.Error("subscription invalid", "err", err)
		r.nak(in, delivery)
		return
	}
	if dec.DeadLetter {
		r.deadLetter(ctx, in, dec.Reason)
		return
	}
	ev := dec.Event
	if r.Metrics.Lag != nil && !ev.OccurredAt.IsZero() {
		r.Metrics.Lag(s.Subject, time.Since(ev.OccurredAt).Seconds())
	}
	if s.Validate != nil {
		if err := s.Validate(ev.Payload); err != nil {
			r.log.Warn("event violates the producer's contract", "id", ev.ID, "err", err)
			r.deadLetter(ctx, in, envelope.ReasonPermanent)
			return
		}
	}
	stop := in.Settle.KeepAlive(ctx, KeepAliveEvery)
	applied, err := r.invoke(ctx, ev, in)
	stop()
	switch {
	case err == nil:
		r.ack(ctx, in, ev, applied)
	case IsPermanent(err):
		r.log.Warn("event handler failed permanently", "id", ev.ID, "err", err)
		r.deadLetter(ctx, in, envelope.ReasonPermanent)
	default:
		r.log.Warn("event handler failed; redelivered", "id", ev.ID, "delivery", delivery, "err", err)
		r.nak(in, delivery)
	}
}

// invoke runs the handler behind the cursor (P12.6, P12.7) and reports whether it ran. A panic
// is turned into an error, so the keepalive stops and the delivery is nak'ed like any failure.
func (r *consumerRunner) invoke(ctx context.Context, ev envelope.Event, in Inbound) (applied bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			if a, ok := p.(problem.Aborted); ok {
				panic(a) // a guard's abort in a test binary (P4) is not a handler failure
			}
			applied, err = false, fmt.Errorf("event handler panicked: %v", p)
		}
	}()
	if r.Context != nil {
		ctx = r.Context(ctx, ev, in)
	}
	ctx, cancel := context.WithTimeout(ctx, HandlerTimeout)
	defer cancel()
	s := r.Subscription
	k := cursorKey{consumer: s.Consumer, aggregateType: ev.AggregateType, aggregateID: ev.AggregateID}
	if s.Apply != nil {
		return r.cur.apply(ctx, k, ev, func(ctx context.Context, tx *pg.Tx) error { return s.Apply(ctx, tx, ev) })
	}
	v, ok, err := r.cur.current(ctx, k)
	if err != nil {
		return false, err
	}
	if ok && v >= ev.Version {
		return false, nil
	}
	if err := s.Run(ctx, ev); err != nil {
		return false, err
	}
	return true, r.cur.advance(ctx, k, ev)
}

// ack acknowledges after the cursor committed (P12.6). A lost Ack means a redelivery the cursor
// skips.
func (r *consumerRunner) ack(ctx context.Context, in Inbound, ev envelope.Event, applied bool) {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), SettleTimeout)
	defer cancel()
	if err := in.Settle.Ack(sctx); err != nil {
		r.log.Warn("ack failed; the redelivery will be skipped by the cursor", "id", ev.ID, "err", err)
	}
	result := ResultApplied
	if !applied {
		result = ResultSkipped
	}
	r.count(result)
}

// nak asks for redelivery after EVENTS_BACKOFF[n − 1] (P12.7).
func (r *consumerRunner) nak(in Inbound, delivery int) {
	s := r.Subscription
	a := envelope.Redelivery(delivery, s.MaxDeliver, s.Backoff, envelope.OutcomeError, r.durable, in.StreamSeq)
	delay := a.Delay
	if a.Kind != envelope.ActionNak {
		delay = envelope.NakDelay(delivery, s.Backoff)
	}
	if err := in.Settle.NakWithDelay(delay); err != nil {
		r.log.Warn("nak failed; redelivered after ack_wait", "err", err)
	}
	r.count(ResultNak)
}

// deadLetter publishes the message to dlq.<durable>.<subject> with message ID
// dlq:<durable>:<stream sequence> and the be-dlq-* headers, then terminates the original only after
// the bus stored the copy; when the copy fails, the original is nak'ed instead (P12.7).
func (r *consumerRunner) deadLetter(ctx context.Context, in Inbound, reason string) {
	delivery := int(in.NumDelivered)
	m := jetstream.Message{Subject: r.dlqSubj, ID: envelope.DLQMsgID(r.durable, in.StreamSeq),
		Header: envelope.DeadLetterHeaders(in.Header, r.durable, delivery, reason), Data: in.Data}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), SettleTimeout)
	defer cancel()
	if errs := r.Publisher.PublishBatch(sctx, []jetstream.Message{m}); errs[0] != nil {
		r.log.Warn("dead-letter publish failed; redelivered", "reason", reason, "err", errs[0])
		r.nak(in, delivery)
		return
	}
	if err := in.Settle.Term(); err != nil {
		r.log.Warn("term failed after dead-lettering; the copy is deduplicated", "err", err)
	}
	r.log.Warn("event dead-lettered", "reason", reason, "id", in.Header[envelope.HeaderID], "delivery", delivery)
	if r.Metrics.DeadLettered != nil {
		r.Metrics.DeadLettered(r.Subscription.Subject)
	}
	r.count(ResultDLQ)
}

func (r *consumerRunner) count(result string) {
	if r.Metrics.Handled != nil {
		r.Metrics.Handled(r.Subscription.Subject, result)
	}
}
