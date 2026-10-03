package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/problem"
	"github.com/stretchr/testify/require"
)

func countingApply(n *int, err error) func(context.Context, *pg.Tx, envelope.Event) error {
	return func(context.Context, *pg.Tx, envelope.Event) error { *n++; return err }
}

func requireDeadLetter(t *testing.T, f *consumerFixture, in Inbound, reason, delivery string) {
	t.Helper()
	msgs := f.pub.messages()
	require.Len(t, msgs, 1)
	m := msgs[0]
	require.Equal(t, "dlq."+subDurable+"."+subSubject, m.Subject)
	require.Equal(t, envelope.DLQMsgID(subDurable, in.StreamSeq), m.ID)
	require.Equal(t, in.Data, m.Data)
	require.Equal(t, reason, m.Header["be-dlq-reason"])
	require.Equal(t, subDurable, m.Header["be-dlq-consumer"])
	require.Equal(t, delivery, m.Header["be-dlq-delivery"])
	if id := in.Header["ce-id"]; id != "" {
		require.Equal(t, id, m.Header["ce-id"])
	}
	require.Equal(t, in.Header["ce-type"], m.Header["ce-type"])
	ops := f.log.all()
	require.Equal(t, "term", ops[len(ops)-1], "settled by Term")
	require.NotContains(t, ops, "ack")
	for _, op := range ops {
		require.NotContains(t, op, "nak")
	}
	require.Equal(t, 1, f.rec.dlq)
	require.Equal(t, 1, f.rec.handled[subSubject+" dlq"])
}

func TestConsumerMaxDeliverDeadLettersWithoutRunningTheHandler(t *testing.T) {
	calls := 0
	f := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, nil)})
	in := inbound(t, 1, 4)
	f.deliver(t, in)
	require.Equal(t, 0, calls)
	require.Equal(t, []string{"term"}, f.log.all(), "no keepalive: the handler never starts")
	requireDeadLetter(t, f, in, envelope.ReasonMaxDeliver, "4")
}

func TestConsumerDeadLettersEnvelopeErrors(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Inbound)
		reason string
	}{
		"missing ce-id": {func(in *Inbound) { delete(in.Header, "ce-id") }, envelope.ReasonEnvelopeInvalid},
		"hop limit":     {func(in *Inbound) { in.Header["ce-hopcount"] = "11" }, envelope.ReasonHopLimit},
		"not an object": {func(in *Inbound) { in.Data = []byte(`[1]`) }, envelope.ReasonPayloadInvalid},
		"no legal entity": {func(in *Inbound) { delete(in.Header, "ce-legalentity") },
			envelope.ReasonLegalEntityMissing},
		"old X- headers only": {func(in *Inbound) {
			in.Header = map[string]string{"X-Event-Id": "1", "X-Event-Type": subSubject}
		}, envelope.ReasonEnvelopeInvalid},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			calls := 0
			f := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, nil)})
			in := inbound(t, 1, 1)
			tc.mutate(&in)
			f.deliver(t, in)
			require.Equal(t, 0, calls)
			require.Equal(t, []string{"term"}, f.log.all())
			requireDeadLetter(t, f, in, tc.reason, "1")
		})
	}
}

func TestConsumerPermanentErrorDeadLetters(t *testing.T) {
	calls := 0
	f := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, Permanent(errors.New("bad")))})
	in := inbound(t, 1, 1)
	f.deliver(t, in)
	require.Equal(t, 1, calls)
	requireDeadLetter(t, f, in, envelope.ReasonPermanent, "1")
	_, ok := f.cur.v[cursorKey{aggregateType: "conformance.widget.widget", aggregateID: "w-1"}]
	require.False(t, ok, "a failed handler never advances the cursor")
}

func TestConsumerContractViolationIsPermanent(t *testing.T) {
	calls := 0
	def, _ := widgetContract(t).Lookup(subSubject)
	f := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, nil), Validate: def.Validate})
	in := inbound(t, 1, 1)
	in.Data = []byte(`{"widget_id":"w-1","legal_entity_id":"LE01"}`)
	f.deliver(t, in)
	require.Equal(t, 0, calls)
	requireDeadLetter(t, f, in, envelope.ReasonPermanent, "1")
}

func TestConsumerErrorNaksWithTheBackoff(t *testing.T) {
	for delivery, want := range map[uint64]string{1: "nak 200ms", 2: "nak 500ms", 3: "nak 1s"} {
		calls := 0
		f := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, errors.New("db busy"))})
		f.deliver(t, inbound(t, 1, delivery))
		require.Equal(t, 1, calls)
		require.Equal(t, []string{"keepalive", "keepalive-stop", want}, f.log.all(), "delivery %d", delivery)
		require.Empty(t, f.pub.messages())
		require.Equal(t, 1, f.rec.handled[subSubject+" nak"])
	}
}

func TestConsumerLastAllowedDeliveryNaksThenTheNextIsDeadLettered(t *testing.T) {
	// be-protocol vectors envelope/cursor last-allowed-failure and conformance-over: the last
	// allowed delivery still naks; the following delivery is dead-lettered on receipt.
	calls := 0
	f := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, errors.New("down"))})
	f.deliver(t, inbound(t, 1, 3))
	require.Equal(t, "nak 1s", f.log.all()[2])
	require.Empty(t, f.pub.messages())

	g := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, errors.New("down"))})
	in := inbound(t, 1, 4)
	g.deliver(t, in)
	require.Equal(t, 1, calls)
	requireDeadLetter(t, g, in, envelope.ReasonMaxDeliver, "4")
}

func TestConsumerDLQPublishFailureNaks(t *testing.T) {
	f := newConsumerFixture(t, Subscription{Apply: countingApply(new(int), nil)})
	f.pub.setFail(func(jetstream.Message) error { return jetstream.ErrUnavailable })
	f.deliver(t, inbound(t, 1, 4))
	require.Equal(t, []string{"nak 1s"}, f.log.all())
	require.Equal(t, 0, f.rec.dlq)
	require.Equal(t, 1, f.rec.handled[subSubject+" nak"])
}

func TestConsumerAcksOnlyAfterTheCursorCommitted(t *testing.T) {
	var seen envelope.Event
	f := newConsumerFixture(t, Subscription{Apply: func(_ context.Context, _ *pg.Tx, ev envelope.Event) error {
		seen = ev
		return nil
	}})
	in := inbound(t, 2, 1)
	s := f.deliver(t, in)
	require.Equal(t, []string{"keepalive", "commit v2", "keepalive-stop", "ack"}, f.log.all())
	require.Equal(t, KeepAliveEvery, s.kaEvery)
	require.True(t, s.kaStopped)
	require.Equal(t, in.Header["ce-id"], seen.ID)
	require.Equal(t, int64(2), seen.Version)
	require.Equal(t, 1, seen.Delivery)
	require.Equal(t, 1, f.rec.handled[subSubject+" applied"])
	require.Len(t, f.rec.lag, 1)
	require.InDelta(t, 2, f.rec.lag[0], 1)
}

func TestConsumerSkipsStaleAndDuplicateVersions(t *testing.T) {
	calls := 0
	f := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, nil)})
	r := f.runner(t)
	for _, v := range []int64{3, 1, 3, 2} {
		in := inbound(t, v, 1)
		in.Settle = &fakeSettle{log: f.log}
		r.handle(context.Background(), in)
	}
	require.Equal(t, 1, calls, "only version 3 is applied")
	require.Equal(t, 3, f.rec.handled[subSubject+" skipped"])
	require.Equal(t, 1, f.rec.handled[subSubject+" applied"])
	require.Equal(t, int64(3), f.cur.v[cursorKey{aggregateType: "conformance.widget.widget", aggregateID: "w-1"}])
}

func TestConsumerRunHandlerChecksThenAdvances(t *testing.T) {
	calls := 0
	var inTx bool
	f := newConsumerFixture(t, Subscription{Consumer: "mail", Run: func(ctx context.Context, ev envelope.Event) error {
		calls++
		inTx = pg.InTx(ctx)
		return nil
	}})
	r := f.runner(t)
	for _, v := range []int64{2, 2, 1} {
		in := inbound(t, v, 1)
		in.Settle = &fakeSettle{log: f.log}
		r.handle(context.Background(), in)
	}
	require.Equal(t, 1, calls)
	require.False(t, inTx)
	require.Equal(t, int64(2), f.cur.v[cursorKey{consumer: "mail", aggregateType: "conformance.widget.widget", aggregateID: "w-1"}])
	require.Equal(t, []string{"keepalive", "advance v2", "keepalive-stop", "ack",
		"keepalive", "keepalive-stop", "ack", "keepalive", "keepalive-stop", "ack"}, f.log.all())
}

func TestConsumerRunHandlerFailureDoesNotAdvance(t *testing.T) {
	f := newConsumerFixture(t, Subscription{Run: func(context.Context, envelope.Event) error { return errors.New("smtp down") }})
	f.deliver(t, inbound(t, 1, 1))
	require.Empty(t, f.cur.v)
	require.Equal(t, "nak 200ms", f.log.all()[2])
}

type ctxKey struct{}

func TestConsumerContextHookAndDeadline(t *testing.T) {
	var got any
	var left time.Duration
	f := newConsumerFixture(t, Subscription{Run: func(ctx context.Context, ev envelope.Event) error {
		got = ctx.Value(ctxKey{})
		dl, ok := ctx.Deadline()
		require.True(t, ok)
		left = time.Until(dl)
		return nil
	}})
	f.c.Context = func(ctx context.Context, ev envelope.Event, in Inbound) context.Context {
		return context.WithValue(ctx, ctxKey{}, ev.ID)
	}
	in := inbound(t, 1, 1)
	f.deliver(t, in)
	require.Equal(t, in.Header["ce-id"], got)
	require.LessOrEqual(t, left, HandlerTimeout)
	require.Greater(t, left, HandlerTimeout-time.Second)
}

func TestConsumerConfigurationErrors(t *testing.T) {
	apply := countingApply(new(int), nil)
	run := func(context.Context, envelope.Event) error { return nil }
	cases := map[string]func(c *Consumer){
		"both handlers": func(c *Consumer) { c.Subscription.Run = run },
		"no handler":    func(c *Consumer) { c.Subscription.Apply = nil },
		"bad component": func(c *Consumer) { c.ComponentID = "Peer" },
		"bad subject":   func(c *Consumer) { c.Subscription.Subject = "a.b" },
		"no aggregate":  func(c *Consumer) { c.Subscription.AggregateType = "" },
		"no publisher":  func(c *Consumer) { c.Publisher = nil },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newConsumerFixture(t, Subscription{Apply: apply})
			mutate(f.c)
			_, err := f.c.newRunner(f.cur)
			require.Error(t, err)
		})
	}
	f := newConsumerFixture(t, Subscription{Apply: apply})
	require.Error(t, f.c.Run(context.Background()), "Run needs Bus and Store")
}

// fakeSource hands the given deliveries to the handler, then returns.
type fakeSource struct {
	ins             []Inbound
	stream, durable string
	concurrency     int
}

func (s *fakeSource) Consume(ctx context.Context, stream, durable string, concurrency int, h func(context.Context, Inbound)) error {
	s.stream, s.durable, s.concurrency = stream, durable, concurrency
	for _, in := range s.ins {
		h(ctx, in)
	}
	return nil
}

func TestConsumerRunConsumesTheDurable(t *testing.T) {
	calls := 0
	f := newConsumerFixture(t, Subscription{Apply: countingApply(&calls, nil)})
	in := inbound(t, 1, 1)
	in.Settle = &fakeSettle{log: f.log}
	src := &fakeSource{ins: []Inbound{in}}
	r := f.runner(t)
	r.Bus = src
	require.NoError(t, r.run(context.Background()))
	require.Equal(t, "BE_CONFORMANCE", src.stream)
	require.Equal(t, subDurable, src.durable)
	require.Equal(t, DefaultConcurrency, src.concurrency)
	require.Equal(t, 1, calls)
}

func TestConsumerHandlerPanicNaksAndStopsTheKeepAlive(t *testing.T) {
	f := newConsumerFixture(t, Subscription{Run: func(context.Context, envelope.Event) error { panic("boom") }})
	s := f.deliver(t, inbound(t, 1, 1))
	require.True(t, s.kaStopped)
	require.Equal(t, []string{"keepalive", "keepalive-stop", "nak 200ms"}, f.log.all())
	require.Contains(t, f.logs.String(), "boom")
}

func TestConsumerLetsGuardAbortsThrough(t *testing.T) {
	f := newConsumerFixture(t, Subscription{Run: func(context.Context, envelope.Event) error {
		panic(problem.Aborted{Err: problem.Be("NESTED_TX", nil)})
	}})
	err := problem.Catch(func() error { f.deliver(t, inbound(t, 1, 1)); return nil })
	require.True(t, problem.Is(err, problem.DomainBe, "NESTED_TX"))
}
