package besdk

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/envelope"
	"github.com/brickKit/be-sdk-go/internal/events"
	"github.com/brickKit/be-sdk-go/internal/logx"
	"github.com/brickKit/be-sdk-go/internal/pg"
	"github.com/brickKit/be-sdk-go/internal/telemetry"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// contractsRoot presents Spec.Contracts (rooted at contracts/) as the component root the events
// package reads contracts/events/*.events.json from.
type contractsRoot struct{ fs.FS }

func (c contractsRoot) Open(name string) (fs.File, error) {
	rest, ok := strings.CutPrefix(name, "contracts/")
	if name == "contracts" {
		rest, ok = ".", true
	}
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return c.FS.Open(rest)
}

// busURL is EVENT_BUS_URL, falling back to NATS_URL (P12.12); only nats:// is offered in this version.
func busURL(b *boot) (string, error) {
	u := optString(b.vals, "EVENT_BUS_URL", optString(b.vals, "NATS_URL", ""))
	if u != "" && !strings.HasPrefix(u, "nats://") {
		return "", fmt.Errorf("EVENT_BUS_URL %q: only the nats:// adapter is offered by this SDK version (P12.12)", strings.SplitN(u, "://", 2)[0])
	}
	return u, nil
}

// wireEvents connects the bus (in the background: P12.13) and prepares the producer once New has
// declared the module's events.
func (p *process) wireEvents() error {
	ev := p.mod.Events
	if len(ev.Publishes) == 0 && len(ev.Subscribe) == 0 {
		return nil
	}
	url, err := busURL(p.b)
	if err != nil || url == "" {
		return fmt.Errorf("Module.Events needs EVENT_BUS_URL or NATS_URL: %v", err)
	}
	if p.rt.deps.store == nil {
		return fmt.Errorf("Module.Events needs the db profile: outbox and cursor live in the component's schema")
	}
	if p.bus, err = jetstream.Connect(jetstream.Options{URL: url, Name: p.b.id, Logger: p.b.log}); err != nil {
		return err
	}
	p.closers = append(p.closers, func(context.Context) { p.bus.Close() })
	if p.evm, err = telemetry.NewEventMetrics(p.b.member.Registerer()); err != nil {
		return err
	}
	if len(ev.Publishes) > 0 {
		if p.b.spec.Contracts == nil {
			return fmt.Errorf("Module.Events.Publishes needs Spec.Contracts with contracts/events (P12.2)")
		}
		contract, err := events.LoadContract(contractsRoot{p.b.spec.Contracts})
		if err != nil {
			return err
		}
		p.rt.deps.producer = &events.Producer{ComponentID: p.b.id, Version: p.b.version, Contract: contract,
			Publishes: ev.Publishes, Propagator: p.b.member.Propagator(), Logger: p.b.log, Now: p.rt.clock}
	}
	return nil
}

// superviseEvents runs the topology check, the outbox pump and one consumer per subscription.
func (p *process) superviseEvents() {
	if p.bus == nil {
		return
	}
	ev := p.mod.Events
	p.sup.Go("be.events.topology", func(ctx context.Context) error {
		warns, err := events.EnsureTopology(ctx, p.bus, p.b.id, ev.Publishes, subjectsOf(ev.Subscribe))
		for _, w := range warns {
			p.b.log.Warn("durable differs from the protocol constants; left unchanged", slog.String("detail", w))
		}
		return err
	})
	if len(ev.Publishes) > 0 {
		pump := &events.Pump{Store: p.rt.deps.store, Bus: p.bus, ComponentID: p.b.id, Logger: p.b.log,
			Metrics: events.PumpMetrics{Pending: func(n int64) { p.evm.OutboxPending.Set(float64(n)) },
				OldestAge: func(s float64) { p.evm.OutboxOldestAge.Set(s) },
				Published: func(s string) { p.evm.Published.WithLabelValues(s).Inc() }}}
		p.sup.Go("be.outbox", pump.Run)
	}
	for _, s := range ev.Subscribe {
		c := p.consumer(s)
		p.sup.Go("be.consume."+s.Subject, c.Run)
	}
}

func (p *process) consumer(s Subscription) *events.Consumer {
	maxDeliver := int(intOr(p.b.vals, "EVENTS_MAX_DELIVER", int64(s.MaxDeliver)))
	backoff := s.Backoff
	if declared(p.b.vals, "EVENTS_BACKOFF") {
		if d, ok := p.b.vals.Durations("EVENTS_BACKOFF"); ok {
			backoff = d
		}
	}
	if p.rt.deps.producer != nil && s.AggregateType == "" {
		if def, own := p.rt.deps.producer.Contract.Lookup(s.Subject); own {
			s.AggregateType, s.TransactionDocument = def.AggregateType, def.TransactionDocument
		}
	}
	es := events.Subscription{Subject: s.Subject, Consumer: s.Consumer, AggregateType: s.AggregateType,
		TransactionDocument: s.TransactionDocument, MaxDeliver: maxDeliver, Backoff: backoff, Concurrency: s.Concurrency}
	if s.Apply != nil {
		apply := s.Apply
		es.Apply = func(ctx context.Context, tx *pg.Tx, e envelope.Event) error {
			return apply(ctx, &Tx{Tx: tx, rt: p.rt}, eventFrom(e))
		}
	}
	if s.Run != nil {
		run := s.Run
		es.Run = func(ctx context.Context, e envelope.Event) error { return run(ctx, eventFrom(e)) }
	}
	m := p.evm
	return &events.Consumer{Bus: events.JetStream{Bus: p.bus}, Store: p.rt.deps.store, ComponentID: p.b.id,
		Subscription: es, Publisher: p.bus, Logger: p.b.log, Context: p.handlerContext,
		Metrics: events.ConsumerMetrics{
			Handled:      func(subj, r string) { m.ConsumerHandled.WithLabelValues(subj, r).Inc() },
			Lag:          func(subj string, s float64) { m.ConsumerLag.WithLabelValues(subj).Set(s) },
			DeadLettered: func(subj string) { m.DLQ.WithLabelValues(subj).Inc() }}}
}

// handlerContext is where a handler runs: a new trace linked to the producer's span (P18.1), the
// event's log fields (P18.2) and the causation of events it publishes (P12.8).
func (p *process) handlerContext(ctx context.Context, e envelope.Event, in events.Inbound) context.Context {
	var opts []trace.SpanStartOption
	if e.TraceParent != "" {
		carrier := propagation.MapCarrier{"traceparent": e.TraceParent, "tracestate": e.TraceState}
		if sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), carrier)); sc.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: sc}))
		}
	}
	opts = append(opts, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithNewRoot())
	ctx, span := p.b.member.Tracer().Start(ctx, "consume "+e.Subject, opts...)
	context.AfterFunc(ctx, func() { span.End() })
	ctx = logx.WithFields(ctx, slog.String("event_id", e.ID), slog.String("subject", e.Subject),
		slog.Int("delivery", int(in.NumDelivered)))
	return withHandling(ctx, e)
}

func subjectsOf(subs []Subscription) []string {
	out := make([]string, 0, len(subs))
	for _, s := range subs {
		out = append(out, s.Subject)
	}
	return out
}

func (p *process) eventProfiles() []string {
	var out []string
	if len(p.mod.Events.Publishes) > 0 {
		out = append(out, "events-pub")
	}
	if len(p.mod.Events.Subscribe) > 0 {
		out = append(out, "events-sub")
	}
	return out
}

// ensureEventTopology creates the streams and durables at migration time (P11.3, P12.4) from the
// manifest's events block (P12.16), which be-ops generates from the same contract and fixtures.
func ensureEventTopology(ctx context.Context, b *boot) error {
	pub, sub := b.man.Events.Publishes, b.man.Events.Subscribes
	if len(pub) == 0 && len(sub) == 0 {
		return nil
	}
	url, err := busURL(b)
	if err != nil || url == "" {
		return fmt.Errorf("component.yaml declares events but neither EVENT_BUS_URL nor NATS_URL is set: %v", err)
	}
	bus, err := jetstream.Connect(jetstream.Options{URL: url, Name: b.id + "/migrate", Logger: b.log})
	if err != nil {
		return err
	}
	defer bus.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for !bus.Connected() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("the event bus is not reachable: streams and durables not ensured (P12.4)")
		case <-time.After(200 * time.Millisecond):
		}
	}
	warns, err := events.EnsureTopology(ctx, bus, b.id, pub, sub)
	for _, w := range warns {
		b.log.Warn("durable differs from the protocol constants; left unchanged", slog.String("detail", w))
	}
	return err
}
