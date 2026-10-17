// Package jetstream is the NATS JetStream adapter of the events transport port (protocol P6.16-P6.22): one stream
// per event stream, durable pull consumers with explicit acks, Nats-Msg-Id deduplication and core NATS for
// best-effort notifications.
package jetstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/nats-io/nats.go"
	js "github.com/nats-io/nats.go/jetstream"
)

// DuplicateWindow is how long JetStream remembers message IDs; a forwarder republishing after a lost ack within it
// is deduplicated by the broker, later ones by the consumers (P6.10).
const DuplicateWindow = 2 * time.Minute

// Transport is the adapter.
type Transport struct {
	nc *nats.Conn
	js js.JetStream
}

// Connect connects to a nats:// BUS_URL; name labels the connection (the member).
func Connect(url, name string) (*Transport, error) {
	nc, err := nats.Connect(url, nats.Name(name), nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("bus %s: %w", redact(url), err)
	}
	j, err := js.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Transport{nc: nc, js: j}, nil
}

func redact(url string) string {
	if i := strings.Index(url, "@"); i >= 0 {
		if j := strings.Index(url, "://"); j >= 0 && j < i {
			return url[:j+3] + "***" + url[i:]
		}
	}
	return url
}

// MaxMessage implements events.Transport.
func (t *Transport) MaxMessage() int { return int(t.nc.MaxPayload()) }

// EnsureStream implements events.Transport: created when missing, an existing stream is left as it is.
func (t *Transport) EnsureStream(ctx context.Context, s events.Stream) error {
	name := events.JetStreamName(s.Name)
	if _, err := t.js.Stream(ctx, name); err == nil {
		return nil
	} else if !errors.Is(err, js.ErrStreamNotFound) {
		return err
	}
	_, err := t.js.CreateStream(ctx, js.StreamConfig{Name: name, Subjects: []string{"be." + s.Name + ".>"},
		Storage: js.FileStorage, Duplicates: DuplicateWindow})
	if errors.Is(err, js.ErrStreamNameAlreadyInUse) {
		return nil // created concurrently by another replica
	}
	return err
}

// Publish implements events.Transport: returns after the stream's acknowledgement.
func (t *Transport) Publish(ctx context.Context, m events.Message) error {
	msg := &nats.Msg{Subject: m.Subject, Data: m.Data, Header: nats.Header{}}
	for k, v := range m.Headers {
		msg.Header[k] = []string{v} // set directly: header names stay exactly as written
	}
	if size(msg) > t.MaxMessage() {
		return events.ErrTooLarge
	}
	var opts []js.PublishOpt
	if id := m.ID(); id != "" {
		opts = append(opts, js.WithMsgID(id))
	}
	_, err := t.js.PublishMsg(ctx, msg, opts...)
	if errors.Is(err, nats.ErrMaxPayload) {
		return events.ErrTooLarge
	}
	return err
}

func size(m *nats.Msg) int {
	n := len(m.Data) + len(m.Subject)
	for k, vs := range m.Header {
		for _, v := range vs {
			n += len(k) + len(v) + 4
		}
	}
	return n
}

type subscription struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	stops  []func()
}

func (s *subscription) Stop() error {
	s.cancel()
	for _, stop := range s.stops {
		stop()
	}
	s.wg.Wait()
	return nil
}

// Subscribe implements events.Transport: one durable pull consumer per event stream, created when missing and left
// as it is when it exists.
func (t *Transport) Subscribe(ctx context.Context, spec events.SubscriptionSpec, h events.Handler) (events.Subscription, error) {
	spec = spec.Defaults()
	runCtx, cancel := context.WithCancel(context.Background())
	sub := &subscription{cancel: cancel}
	for _, stream := range spec.Streams {
		cons, err := t.consumer(ctx, stream, spec)
		if err != nil {
			_ = sub.Stop()
			return nil, err
		}
		it, err := cons.Messages(js.PullMaxMessages(spec.Concurrency))
		if err != nil {
			_ = sub.Stop()
			return nil, err
		}
		sub.stops = append(sub.stops, it.Stop)
		slots := make(chan struct{}, spec.Concurrency)
		sub.wg.Add(1)
		go func() {
			defer sub.wg.Done()
			var running sync.WaitGroup
			defer running.Wait()
			for {
				msg, err := it.Next()
				if err != nil {
					if errors.Is(err, js.ErrMsgIteratorClosed) || runCtx.Err() != nil {
						return
					}
					continue
				}
				slots <- struct{}{}
				running.Add(1)
				go func() {
					defer func() { <-slots; running.Done() }()
					t.handle(runCtx, spec, msg, h)
				}()
			}
		}()
	}
	return sub, nil
}

func (t *Transport) consumer(ctx context.Context, stream string, spec events.SubscriptionSpec) (js.Consumer, error) {
	name := events.JetStreamName(stream)
	if c, err := t.js.Consumer(ctx, name, spec.Durable); err == nil {
		return c, nil
	} else if !errors.Is(err, js.ErrConsumerNotFound) {
		return nil, err
	}
	var filters []string
	for _, f := range spec.Filters {
		if events.SubjectMatches("be."+stream+".>", f) || strings.HasPrefix(f, "be."+stream+".") {
			filters = append(filters, f)
		}
	}
	cfg := js.ConsumerConfig{Durable: spec.Durable, AckPolicy: js.AckExplicitPolicy, AckWait: spec.AckWait,
		MaxDeliver: spec.MaxDeliver, DeliverPolicy: js.DeliverAllPolicy, MaxAckPending: 1000}
	switch len(filters) {
	case 0:
		if len(spec.Filters) > 0 {
			cfg.FilterSubject = "be." + stream + ".__none__" // none of the filters belongs to this stream
		}
	case 1:
		cfg.FilterSubject = filters[0]
	default:
		cfg.FilterSubjects = filters
	}
	c, err := t.js.CreateConsumer(ctx, name, cfg)
	if err != nil {
		if existing, gerr := t.js.Consumer(ctx, name, spec.Durable); gerr == nil {
			return existing, nil // created concurrently by another instance; its settings stay
		}
	}
	return c, err
}

func (t *Transport) handle(ctx context.Context, spec events.SubscriptionSpec, msg js.Msg, h events.Handler) {
	attempt := 1
	if md, err := msg.Metadata(); err == nil {
		attempt = int(md.NumDelivered)
	}
	headers := map[string]string{}
	for k, vs := range msg.Headers() {
		if len(vs) > 0 && !strings.HasPrefix(k, "Nats-") {
			headers[k] = vs[0]
		}
	}
	d := &events.Delivery{Message: events.Message{Subject: msg.Subject(), Headers: headers, Data: msg.Data()},
		Attempt: attempt, InProgress: msg.InProgress}
	switch h(ctx, d) {
	case events.Ack:
		_ = msg.DoubleAck(ctx)
	case events.Nak:
		_ = msg.NakWithDelay(spec.BackoffFor(attempt))
	case events.Term:
		_ = msg.Term()
	}
}

// Notify implements events.Transport with core NATS: nothing is stored.
func (t *Transport) Notify(_ context.Context, subject string, data []byte) error {
	return t.nc.Publish(subject, data)
}

type listener struct{ s *nats.Subscription }

func (l listener) Stop() error { return l.s.Unsubscribe() }

// Listen implements events.Transport.
func (t *Transport) Listen(_ context.Context, subject string, fn func([]byte)) (events.Subscription, error) {
	s, err := t.nc.Subscribe(subject, func(m *nats.Msg) { fn(m.Data) })
	if err != nil {
		return nil, err
	}
	return listener{s}, t.nc.Flush()
}

// DeleteStream removes an event stream's storage (tests and decommissioning only).
func (t *Transport) DeleteStream(ctx context.Context, stream string) error {
	return t.js.DeleteStream(ctx, events.JetStreamName(stream))
}

// Close implements events.Transport.
func (t *Transport) Close() error { t.nc.Close(); return nil }
