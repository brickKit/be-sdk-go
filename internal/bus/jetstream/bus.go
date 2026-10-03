// Package jetstream is the NATS JetStream bus adapter (be-protocol P12.12, scheme nats://): the
// connection (P12.13), streams (P12.4), create-only durables (P12.5), publishing with PubAck
// (P12.1), the pull consume loop with runtime-side delays and progress (P12.5, P12.7, P12.9) and
// best-effort signals (P12.10). It knows nothing about envelopes, cursors or the outbox: stream and
// durable names are passed in by the runtime.
package jetstream

import (
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/semaphore"
)

// Connection constants (P12.13) and the async-publish bound (P12.1).
const (
	ReconnectWait      = 2 * time.Second        // P12.13: reconnect every 2 s ...
	ReconnectJitter    = 500 * time.Millisecond // ... with jitter
	MaxPublishInFlight = 256                    // P12.1: at most 256 acknowledgements in flight
	// DefaultAPITimeout bounds a JetStream API call or a PubAck wait whose context has no deadline.
	DefaultAPITimeout = 5 * time.Second
)

// ErrUnavailable marks an error caused by the bus being unreachable or not answering in time; the
// outbox pump keeps the row PENDING and retries (P12.1). Test with errors.Is.
var ErrUnavailable = errors.New("event bus unavailable")

// Options configures Connect (P12.13).
type Options struct {
	URL    string       // nats://host:port (EVENT_BUS_URL, falling back to NATS_URL; P12.12)
	Name   string       // the connection name: the member's component ID
	Logger *slog.Logger // disconnects, reconnects and async errors are logged here; nil discards
}

// Bus is one connection to NATS JetStream. In a shell the process holds one Bus shared by every
// member (P12.13). It is safe for concurrent use.
type Bus struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	log *slog.Logger
	// inflight bounds async publishes awaiting a PubAck across the whole connection (P12.1).
	inflight *semaphore.Weighted
	// ackWait overrides DurableAckWait for durables this Bus creates; tests only, zero in production.
	ackWait time.Duration
}

// Connect opens the bus connection (P12.13): it reconnects forever every 2 s with jitter, and when
// the server is down at start it returns at once and keeps connecting in the background.
func Connect(o Options) (*Bus, error) {
	if o.URL == "" {
		return nil, errors.New("jetstream: Connect: URL is empty")
	}
	log := o.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log = log.With("component", "event-bus", "bus", "nats")
	nc, err := nats.Connect(o.URL, connectOptions(o.Name, log)...)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncMaxPending(MaxPublishInFlight),
		jetstream.WithPublishAsyncTimeout(DefaultAPITimeout),
		jetstream.WithDefaultTimeout(DefaultAPITimeout))
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Bus{nc: nc, js: js, log: log, inflight: semaphore.NewWeighted(MaxPublishInFlight)}, nil
}

// connectOptions are the P12.13 client settings. The reconnect buffer is disabled so that a publish
// during an outage fails at once instead of being buffered without a PubAck.
func connectOptions(name string, log *slog.Logger) []nats.Option {
	return []nats.Option{
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(ReconnectWait),
		nats.ReconnectJitter(ReconnectJitter, ReconnectJitter),
		nats.RetryOnFailedConnect(true),
		nats.ReconnectBufSize(-1),
		nats.ConnectHandler(func(c *nats.Conn) {
			log.Info("event bus connected", "url", c.ConnectedUrlRedacted())
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("event bus disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Info("event bus reconnected", "url", c.ConnectedUrlRedacted())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, s *nats.Subscription, err error) {
			subject := ""
			if s != nil {
				subject = s.Subject
			}
			log.Error("event bus async error", "subject", subject, "error", err)
		}),
	}
}

// Connected reports whether the connection is currently up.
func (b *Bus) Connected() bool { return b.nc.IsConnected() }

// Close closes the connection; pending async publishes fail.
func (b *Bus) Close() { b.nc.Close() }
