// Package bus opens the transport BUS_URL names (protocol P6.16): nats:// for JetStream, pgqueue:// for the
// PostgreSQL queue. The launcher and the shell open one per process; members share it.
package bus

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/brickKit/be-sdk-go/v3/config"
	"github.com/brickKit/be-sdk-go/v3/events"
	"github.com/brickKit/be-sdk-go/v3/events/jetstream"
	"github.com/brickKit/be-sdk-go/v3/events/pgqueue"
)

// Keys are the bus keys of protocol chapter 02: BUS_URL for every component that produces or consumes events,
// IMPLEMENTATION_INSTANCE for producers.
func Keys(produces bool) []config.Key {
	keys := []config.Key{{Name: "BUS_URL", Type: config.String, Required: true, Secret: true, Process: true}}
	if produces {
		keys = append(keys, config.Key{Name: "IMPLEMENTATION_INSTANCE", Type: config.String, Required: true})
	}
	return keys
}

// Open connects to the bus; name labels the connection (the component or the shell).
func Open(ctx context.Context, url, name string, log *slog.Logger) (events.Transport, error) {
	switch {
	case strings.HasPrefix(url, "nats://"), strings.HasPrefix(url, "tls://"):
		return jetstream.Connect(url, name)
	case strings.HasPrefix(url, "pgqueue://"):
		t, err := pgqueue.Connect(ctx, url, name, 8)
		if err != nil {
			return nil, err
		}
		t.OnError = func(err error) { log.Warn("bus", "error", err.Error()) }
		return t, nil
	}
	return nil, fmt.Errorf("BUS_URL: the scheme must be nats:// or pgqueue://")
}
