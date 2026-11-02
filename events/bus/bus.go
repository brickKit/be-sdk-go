// Package bus opens the transport BUS_URL names (protocol P6.16): nats:// for JetStream, pgqueue:// for the
// PostgreSQL queue. The launcher and the shell open one per process; members share it.
package bus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nats-io/nats.go"

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
		t.SetOnError(func(err error) { log.Warn("bus", "error", err.Error()) })
		return t, nil
	}
	return nil, fmt.Errorf("%w: BUS_URL: the scheme must be nats:// or pgqueue://", ErrRefused)
}

// Errors of Ready that end the start instead of being retried.
var (
	// ErrRefused: the bus refused the credentials, or BUS_URL names no transport; retrying can't help (exit 78).
	ErrRefused = errors.New("the bus refused the connection settings")
	// ErrStartCheck: the PostgreSQL queue's database does not exist (exit 69).
	ErrStartCheck = errors.New("a start check of the bus failed")
)

// Ready opens the bus and waits until it answers (protocol P6.34): the bus may start after the component, as the
// database may (P5.10). Connection failures are retried every second until ctx ends, the component staying
// "starting"; refused credentials, a wrong scheme and a missing queue database end the wait.
func Ready(ctx context.Context, url, name string, log *slog.Logger) (events.Transport, error) {
	for attempt := 1; ; attempt++ {
		t, err := open(ctx, url, name, log)
		if err == nil {
			if attempt > 1 {
				log.Info("the bus answers", "attempts", attempt)
			}
			return t, nil
		}
		if errors.Is(err, ErrRefused) || errors.Is(err, ErrStartCheck) {
			return nil, err
		}
		if attempt == 1 || attempt%10 == 0 {
			log.Warn("waiting for the bus", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// open is one attempt: Open, and for the lazy PostgreSQL queue a ping, with the failures that retrying can't fix
// marked as such.
func open(ctx context.Context, url, name string, log *slog.Logger) (events.Transport, error) {
	t, err := Open(ctx, url, name, log)
	if err != nil {
		if errors.Is(err, nats.ErrAuthorization) || errors.Is(err, nats.ErrAuthExpired) || errors.Is(err, nats.ErrAuthRevoked) {
			return nil, fmt.Errorf("%w: %v", ErrRefused, err)
		}
		return nil, err
	}
	q, ok := t.(*pgqueue.Transport)
	if !ok {
		return t, nil
	}
	err = q.Ping(ctx)
	if err == nil {
		return t, nil
	}
	_ = t.Close()
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "28P01", "28000":
			return nil, fmt.Errorf("%w: %s", ErrRefused, pg.Message)
		case "3D000":
			return nil, fmt.Errorf("%w: %s (not provisioned)", ErrStartCheck, pg.Message)
		}
	}
	return nil, err
}
