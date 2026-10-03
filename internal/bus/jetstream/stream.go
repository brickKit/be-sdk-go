package jetstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Stream defaults and the dead-letter stream (P12.4).
const (
	DefaultStreamMaxAge          = 7 * 24 * time.Hour
	DefaultStreamMaxBytes  int64 = 1 << 30
	DefaultDuplicateWindow       = 10 * time.Minute
	DefaultStreamReplicas        = 1
	DLQStream                    = "BE_DLQ"
	DLQSubjects                  = "dlq.>"
	DLQMaxAge                    = 30 * 24 * time.Hour
)

// StreamOptions overrides the P12.4 defaults; a zero field keeps the default.
type StreamOptions struct {
	MaxAge          time.Duration
	MaxBytes        int64
	DuplicateWindow time.Duration
	Replicas        int
}

// streamConfig applies the P12.4 defaults: discard old, file storage.
func streamConfig(name string, subjects []string, o StreamOptions) jetstream.StreamConfig {
	c := jetstream.StreamConfig{
		Name: name, Subjects: subjects,
		MaxAge: DefaultStreamMaxAge, MaxBytes: DefaultStreamMaxBytes, Duplicates: DefaultDuplicateWindow,
		Discard: jetstream.DiscardOld, Storage: jetstream.FileStorage, Replicas: DefaultStreamReplicas,
	}
	if o.MaxAge > 0 {
		c.MaxAge = o.MaxAge
	}
	if o.MaxBytes > 0 {
		c.MaxBytes = o.MaxBytes
	}
	if o.DuplicateWindow > 0 {
		c.Duplicates = o.DuplicateWindow
	}
	if o.Replicas > 0 {
		c.Replicas = o.Replicas
	}
	return c
}

func dlqStreamConfig() jetstream.StreamConfig {
	return streamConfig(DLQStream, []string{DLQSubjects}, StreamOptions{MaxAge: DLQMaxAge})
}

// EnsureStream creates the stream when it is missing and never changes an existing one (P12.4).
// An error names the stream, so a migration that may not create it fails naming it.
func (b *Bus) EnsureStream(ctx context.Context, name string, subjects []string, o StreamOptions) error {
	return b.ensureStream(ctx, streamConfig(name, subjects, o))
}

// EnsureDLQStream ensures the dead-letter stream BE_DLQ, subjects dlq.>, 30 days (P12.4).
func (b *Bus) EnsureDLQStream(ctx context.Context) error {
	return b.ensureStream(ctx, dlqStreamConfig())
}

func (b *Bus) ensureStream(ctx context.Context, c jetstream.StreamConfig) error {
	_, err := b.js.Stream(ctx, c.Name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("jetstream: look up stream %s: %w", c.Name, classify(err))
	}
	_, err = b.js.CreateStream(ctx, c)
	if err == nil || errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return nil // created, or created concurrently by another replica or member
	}
	return fmt.Errorf("jetstream: create stream %s: %w", c.Name, classify(err))
}
