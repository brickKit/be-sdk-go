package events

import (
	"context"
	"fmt"
	"sort"

	"github.com/brickKit/be-sdk-go/internal/bus/jetstream"
	"github.com/brickKit/be-sdk-go/internal/envelope"
)

// Topology is the part of the bus that creates streams and durables, create-only (P12.4, P12.5).
// *jetstream.Bus implements it.
type Topology interface {
	EnsureStream(ctx context.Context, name string, subjects []string, o jetstream.StreamOptions) error
	EnsureDLQStream(ctx context.Context) error
	EnsureDurable(ctx context.Context, stream, durable, filterSubject string) (warnings []string, err error)
}

// EnsureTopology creates what a member's events need, at the platform migration and again at start
// (P12.4, P12.5): one stream per first subject segment of publishes and subscribes (protocol
// defaults), the dead-letter stream BE_DLQ, and one durable per subscribed subject. Nothing that
// exists is changed; a durable whose configuration differs from the protocol constants yields a
// warning "<durable>: <difference>". An error names the stream or durable.
func EnsureTopology(ctx context.Context, bus Topology, componentID string, publishes, subscribes []string) ([]string, error) {
	streams := map[string]string{}
	for _, s := range append(append([]string(nil), publishes...), subscribes...) {
		name, filter, err := envelope.Stream(s)
		if err != nil {
			return nil, fmt.Errorf("events topology: %w", err)
		}
		streams[name] = filter
	}
	names := make([]string, 0, len(streams))
	for n := range streams {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if err := bus.EnsureStream(ctx, n, []string{streams[n]}, jetstream.StreamOptions{}); err != nil {
			return nil, fmt.Errorf("events topology: stream %s: %w", n, err)
		}
	}
	if err := bus.EnsureDLQStream(ctx); err != nil {
		return nil, fmt.Errorf("events topology: stream %s: %w", envelope.DLQStream, err)
	}
	var warnings []string
	for _, s := range subscribes {
		durable, _, err := envelope.Durable(componentID, s)
		if err != nil {
			return nil, fmt.Errorf("events topology: %w", err)
		}
		stream, _, _ := envelope.Stream(s) // valid: checked above
		w, err := bus.EnsureDurable(ctx, stream, durable, s)
		if err != nil {
			return nil, fmt.Errorf("events topology: durable %s: %w", durable, err)
		}
		for _, x := range w {
			warnings = append(warnings, durable+": "+x)
		}
	}
	return warnings, nil
}
