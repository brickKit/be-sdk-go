package events

import (
	"context"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
)

// Stream is a declared event stream with its fixed number of partitions (P6.1).
type Stream struct {
	Name       string // e.g. erp.inventory.stock
	Partitions int    // 1-256, never changed after release
	// Watermark declares the stream a watermark source (P9.13): every event lists its subjects, and the member answers
	// pulls of it; nil: an ordinary stream.
	Watermark *Watermark
	// Records declares the stream's batch read and snapshot (P9.30); a watermark stream must. nil: no reads.
	Records *Records
}

// Watermark is what a watermark source's stream declares (P9.13).
type Watermark struct {
	// Operation is the operation the pull method performs, declared in the component's resource definition like
	// every system method's (the method has no other authorization than its callers).
	Operation string
	// Pullers are the components (service identities) that may pull the stream: the ones that subscribe to it and
	// use watermarks, and readers of its high point (protocol 06's cost component).
	Pullers []string
	// Before, when set, runs and commits before a pull's first page writes its echoes (P9.16): a source mirroring an
	// external product pulls the product's changes up to now into the outbox here, so they come before the echo
	// (the assembly project's security 03). When it fails no echo is written and the pull answers UNAVAILABLE: the
	// consumer's watermark does not advance.
	Before func(ctx context.Context) error
}

var streamName = regexp.MustCompile(`^[a-z0-9_]+(\.[a-z0-9_]+)*$`)

// Validate checks the declaration.
func (s Stream) Validate() error {
	if !streamName.MatchString(s.Name) {
		return fmt.Errorf("stream %q: lower-case letters, digits, underscores and dots", s.Name)
	}
	if s.Partitions < 1 || s.Partitions > 256 {
		return fmt.Errorf("stream %s: partitions must be 1-256, not %d", s.Name, s.Partitions)
	}
	if w := s.Watermark; w != nil && (w.Operation == "" || len(w.Pullers) == 0) {
		return fmt.Errorf("stream %s: a watermark source declares the pull's operation and who may pull it", s.Name)
	}
	if r := s.Records; r != nil && (r.Operation == "" || len(r.Readers) == 0 || r.Read == nil || r.Page == nil) {
		return fmt.Errorf("stream %s: record reads declare the operation, who may read, and both read functions", s.Name)
	}
	if w := s.Watermark; w != nil {
		if s.Records == nil {
			return fmt.Errorf("stream %s: a watermark source declares its record reads (P9.30)", s.Name)
		}
		for _, c := range w.Pullers {
			if !contains(s.Records.Readers, c) {
				return fmt.Errorf("stream %s: puller %s is not among the record readers (P9.30)", s.Name, c)
			}
		}
	}
	return nil
}

// PartitionOf is P6.1: FNV-1a 64 of the record ID's lower-case form, modulo the partition count.
func (s Stream) PartitionOf(recordID string) int {
	h := fnv.New64a()
	h.Write([]byte(strings.ToLower(recordID)))
	return int(h.Sum64() % uint64(s.Partitions))
}

// Projection is what an event carries (protocol 02, S2).
type Projection string

// Projections.
const (
	Full    Projection = "full"
	Partial Projection = "partial"
	Delete  Projection = "delete"
)

// Event is one event a module emits.
type Event struct {
	Type        string // declared in the contract, with its major version: erp.inventory.stock_moved.v1
	RecordID    string
	Version     int64
	Projection  Projection
	Data        any      // encoded as JSON
	LegalEntity string   // transaction facts
	Subjects    []string // watermark sources: the affected principals (at most 1000)
}

// Subject is the transport subject of a stream partition and type (P6.17).
func Subject(stream string, partition int, eventType string) string {
	return fmt.Sprintf("be.%s.%d.%s", stream, partition, eventType)
}

// JetStreamName is the JetStream stream of an event stream (P6.17).
func JetStreamName(stream string) string {
	return "BE_" + strings.ToUpper(strings.ReplaceAll(stream, ".", "_"))
}

// DurableName is a subscription's durable name (P6.18).
func DurableName(componentID, subscription string) string {
	return strings.NewReplacer("/", "_", ".", "_", "-", "_").Replace(componentID + "_" + subscription)
}
