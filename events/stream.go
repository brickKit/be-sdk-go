// Package events is protocol chapter 06: the outbox written in the business transaction, numbering after commit under
// the partition counter's lock with the database epoch, the forwarder, the CloudEvents envelope, the transport port
// with its JetStream and PostgreSQL queue adapters, and the consumer's inbox, dead letters and claim checks.
package events

import (
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
)

// Stream is a declared event stream with its fixed number of partitions (P6.1).
type Stream struct {
	Name       string // e.g. erp.inventory.stock
	Partitions int    // 1-256, never changed after release
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
