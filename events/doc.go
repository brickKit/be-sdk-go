// Package events is protocol chapter 06: the outbox written in the business transaction, numbering after commit under
// the partition counter's lock with the database epoch, the forwarder, the CloudEvents envelope, the transport port
// with its JetStream and PostgreSQL queue adapters, and the consumer's inbox, dead letters and claim checks.
//
// The files:
//
//   - stream.go: a stream's declaration, its partitions and subjects, the durable names.
//   - producer.go: the outbox, written in the business transaction (Producer.Emit; P6.14, P6.15).
//   - number.go: numbering after commit under the partition counter's lock, with the database epoch (P6.5).
//   - envelope.go: the CloudEvents envelope, the epoch and the digest (P6.7).
//   - forwarder.go: the forwarder from the outbox to the transport, and the outbox's purge job.
//   - transport.go: the transport port (messages, subscriptions, back-off); memory.go is the in-process transport
//     the conformance suite is checked against; the adapters are in bus/ (by BUS_URL), jetstream/ and pgqueue/,
//     and transporttest/ is their shared suite.
//   - consumer.go: subscriptions, the inbox, dead letters (Receive).
//   - claim.go, objectstore.go: claim-checked payloads over the object store (P6.14).
//   - subject.go: NATS-style subject patterns.
//   - readafter.go: reading one stream by position from the outbox (the identity member's ended sessions).
//   - progress.go: a run's progress points (P6.37); resend.go: the producer's ResendService (P6.38);
//     redelivery.go: the consumer side after its database went back (P6.39, P6.40).
//   - records.go: the records' batch reads and snapshots, the standard RecordService (P9.31, P9.32);
//     watermark.go: a watermark source's pulls and their check (P9.16); stamp.go: what a source declares with every
//     read (protocol 02).
package events
