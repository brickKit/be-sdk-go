//go:build integration

package events

import (
	"context"
	"strings"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/db"
	"github.com/brickKit/be-sdk-go/v3/errs"
	"github.com/brickKit/be-sdk-go/v3/ids"
	"github.com/brickKit/be-sdk-go/v3/internal/pgtest"
	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The producer's side of a redelivery (P6.38): from after the asked position, in position order, with the event's
// identity and a message identity of its own; another instance's position, or none, from the first row kept; a start
// past what the partition has is "ahead" (past the counter, an epoch it never had, past its epoch's fork point); a
// start before the purge is "beyond the retention".
func TestResendFromTheAskedPosition(t *testing.T) {
	ctx := context.Background()
	d, admin := source(t, "evresend")
	single := Stream{Name: "demo.single", Partitions: 1}
	p, err := NewProducer("demo/evresend", []Stream{single}, nil)
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		require.NoError(t, d.Tx(ctx, func(ctx context.Context, tx db.Tx) error {
			_, err := p.Emit(ctx, tx, single.Name, Event{Type: "demo.single.made.v1", RecordID: ids.New().String(),
				Version: 1, Projection: Full})
			return err
		}))
	}
	impl := ids.New().String()
	mem := NewMemory(0)
	f := &Forwarder{DB: d, Producer: p, Transport: mem, Source: "demo/evresend", Impl: impl}
	_, err = f.Once(ctx)
	require.NoError(t, err)
	var epoch string
	require.NoError(t, admin.QueryRow(ctx, `SELECT epoch_generation || '.' || epoch_timeline FROM be_outbox_partition`).Scan(&epoch))
	r := &Resender{DB: d, Producer: p, Transport: mem, Source: "demo/evresend", Impl: impl}
	ask := func(req string, after *sourcepb.Position) (*sourcepb.ResendResponse, []Message) {
		t.Helper()
		mem.mu.Lock()
		before := len(mem.log)
		mem.mu.Unlock()
		in := &sourcepb.ResendRequest{Stream: single.Name, RequestId: req}
		if after != nil {
			in.From = []*sourcepb.PartitionFrom{{Partition: 0, After: after}}
		}
		out, err := r.Resend(ctx, in)
		require.NoError(t, err)
		mem.mu.Lock()
		defer mem.mu.Unlock()
		return out, append([]Message(nil), mem.log[before:]...)
	}
	at := func(e string, seq uint64) *sourcepb.Position {
		return &sourcepb.Position{Impl: impl, Epoch: e, Seq: seq}
	}

	out, sent := ask("r1", at(epoch, 2))
	assert.EqualValues(t, 4, out.GetResent())
	require.Len(t, sent, 4)
	assert.Equal(t, "3", sent[0].Headers[HeaderSeq], "in position order, after the start")
	assert.Equal(t, "6", sent[3].Headers[HeaderSeq])
	assert.Empty(t, out.GetBeyondRetention())
	assert.Empty(t, out.GetAhead())

	out, sent = ask("r2", &sourcepb.Position{Impl: ids.New().String(), Epoch: epoch, Seq: 4})
	assert.EqualValues(t, 6, out.GetResent(), "another instance's position: from the first row kept")
	out, _ = ask("r3", nil)
	assert.EqualValues(t, 6, out.GetResent(), "no position: from the first row kept")

	out, _ = ask("r4", at(epoch, 9))
	assert.Equal(t, []uint32{0}, out.GetAhead(), "past the counter")
	assert.Zero(t, out.GetResent())
	out, _ = ask("r5", at("99.1", 1))
	assert.Equal(t, []uint32{0}, out.GetAhead(), "an epoch it never had")
	pgtest.Exec(t, admin, `INSERT INTO be_epoch_history (stream, partition, old_generation, old_timeline, fork_seq,
		new_generation, new_timeline) SELECT 'demo.single', 0, 0, 1, 4, epoch_generation, epoch_timeline FROM be_outbox_partition`)
	out, _ = ask("r6", at("0.1", 3))
	assert.Empty(t, out.GetAhead(), "an earlier epoch, before its fork point")
	assert.EqualValues(t, 6, out.GetResent(), "the later epoch follows")
	out, _ = ask("r7", at("0.1", 5))
	assert.Equal(t, []uint32{0}, out.GetAhead(), "past the earlier epoch's fork point")

	pgtest.Exec(t, admin, `UPDATE be_outbox_partition SET purged_generation = epoch_generation, purged_timeline = epoch_timeline,
		purged_seq = 2`)
	out, _ = ask("r8", at(epoch, 1))
	assert.Equal(t, []uint32{0}, out.GetBeyondRetention(), "rows after the start were purged")
	out, _ = ask("r9", at(epoch, 2))
	assert.Empty(t, out.GetBeyondRetention(), "everything after the start is kept")
	out, _ = ask("r10", nil)
	assert.Equal(t, []uint32{0}, out.GetBeyondRetention(), "no start, and rows were purged")

	// the redelivered messages: the event's identity and headers, a message identity of their own
	out, sent = ask("r11", at(epoch, 5))
	require.Len(t, sent, 1)
	assert.Equal(t, sent[0].ID()+"/resend/r11", sent[0].MsgID)
	assert.True(t, strings.HasPrefix(sent[0].Subject, "be.demo.single.0."), sent[0].Subject)
	assert.Equal(t, impl, sent[0].Headers[HeaderImpl])

	_, err = r.Resend(ctx, &sourcepb.ResendRequest{Stream: "demo.other"})
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "%v", err)
	_, err = r.Resend(ctx, &sourcepb.ResendRequest{Stream: single.Name, From: []*sourcepb.PartitionFrom{{Partition: 3}}})
	assert.True(t, errs.Is(err, "be", errs.ReasonRequestInvalid), "a partition the stream does not have: %v", err)
}
