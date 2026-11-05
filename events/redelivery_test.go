package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Where a consumer asks each partition's redelivery to start (P6.39): the smallest progress of the subscriptions that
// read the stream; a partition some subscription has no progress on, or where they saw different instances, is left
// out (from the first row kept); other streams and producers do not count.
func TestTheRedeliveryStartsAtTheSmallestProgress(t *testing.T) {
	ss := sourceStream{component: "demo/p", stream: "demo.s"}
	k := func(impl string, n int) SourceKey {
		return SourceKey{Component: "demo/p", Impl: impl, Stream: "demo.s", Partition: n}
	}
	pt := func(seq int64) Point { return Point{Epoch: Epoch{Generation: 1, Timeline: 1}, Seq: seq} }
	progress := map[string]map[SourceKey]Point{
		"a": {k("i1", 0): pt(9), k("i1", 1): pt(4), k("i1", 2): pt(7), k("i1", 3): pt(5),
			{Component: "demo/other", Impl: "i1", Stream: "demo.s", Partition: 4}: pt(1)},
		"b": {k("i1", 0): pt(6), k("i1", 1): pt(8), k("i2", 3): pt(5), k("i1", 5): pt(2)},
		"c": {},
	}
	from := fromOf(ss, []string{"a", "b"}, progress)
	got := map[uint32]uint64{}
	for _, f := range from {
		assert.Equal(t, "i1", f.GetAfter().GetImpl())
		assert.Equal(t, "1.1", f.GetAfter().GetEpoch())
		got[f.GetPartition()] = f.GetAfter().GetSeq()
	}
	assert.Equal(t, map[uint32]uint64{0: 6, 1: 4}, got,
		"2: b has none; 3: different instances; 5: a has none; the other producer's partition 4 does not count")
	assert.Empty(t, fromOf(ss, []string{"a", "c"}, progress), "a subscription with no progress: everything from the start")
	assert.Len(t, fromOf(ss, []string{"a"}, progress), 4, "one subscription: its own progress")
}
