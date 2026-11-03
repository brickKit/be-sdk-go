package projection

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"

	"github.com/brickKit/be-sdk-go/v3/internal/sourcepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// fakeConn answers RecordService calls with answer(request).
type fakeConn struct {
	grpc.ClientConnInterface
	answer func(req any) proto.Message
	calls  int
}

func (f *fakeConn) Invoke(_ context.Context, _ string, req, reply any, _ ...grpc.CallOption) error {
	f.calls++
	proto.Merge(reply.(proto.Message), f.answer(req))
	return nil
}

type mapClaims map[string][]byte

func (m mapClaims) Get(_ context.Context, uri string) ([]byte, error) {
	if b, ok := m[uri]; ok {
		return b, nil
	}
	return nil, errors.New("no such object")
}

func primary(epoch string) *sourcepb.Declaration {
	return &sourcepb.Declaration{Deployment: "dep-1", Impl: "impl-1", SystemId: "7", Epoch: epoch, Primary: true}
}

func claimRef(uri string, data []byte) []byte {
	sum := sha256.Sum256(data)
	b, _ := json.Marshal(map[string]any{"claim": map[string]any{"uri": uri, "size": len(data), "sha256": hex.EncodeToString(sum[:])}})
	return b
}

func TestRecordClientResolvesClaims(t *testing.T) { // P9.30, P6.14
	big := []byte(`{"text":"big"}`)
	conn := &fakeConn{answer: func(any) proto.Message {
		return &sourcepb.SnapshotResponse{Source: primary("1.1"), Records: []*sourcepb.Record{
			{RecordId: "a", Version: 1, Payload: claimRef("mem://a", big), Claim: true},
			{RecordId: "b", Version: 2, Payload: []byte(`{"claim":"not a reference"}`)}}}
	}}
	c := RecordClient{Conn: conn, Stream: "demo.rows", Claims: mapClaims{"mem://a": big}}
	recs, next, from, err := c.Snapshot(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Equal(t, Identity{Deployment: "dep-1", Impl: "impl-1", SystemID: "7", Epoch: "1.1", Primary: true}, from)
	assert.JSONEq(t, string(big), string(recs[0].Data), "the claimed object")
	assert.JSONEq(t, `{"claim":"not a reference"}`, string(recs[1].Data), "a content without the flag is as sent")

	c.Claims = mapClaims{"mem://a": []byte(`{"text":"bog"}`)}
	_, _, _, err = c.Snapshot(context.Background(), "")
	assert.ErrorContains(t, err, "does not match its size and digest")
	c.Claims = nil
	_, _, _, err = c.Snapshot(context.Background(), "")
	assert.ErrorContains(t, err, "no object storage reader")
}

func TestRecordClientReadsInBatchesFromOneSource(t *testing.T) { // P9.31, P9.33
	ids := make([]string, 501)
	for i := range ids {
		ids[i] = string(rune('a'+i%26)) + string(rune('0'+i/26%10)) + string(rune('0'+i/260))
	}
	epochs := []string{"1.1", "1.1"}
	conn := &fakeConn{}
	conn.answer = func(req any) proto.Message {
		r := req.(*sourcepb.ReadRequest)
		out := &sourcepb.ReadResponse{Source: primary(epochs[conn.calls-1])}
		for _, id := range r.GetRecordIds() {
			out.Results = append(out.Results, &sourcepb.ReadResult{RecordId: id, State: sourcepb.ReadState_READ_STATE_PURGED})
		}
		return out
	}
	c := RecordClient{Conn: conn, Stream: "demo.rows"}
	res, from, err := c.Read(context.Background(), ids)
	require.NoError(t, err)
	assert.Equal(t, 2, conn.calls, "500 per read")
	assert.Len(t, res, 501)
	assert.Equal(t, "1.1", from.Epoch)

	conn.calls, epochs = 0, []string{"1.1", "2.1"}
	_, _, err = c.Read(context.Background(), ids)
	assert.ErrorIs(t, err, ErrOtherSource, "the reads of one batch declared different epochs")

	conn.calls = 0
	conn.answer = func(req any) proto.Message {
		return &sourcepb.ReadResponse{Source: primary("1.1"), Results: []*sourcepb.ReadResult{
			{RecordId: "y", State: sourcepb.ReadState_READ_STATE_EXISTS}, {RecordId: "x", State: sourcepb.ReadState_READ_STATE_EXISTS}}}
	}
	_, _, err = c.Read(context.Background(), []string{"x", "y"})
	assert.ErrorContains(t, err, "answered y where x was asked")
}
