package rpc

import (
	"sync"

	"google.golang.org/grpc/encoding"
	protoenc "google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// grpc-go turns a request that does not decode into INTERNAL before any interceptor runs, so the
// chain would neither see nor log it. The server's codec therefore never fails a decode: it resets
// the message, remembers it as malformed, and the batch layer answers REQUEST_INVALID (stage-B
// ruling: every malformed request the SDK detects is be/REQUEST_INVALID), after identity like any
// other validation. The outermost layer forgets the mark however the call ends.

// malformed is the set of request messages of one server whose bytes did not decode, keyed by the
// message pointer the codec was handed.
type malformed struct{ m sync.Map } // any (pointer) → error

// requestCodec is the server's codec: the standard proto codec, except that decoding never fails.
type requestCodec struct {
	inner encoding.CodecV2
	bad   *malformed
}

func newRequestCodec(bad *malformed) *requestCodec {
	return &requestCodec{inner: encoding.GetCodecV2(protoenc.Name), bad: bad}
}

func (c *requestCodec) Marshal(v any) (mem.BufferSlice, error) { return c.inner.Marshal(v) }
func (c *requestCodec) Name() string                           { return protoenc.Name }

func (c *requestCodec) Unmarshal(data mem.BufferSlice, v any) error {
	err := c.inner.Unmarshal(data, v)
	if err == nil {
		return nil
	}
	m, ok := v.(proto.Message)
	if !ok {
		return err
	}
	proto.Reset(m)
	c.bad.m.Store(v, err)
	return nil
}

// take returns REQUEST_INVALID with one violation when req did not decode, forgetting the mark.
func (b *malformed) take(req any) *problem.Error {
	v, ok := b.m.LoadAndDelete(req)
	if !ok {
		return nil
	}
	e := problem.Wrap(v.(error), "REQUEST_INVALID", nil)
	e.Violations = []problem.Violation{{Field: "request", Reason: "INVALID", Description: "the request message does not decode"}}
	return e
}

// forget drops a mark the chain did not reach (a call refused before the batch layer).
func (b *malformed) forget(req any) { b.m.Delete(req) }
