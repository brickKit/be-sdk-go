package rpc

import (
	"context"

	"google.golang.org/grpc"

	"github.com/brickKit/be-sdk-go/internal/problem"
)

// The streaming half of the client chain (P7.12). The protocol has no streaming rpcs (P7.11); the
// chain still applies so a stream never leaves without be-caller, a deadline, a bulkhead slot or the
// transaction guard. The deadline bounds the whole stream; the bulkhead slot and the in-flight
// gauge are held until the stream's context ends, when the handled counter is recorded with code OK
// (a stream's final status is not observed; a stream that fails to open is recorded with its code).

func (ch *clientChain) deadlineStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, cancel, err := outboundContext(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		cancel()
		return nil, restore(err)
	}
	context.AfterFunc(cs.Context(), cancel)
	return cs, nil
}

func (ch *clientChain) bulkheadStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if !ch.bulkhead.acquire() {
		return nil, problem.Be("OUTBOUND_LIMIT", nil)
	}
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		ch.bulkhead.release()
		return nil, err
	}
	context.AfterFunc(cs.Context(), ch.bulkhead.release)
	return cs, nil
}

func (ch *clientChain) metadataStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(ch.outgoing(ctx), desc, cc, method, opts...)
}

func (ch *clientChain) metricsStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	done := ch.observe()
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		done(method, err)
		return nil, err
	}
	context.AfterFunc(cs.Context(), func() { done(method, nil) })
	return cs, nil
}

func (ch *clientChain) txGuardStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
	method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if err := ch.inTx(ctx, method); err != nil {
		return nil, err
	}
	return streamer(ctx, desc, cc, method, opts...)
}
