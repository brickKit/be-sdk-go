package rpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

// The streaming half of the server chain (P7.4): the same layers as the unary one. The protocol has
// no streaming rpcs of its own (P7.11), but the health service's Watch is one, and a server must not
// let a stream bypass the chain.

// wrappedStream replaces a server stream's context and checks that each received message decoded and
// is within its batch limits.
type wrappedStream struct {
	grpc.ServerStream
	ctx   context.Context
	batch *batchLimits
	bad   *malformed // the server's undecodable messages (REQUEST_INVALID)
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func (w *wrappedStream) RecvMsg(m any) error {
	if err := w.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if w.bad != nil {
		if e := w.bad.take(m); e != nil {
			return e
		}
	}
	if w.batch != nil {
		if e := w.batch.check(m); e != nil {
			return e
		}
	}
	return nil
}

func withContext(ss grpc.ServerStream, ctx context.Context) grpc.ServerStream {
	if w, ok := ss.(*wrappedStream); ok {
		return &wrappedStream{ServerStream: w.ServerStream, ctx: ctx, batch: w.batch, bad: w.bad}
	}
	return &wrappedStream{ServerStream: ss, ctx: ctx}
}

func (s *serverChain) reportStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo,
	next grpc.StreamHandler) (err error) {
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			err = recovered(r)
		}
		err = s.finish(ss.Context(), info.FullMethod, start, err)
	}()
	return next(srv, ss)
}

func (s *serverChain) identityStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo,
	next grpc.StreamHandler) error {
	ctx, err := s.principal(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	return next(srv, withContext(ss, ctx))
}

func (s *serverChain) deadlineStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo,
	next grpc.StreamHandler) error {
	ctx, cancel := withFloor(ss.Context())
	defer cancel()
	return next(srv, withContext(ss, ctx))
}

func (s *serverChain) batchStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo,
	next grpc.StreamHandler) error {
	w, ok := ss.(*wrappedStream)
	if !ok {
		w = &wrappedStream{ServerStream: ss, ctx: ss.Context()}
	}
	return next(srv, &wrappedStream{ServerStream: w.ServerStream, ctx: w.ctx, batch: &s.batch, bad: &s.bad})
}
