// Package echotest is a hand-written one-method gRPC service for the SDK's tests: /demo.Echo/Say(StringValue)
// StringValue, so tests need no generated code.
package echotest

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Method is the full method name.
const Method = "/demo.Echo/Say"

// Server is the service's interface.
type Server interface {
	Say(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
}

// Say adapts a function to Server.
type Say func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)

// Say implements Server.
func (f Say) Say(ctx context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return f(ctx, in)
}

// Desc is the service description.
var Desc = grpc.ServiceDesc{ServiceName: "demo.Echo", HandlerType: (*Server)(nil),
	Methods: []grpc.MethodDesc{{MethodName: "Say", Handler: func(srv any, ctx context.Context, dec func(any) error,
		ic grpc.UnaryServerInterceptor) (any, error) {
		in := new(wrapperspb.StringValue)
		if err := dec(in); err != nil {
			return nil, err
		}
		h := func(ctx context.Context, req any) (any, error) {
			return srv.(Server).Say(ctx, req.(*wrapperspb.StringValue))
		}
		if ic == nil {
			return h(ctx, in)
		}
		return ic(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: Method}, h)
	}}}}
