// Package reqctx carries the request-scoped values that outbound calls forward (protocol chapter 04, P4.2): the
// request ID and the user's credential. The user surface and the system surface put them in; the call clients read
// them. Modules never need it directly.
package reqctx

import "context"

type key int

const (
	requestIDKey key = iota
	userCredentialKey
)

// WithRequestID returns ctx carrying the request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestID returns the request ID ("" when none).
func RequestID(ctx context.Context) string { s, _ := ctx.Value(requestIDKey).(string); return s }

// WithUserCredential returns ctx carrying the caller's credential (the Authorization header value as received), so a
// read on the user's behalf forwards the original credential (component implementation conventions, "calling other
// components").
func WithUserCredential(ctx context.Context, authorization string) context.Context {
	return context.WithValue(ctx, userCredentialKey, authorization)
}

// UserCredential returns the forwarded credential ("" when none).
func UserCredential(ctx context.Context) string {
	s, _ := ctx.Value(userCredentialKey).(string)
	return s
}
