package logx

import (
	"context"
	"log/slog"
)

// fieldsKey is the context key of the request-scoped log fields.
type fieldsKey struct{}

// WithFields returns a context whose log lines carry these request-scoped fields (P18.2 log fields:
// request_id, sub, act, caller, perm, event_id, subject, delivery, job, …) after the trace fields and
// before the record's own attributes. Fields added later override earlier ones of the same key. The
// parent context is not changed.
func WithFields(ctx context.Context, attrs ...slog.Attr) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	prev := ContextFields(ctx)
	next := make([]slog.Attr, 0, len(prev)+len(attrs))
	next = append(append(next, prev...), attrs...)
	return context.WithValue(ctx, fieldsKey{}, next)
}

// ContextFields returns the request-scoped log fields of ctx in the order they were added (nil when
// none). The caller must not modify the returned slice.
func ContextFields(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(fieldsKey{}).([]slog.Attr)
	return v
}
